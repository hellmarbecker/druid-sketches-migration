#!/usr/bin/env python
"""Spike: read real DataSketches columns out of Druid and introspect them.

Answers the questions the migration design depends on:
  1. How do sketch bytes come off the wire?  (base64, double-JSON-quoted)
  2. Which sketch parameters are recoverable from the bytes alone?
  3. Can the Python `datasketches` lib read what datasketches-java 4.2.0 wrote?
     (compared against Druid's own estimate for the same sketch)
  4. Does ClickHouse's uniqTheta use the same DataSketches seed as Druid?
     (if the seed hashes match, cross-system Theta unions dedupe correctly)

Run:  .venv/bin/python spikes/inspect_sketches.py
Needs the Druid datasource built by druid/rollup-sketches-index.json.
"""

from __future__ import annotations

import base64
import json
import os
import struct
import subprocess
import sys

import requests
from datasketches import hll_sketch, compact_theta_sketch
from dotenv import load_dotenv

load_dotenv()

DRUID_URL = os.environ.get("DRUID_ROUTER_URL", "http://localhost:8888")
CLICKHOUSE_BINARY = os.environ.get("CLICKHOUSE_BINARY", "clickhouse")
DATASOURCE = "wikipedia_rollup_sketches"

# DataSketches family IDs (shared preamble byte 2 across all sketch types).
FAMILY = {1: "ALPHA", 2: "QUICKSELECT", 3: "COMPACT (theta)", 7: "HLL"}
# HLL preamble byte 7: low 2 bits = current mode, bits 2-3 = configured target type.
HLL_MODE = {0: "LIST (sparse)", 1: "SET (sparse)", 2: "HLL (dense)"}
HLL_TGT = {0: "HLL_4", 1: "HLL_6", 2: "HLL_8"}


def sql(query: str) -> list[dict]:
    r = requests.post(f"{DRUID_URL}/druid/v2/sql", json={"query": query}, timeout=60)
    r.raise_for_status()
    return r.json()


def decode_sketch(value: str) -> bytes:
    """Druid emits complex columns as base64 wrapped in a *quoted* JSON string."""
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    return base64.b64decode(value)


def parse_hll_preamble(b: bytes) -> dict:
    mode_byte = b[7]
    return {
        "preInts": b[0] & 0x3F,
        "serVer": b[1],
        "family": FAMILY.get(b[2], f"? ({b[2]})"),
        "lgK": b[3],
        "lgArr": b[4],
        "mode": HLL_MODE.get(mode_byte & 0x03, f"? ({mode_byte & 3})"),
        "tgtHllType": HLL_TGT.get((mode_byte >> 2) & 0x03, f"? ({(mode_byte >> 2) & 3})"),
    }


def parse_theta_preamble(b: bytes) -> dict:
    pre_longs = b[0] & 0x3F
    out = {
        "preLongs": pre_longs,
        "serVer": b[1],
        "family": FAMILY.get(b[2], f"? ({b[2]})"),
        "lgNomLongs": b[3],  # 0 in compact form -- the configured `size` is NOT stored
        "lgArrLongs": b[4],
        "flags": f"0x{b[5]:02x}",
        "seedHash": struct.unpack_from("<H", b, 6)[0],
    }
    if pre_longs >= 2:
        out["retainedEntries"] = struct.unpack_from("<I", b, 8)[0]
    # theta itself only present when preLongs >= 3; otherwise it is implicitly 1.0
    out["thetaLong"] = struct.unpack_from("<Q", b, 16)[0] if pre_longs >= 3 else "implicit MAX"
    return out


def read_varint(b: bytes, pos: int = 0) -> tuple[int, int]:
    """ClickHouse prefixes serialized aggregate states with a LEB128 length."""
    result = shift = 0
    while True:
        byte = b[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7


def clickhouse_theta_seed_hash() -> dict | None:
    """Pull a uniqTheta state out of ClickHouse and read its DataSketches preamble."""
    query = (
        "SELECT hex(toString(uniqThetaState(number))) FROM numbers(1000)"
    )
    try:
        out = subprocess.run(
            [CLICKHOUSE_BINARY, "local", "--query", query],
            capture_output=True, text=True, timeout=60, check=True, cwd="/tmp",
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError) as exc:
        print(f"  ClickHouse probe skipped: {exc}")
        return None
    raw = bytes.fromhex(out)
    payload_len, offset = read_varint(raw)
    sketch = raw[offset:offset + payload_len]
    return parse_theta_preamble(sketch)


def show(label: str, fields: dict) -> None:
    print(f"  {label}")
    for k, v in fields.items():
        print(f"      {k:<18} {v}")


def main() -> int:
    print(f"=== Druid columns in {DATASOURCE} ===")
    for row in sql(
        "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_NAME = '{DATASOURCE}' ORDER BY ORDINAL_POSITION"
    ):
        print(f"  {row['COLUMN_NAME']:<22} {row['DATA_TYPE']}")

    rollup = sql(
        f'SELECT COUNT(*) AS stored_rows, SUM("count") AS raw_events FROM {DATASOURCE}'
    )[0]
    ratio = rollup["raw_events"] / rollup["stored_rows"]
    print(f"\n  rollup: {rollup['raw_events']} events -> {rollup['stored_rows']} rows ({ratio:.1f}x)")

    hll_cols = ["users_hll_k12_hll4", "pages_hll_k14_hll8"]
    theta_cols = ["users_theta_16384", "pages_theta_4096"]

    # --- Stored per-row sketches: what actually sits in the segment ----------
    print("\n=== Stored sketches (one rollup row) ===")
    cols = ", ".join(hll_cols + theta_cols)
    estimates = ", ".join(
        [f"HLL_SKETCH_ESTIMATE({c}) AS est_{c}" for c in hll_cols]
        + [f"THETA_SKETCH_ESTIMATE({c}) AS est_{c}" for c in theta_cols]
    )
    row = sql(
        f"SELECT {cols}, {estimates} FROM {DATASOURCE} "
        "WHERE channel = '#en.wikipedia' ORDER BY __time LIMIT 1"
    )[0]

    for col in hll_cols:
        b = decode_sketch(row[col])
        p = parse_hll_preamble(b)
        local = hll_sketch.deserialize(b)
        p["bytes"] = len(b)
        p["druid_estimate"] = f"{row['est_' + col]:.4f}"
        p["python_estimate"] = f"{local.get_estimate():.4f}"
        p["MATCH"] = abs(local.get_estimate() - row["est_" + col]) < 1e-6
        show(col, p)

    for col in theta_cols:
        b = decode_sketch(row[col])
        p = parse_theta_preamble(b)
        local = compact_theta_sketch.deserialize(b)
        p["bytes"] = len(b)
        p["druid_estimate"] = f"{row['est_' + col]:.4f}"
        p["python_estimate"] = f"{local.get_estimate():.4f}"
        p["MATCH"] = abs(local.get_estimate() - row["est_" + col]) < 1e-6
        show(col, p)

    # --- Merged sketches: the dense forms a migration would actually export --
    print("\n=== Merged over the whole datasource (dense forms) ===")
    merged = sql(
        f"SELECT DS_HLL({hll_cols[0]}) AS h, DS_THETA({theta_cols[0]}) AS t, "
        f"APPROX_COUNT_DISTINCT_DS_HLL({hll_cols[0]}) AS h_est, "
        f"APPROX_COUNT_DISTINCT_DS_THETA({theta_cols[0]}) AS t_est "
        f"FROM {DATASOURCE}"
    )[0]

    hb = decode_sketch(merged["h"])
    hp = parse_hll_preamble(hb)
    hp["bytes"] = len(hb)
    hp["register_offset"] = 40 if (hb[7] & 3) == 2 else "n/a (sparse)"
    hp["druid_estimate"] = f"{merged['h_est']:.4f}"
    hp["python_estimate"] = f"{hll_sketch.deserialize(hb).get_estimate():.4f}"
    show(f"DS_HLL({hll_cols[0]})", hp)

    tb = decode_sketch(merged["t"])
    tp = parse_theta_preamble(tb)
    tp["bytes"] = len(tb)
    tp["druid_estimate"] = f"{merged['t_est']:.4f}"
    tp["python_estimate"] = f"{compact_theta_sketch.deserialize(tb).get_estimate():.4f}"
    show(f"DS_THETA({theta_cols[0]})", tp)

    # --- Cross-system seed check: decides whether Theta transcoding is viable -
    print("\n=== ClickHouse uniqTheta vs Druid theta ===")
    ch = clickhouse_theta_seed_hash()
    if ch:
        show("clickhouse uniqThetaState(number)", ch)
        same = ch["seedHash"] == tp["seedHash"]
        print(f"\n  druid seedHash={tp['seedHash']}  clickhouse seedHash={ch['seedHash']}")
        print(f"  -> same DataSketches seed: {same}")
        print("     %s" % (
            "Cross-system Theta unions will dedupe correctly."
            if same else
            "Seeds differ: transcoded sketches CANNOT be merged with native ClickHouse ones."
        ))
    return 0


if __name__ == "__main__":
    sys.exit(main())
