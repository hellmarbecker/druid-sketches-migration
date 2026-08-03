"""Shared plumbing for the Druid -> ClickHouse sketch migrations.

Druid reads, ClickHouse writes, and the RowBinary encoders. Both migrate_theta.py and
migrate_hll.py build on this; keep engine-specific knowledge here rather than duplicating it.
"""

from __future__ import annotations

import base64
import json
import os
import struct
from datetime import datetime
from typing import Iterator

import requests
from dotenv import load_dotenv

load_dotenv()

DRUID_URL = os.environ.get("DRUID_ROUTER_URL", "http://localhost:8888")
CH_URL = os.environ.get("CLICKHOUSE_URL", "http://localhost:8123")


# --------------------------------------------------------------------------- ClickHouse
def ch(sql: str, data: bytes | None = None) -> str:
    r = requests.post(CH_URL, params={"query": sql}, data=data, timeout=300)
    if r.status_code != 200:
        raise RuntimeError(f"ClickHouse error: {r.text[:800]}")
    return r.text.strip()


def esc(s: str) -> str:
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"


# ------------------------------------------------------------------------------- Druid
def druid_rows(query: str) -> Iterator[list]:
    """Stream Druid SQL results as newline-delimited JSON arrays."""
    body = {"query": query, "resultFormat": "arrayLines", "header": True}
    with requests.post(f"{DRUID_URL}/druid/v2/sql", json=body, stream=True, timeout=600) as r:
        if r.status_code != 200:
            raise RuntimeError(f"Druid error: {r.text[:800]}")
        header_seen = False
        for line in r.iter_lines(decode_unicode=True):
            if not line:
                continue
            if not header_seen:  # first line is the column-name array
                header_seen = True
                continue
            yield json.loads(line)


def druid_one(query: str) -> list:
    return next(iter(druid_rows(query)))


def decode_druid_sketch(value: str) -> bytes:
    """Druid emits complex columns as base64 inside a *quoted* JSON string."""
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    return base64.b64decode(value)


# ------------------------------------------------------------------ RowBinary encoding
def varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def enc_string(s: str) -> bytes:
    b = s.encode()
    return varint(len(b)) + b


def enc_bytes(b: bytes) -> bytes:
    """A ClickHouse String column is arbitrary bytes -- fine for raw sketch payloads."""
    return varint(len(b)) + b


def enc_nullable_string(s: str | None) -> bytes:
    # Nullable(T): 1 flag byte, then the value only when not null.
    return b"\x01" if s is None else b"\x00" + enc_string(s)


def enc_agg_state(sketch: bytes) -> bytes:
    """An AggregateFunction value is exactly what the function's serialize() writes:
    for uniqTheta that is a LEB128 length followed by the compact sketch."""
    return varint(len(sketch)) + sketch


def enc_datetime_iso(ts: str) -> bytes:
    return struct.pack("<I", int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()))


def enc_uint64(n: int) -> bytes:
    return struct.pack("<Q", int(n))


def enc_int64(n: int) -> bytes:
    return struct.pack("<q", int(n))


def enc_float64(x: float) -> bytes:
    return struct.pack("<d", float(x))


# -------------------------------------------------------------- DataSketches preamble
def hll_lg_k(sketch: bytes) -> int:
    """lgK lives in preamble byte 3 of an HLL sketch -- read it, never assume the default."""
    if sketch[2] != 7:
        raise ValueError(f"not an HLL sketch (familyId={sketch[2]})")
    return sketch[3]
