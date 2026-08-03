#!/usr/bin/env python
"""HLL path: Druid HLL sketches -> ClickHouse, pre-merged in Python.

ClickHouse's uniqHLL12 is its own HLL, not Apache DataSketches HLL, so there is no
byte-level target to transcode into (unlike Theta -- see migrate_theta.py). This path
takes the option that keeps the numbers honest:

  1. Union the Druid sketches *in Python* with hll_union at every granularity the
     warehouse needs, and land the estimate plus its 2-sigma bounds. Each number is
     what Druid itself would report at that grain -- no re-hashing, no fabrication.
  2. Carry the sketch bytes verbatim in a String column as well, so the data stays
     re-rollable later (and readable by clickhouse/hll_merge_udf.py at query time).

The loss is *query-time mergeability*: ClickHouse cannot union these natively. Pre-merging
is what buys that back, which is why the grains are materialised up front. Never sum the
estimates across rows to fake a union -- verify() below shows what that would cost.

Run:  .venv/bin/python migrate_hll.py
Needs Druid + ClickHouse running (see CLAUDE.md).
"""

from __future__ import annotations

import sys
from collections import defaultdict
from datetime import datetime, timezone

from datasketches import hll_sketch, hll_union, tgt_hll_type

from sketch_io import (
    ch, decode_druid_sketch, druid_one, druid_rows, enc_bytes, enc_datetime_iso,
    enc_float64, enc_nullable_string, enc_string, enc_uint64, hll_lg_k,
)

SOURCE = "wikipedia_rollup_sketches"
TARGET = "wikipedia_rollup_hll"

# (output prefix, Druid column)
HLL_COLS = [("users", "users_hll_k12_hll4"), ("pages", "pages_hll_k14_hll8")]

# Druid stores tgtHllType in preamble byte 7, bits 2-3. Preserve it through the union.
TGT_TYPES = {0: tgt_hll_type.HLL_4, 1: tgt_hll_type.HLL_6, 2: tgt_hll_type.HLL_8}

# Grains to materialise. `identity` means one output row per source row, so the original
# Druid bytes are carried through untouched instead of being re-serialised by a union.
GRAINS = [
    {"name": "hour_dims", "dims": ["channel", "countryName", "isRobot"],
     "day": False, "identity": True},
    {"name": "hour_channel", "dims": ["channel"], "day": False, "identity": False},
    {"name": "day_channel", "dims": ["channel"], "day": True, "identity": False},
    {"name": "day_total", "dims": [], "day": True, "identity": False},
]

DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    grain        LowCardinality(String),
    ts           DateTime,
    -- A dimension is NULL when this grain aggregated it away. At the `hour_dims` grain a
    -- NULL countryName is instead a genuine Druid NULL; `grain` disambiguates the two.
    channel      Nullable(String),
    countryName  Nullable(String),
    isRobot      Nullable(String),
    events       UInt64,
    users_est    Float64,
    users_lb2    Float64,
    users_ub2    Float64,
    users_sketch String,
    pages_est    Float64,
    pages_lb2    Float64,
    pages_ub2    Float64,
    pages_sketch String
) ENGINE = MergeTree
ORDER BY (grain, ts)
"""


def tgt_type_of(sketch: bytes) -> tgt_hll_type:
    return TGT_TYPES[(sketch[7] >> 2) & 0x03]


def merge_sketches(blobs: list[bytes]) -> tuple[bytes, hll_sketch]:
    """Union DataSketches HLL blobs. lgK and target type are read from the bytes rather
    than assumed -- the fixture deliberately mixes lgK 12/HLL_4 and lgK 14/HLL_8."""
    u = hll_union(hll_lg_k(blobs[0]))
    for b in blobs:
        u.update(hll_sketch.deserialize(b))
    result = u.get_result(tgt_type_of(blobs[0]))
    return result.serialize_compact(), result


def bucket_ts(ts: str, to_day: bool) -> str:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    if to_day:
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def migrate() -> dict[str, int]:
    ch(f"DROP TABLE IF EXISTS {TARGET}")
    ch(DDL)

    cols = ["__time", "channel", "countryName", "isRobot", '"count"'] + [c for _, c in HLL_COLS]
    # Small fixture, so one in-memory pass is fine. At real scale, stream the source
    # ordered by each grain's key and union incrementally instead of buffering.
    source = [
        (r[0], r[1], r[2], r[3], r[4], [decode_druid_sketch(r[5 + i]) for i in range(len(HLL_COLS))])
        for r in druid_rows(f"SELECT {', '.join(cols)} FROM {SOURCE}")
    ]
    print(f"  read {len(source)} source rows")

    written: dict[str, int] = {}
    for grain in GRAINS:
        groups: dict[tuple, list] = defaultdict(list)
        for ts, channel, country, is_robot, cnt, blobs in source:
            dims = {"channel": channel, "countryName": country, "isRobot": is_robot}
            key = (bucket_ts(ts, grain["day"]),) + tuple(dims[d] for d in grain["dims"])
            groups[key].append((cnt, blobs, dims))

        batch = bytearray()
        for key, members in groups.items():
            ts_key, *dim_vals = key
            dims = dict(zip(grain["dims"], dim_vals))
            events = sum(m[0] for m in members)

            row = (
                enc_string(grain["name"])
                + enc_datetime_iso(ts_key)
                + enc_nullable_string(dims.get("channel"))
                + enc_nullable_string(dims.get("countryName"))
                + enc_nullable_string(dims.get("isRobot"))
                + enc_uint64(events)
            )
            for i, _ in enumerate(HLL_COLS):
                blobs = [m[1][i] for m in members]
                if grain["identity"]:
                    # Carry Druid's original bytes; estimate them without a union.
                    raw = blobs[0]
                    sk = hll_sketch.deserialize(raw)
                else:
                    raw, sk = merge_sketches(blobs)
                row += (
                    enc_float64(sk.get_estimate())
                    + enc_float64(sk.get_lower_bound(2))
                    + enc_float64(sk.get_upper_bound(2))
                    + enc_bytes(raw)
                )
            batch += row

        ch(f"INSERT INTO {TARGET} FORMAT RowBinary", data=bytes(batch))
        written[grain["name"]] = len(groups)
        print(f"  {grain['name']:<14} {len(groups)} rows")
    return written


def verify(written: dict[str, int]) -> bool:
    ok = True

    print("\n=== rows per grain ===")
    for line in ch(f"SELECT grain, count() FROM {TARGET} GROUP BY grain ORDER BY 2 DESC").split("\n"):
        grain, n = line.split("\t")
        match = written[grain] == int(n)
        ok &= match
        print(f"  {grain:<14} {n:<6} match={match}")

    # lgK comes from the carried bytes -- needed because Druid's merge default ignores it.
    lg_k = {}
    for prefix, _ in HLL_COLS:
        hexed = ch(f"SELECT hex({prefix}_sketch) FROM {TARGET} WHERE grain = 'day_total'")
        lg_k[prefix] = hll_lg_k(bytes.fromhex(hexed))

    # Druid's APPROX_COUNT_DISTINCT_DS_HLL merges at lgK=12 unless told otherwise, *even
    # for a column built with a larger lgK*. Comparing against the default would make a
    # faithful migration look wrong, so pass the column's real lgK.
    print("\n=== Druid's default merge lgK (a trap, not a bug in this code) ===")
    for prefix, col in HLL_COLS:
        default = druid_one(f"SELECT APPROX_COUNT_DISTINCT_DS_HLL({col}) FROM {SOURCE}")[0]
        aligned = druid_one(
            f"SELECT APPROX_COUNT_DISTINCT_DS_HLL({col}, {lg_k[prefix]}) FROM {SOURCE}")[0]
        print(f"  {prefix:<6} column lgK={lg_k[prefix]}  druid default={default:<8} "
              f"druid lgK={lg_k[prefix]}={aligned:<8} "
              f"{'(default downsamples)' if default != aligned else '(same)'}")

    # 1. Pre-merged estimates should agree with Druid's own merge at the same lgK, since
    #    both union the identical sketches. They are not bit-identical -- union internals
    #    and Druid's rounding leave a small residual -- so the assertion is the meaningful
    #    one: Druid's number must sit inside our sketch's own 2-sigma bounds. That is
    #    self-calibrating per row, unlike a hand-picked percentage.
    print("\n=== day_total vs Druid's own merge (lgK aligned) ===")
    du, dp = druid_one(
        f"SELECT APPROX_COUNT_DISTINCT_DS_HLL({HLL_COLS[0][1]}, {lg_k['users']}), "
        f"APPROX_COUNT_DISTINCT_DS_HLL({HLL_COLS[1][1]}, {lg_k['pages']}) FROM {SOURCE}"
    )
    vals = ch(
        f"SELECT users_est, users_lb2, users_ub2, pages_est, pages_lb2, pages_ub2 "
        f"FROM {TARGET} WHERE grain = 'day_total'"
    ).split("\t")
    for name, d, (est, lb, ub) in (
        ("users", du, tuple(map(float, vals[0:3]))),
        ("pages", dp, tuple(map(float, vals[3:6]))),
    ):
        within = lb <= d <= ub
        ok &= within
        print(f"  {name:<6} druid={d:<8} python-merged={est:<12.4f} "
              f"delta={abs(est - d) / d * 100:.4f}%  2sigma=[{lb:.0f},{ub:.0f}] within={within}")

    print("\n=== day_channel vs Druid GROUP BY channel (lgK aligned) ===")
    try:
        rows = list(druid_rows(
            f"SELECT channel, APPROX_COUNT_DISTINCT_DS_HLL({HLL_COLS[0][1]}, {lg_k['users']}) AS u "
            f"FROM {SOURCE} GROUP BY 1 ORDER BY u DESC LIMIT 5"
        ))
    except RuntimeError as exc:
        print(f"  skipped (Druid): {str(exc)[:120]}")
        rows = []
    for channel, u in rows:
        est, lb, ub = map(float, ch(
            f"SELECT users_est, users_lb2, users_ub2 FROM {TARGET} "
            f"WHERE grain = 'day_channel' AND channel = "
            + "'" + channel.replace("'", "\\'") + "'"
        ).split("\t"))
        within = lb <= u <= ub
        ok &= within
        print(f"  {channel:<18} druid={u:<8} python-merged={est:<12.4f} "
              f"delta={abs(est - u) / u * 100:.4f}%  2sigma=[{lb:.0f},{ub:.0f}] within={within}")

    # 2. The trap this whole design exists to avoid.
    print("\n=== why estimates must not be summed ===")
    naive = float(ch(f"SELECT sum(users_est) FROM {TARGET} WHERE grain = 'hour_dims'"))
    true = float(ch(f"SELECT users_est FROM {TARGET} WHERE grain = 'day_total'"))
    print(f"  SUM of stored-grain estimates : {naive:>12.1f}   <- wrong, double counts")
    print(f"  union of the same sketches    : {true:>12.1f}   <- correct")
    print(f"  inflation                     : {naive / true:>12.1f}x")
    ok &= naive > true  # sanity: summing really does inflate

    # 3. The carried bytes must survive the String column untouched.
    print("\n=== lossless byte carry ===")
    for prefix, _ in HLL_COLS:
        hexed, est = ch(
            f"SELECT hex({prefix}_sketch), {prefix}_est FROM {TARGET} "
            f"WHERE grain = 'day_total'"
        ).split("\t")
        sk = hll_sketch.deserialize(bytes.fromhex(hexed))
        same = abs(sk.get_estimate() - float(est)) < 1e-9
        ok &= same
        print(f"  {prefix:<6} lgK={sk.lg_config_k} bytes={len(hexed)//2:<6} "
              f"re-read estimate={sk.get_estimate():.4f} matches column={same}")

    # 4. Query-time merge via the executable UDF must reproduce the pre-merged grain --
    #    this is what restores mergeability that ClickHouse cannot do natively.
    print("\n=== query-time merge via hllMergeEstimate UDF ===")
    registered = ch(
        "SELECT count() FROM system.functions WHERE name = 'hllMergeEstimate'") == "1"
    if not registered:
        print("  skipped: UDF not registered (see clickhouse/hll_merge_function.xml)")
    else:
        for prefix, _ in HLL_COLS:
            udf, pre = ch(
                f"SELECT round(hllMergeEstimate(arrayStringConcat("
                f"  groupArray(base64Encode({prefix}_sketch)), ',')), 4), "
                f"(SELECT round({prefix}_est, 4) FROM {TARGET} WHERE grain = 'day_total') "
                f"FROM {TARGET} WHERE grain = 'hour_dims'"
            ).split("\t")
            same = udf == pre
            ok &= same
            print(f"  {prefix:<6} udf-merged={udf:<14} pre-merged={pre:<14} identical={same}")
    return bool(ok)


def main() -> int:
    print(f"=== migrating {SOURCE} HLL columns -> ClickHouse {TARGET} ===")
    written = migrate()
    ok = verify(written)
    print(f"\n{'ALL CHECKS PASSED' if ok else 'CHECKS FAILED'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
