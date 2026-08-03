#!/usr/bin/env python
"""HLL register transplant: Druid HLL -> ClickHouse AggregateFunction(uniqHLL12, String).

The third option from the HLL analysis, now unblocked. ClickHouse's uniqHLL12 state format
was reverse-engineered (see uniqhll12.py and spikes/reverse_uniqhll12.py), so DataSketches
registers can be written straight into a native ClickHouse aggregate state. The migrated
column is then queryable with plain `uniqHLL12Merge` -- no UDF, no external process, and
it merges across rows inside the engine.

Why this works at all: HLL's estimator is symmetric in its buckets, so transplanting a
register array preserves the cardinality estimate even though the two systems hash
differently. Merging is a bucket-wise max, so transplanted states also union correctly
*with each other* -- they all share DataSketches' hashing.

THE RULE THIS PATH LIVES BY: never merge a transplanted state with a natively-built
ClickHouse one. The same key lands in different buckets, so the union double-counts
instead of deduplicating. verify() demonstrates the breakage rather than just asserting
the rule. Practically: nothing may ever write to these columns with uniqHLL12State().

Compared with migrate_hll.py (pre-merge + carried bytes + UDF), this trades exactness for
native queryability: estimates land within ~1-3% of the source rather than reproducing it,
because ClickHouse applies its own bias correction to the same registers.

Run:  .venv/bin/python migrate_hll_transplant.py
"""

from __future__ import annotations

import sys

from datasketches import hll_sketch, hll_union, tgt_hll_type

from sketch_io import (
    ch, decode_druid_sketch, druid_one, druid_rows, enc_datetime_iso,
    enc_nullable_string, enc_string, enc_uint64, esc,
)
from uniqhll12 import LG_K, datasketches_registers, state_from_datasketches

SOURCE = "wikipedia_rollup_sketches"
TARGET = "wikipedia_rollup_hll_native"
BATCH_ROWS = 500

HLL_COLS = [("users_hll", "users_hll_k12_hll4"), ("pages_hll", "pages_hll_k14_hll8")]

DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    ts          DateTime,
    channel     String,
    countryName Nullable(String),
    isRobot     String,
    cnt         SimpleAggregateFunction(sum, UInt64),
    -- Argument type must match whatever a native state would declare, or ClickHouse
    -- refuses the conversion. Druid built these over string dimensions.
    users_hll   AggregateFunction(uniqHLL12, String),
    pages_hll   AggregateFunction(uniqHLL12, String)
) ENGINE = AggregatingMergeTree
ORDER BY (channel, countryName, isRobot, ts)
SETTINGS allow_nullable_key = 1
"""


def enc_row(row: list) -> bytes:
    ts, channel, country, is_robot, cnt, users, pages = row
    return (
        enc_datetime_iso(ts)
        + enc_string(channel or "")
        + enc_nullable_string(country)
        + enc_string(is_robot or "")
        + enc_uint64(cnt)
        # AggregateFunction value is the serialize() output verbatim: the fixed 2651-byte
        # dense state, with no length prefix (unlike uniqTheta, which writes its own).
        + state_from_datasketches(decode_druid_sketch(users))
        + state_from_datasketches(decode_druid_sketch(pages))
    )


def migrate() -> int:
    ch(f"DROP TABLE IF EXISTS {TARGET}")
    ch(DDL)
    cols = ["__time", "channel", "countryName", "isRobot", '"count"'] + [c for _, c in HLL_COLS]
    insert = f"INSERT INTO {TARGET} FORMAT RowBinary"

    batch, total = bytearray(), 0
    for row in druid_rows(f"SELECT {', '.join(cols)} FROM {SOURCE}"):
        batch += enc_row(row)
        total += 1
        if total % BATCH_ROWS == 0:
            ch(insert, data=bytes(batch))
            batch.clear()
            print(f"  ... {total} rows")
    if batch:
        ch(insert, data=bytes(batch))
    print(f"  migrated {total} rows")
    return total


def verify(migrated: int) -> bool:
    ok = True

    print("\n=== row count ===")
    src = druid_one(f"SELECT COUNT(*) FROM {SOURCE}")[0]
    dst = int(ch(f"SELECT count() FROM {TARGET}"))
    ok &= src == dst == migrated
    print(f"  druid={src}  clickhouse={dst}  match={src == dst == migrated}")

    # 1. Native cross-row merge must reproduce the true union. This is the whole point:
    #    ClickHouse does the merging itself, in SQL, with no UDF.
    print("\n=== native uniqHLL12Merge across all rows vs Druid ===")
    for out_col, src_col in HLL_COLS:
        druid_est = druid_one(
            f"SELECT APPROX_COUNT_DISTINCT_DS_HLL({src_col}, {LG_K}) FROM {SOURCE}")[0]
        chv = int(ch(f"SELECT uniqHLL12Merge({out_col}) FROM {TARGET}"))
        delta = abs(chv - druid_est) / druid_est * 100
        ok &= delta < 5.0
        print(f"  {out_col:<10} druid(lgK={LG_K})={druid_est:<8} clickhouse={chv:<8} delta={delta:.2f}%")

    # 2. Per-group merges, to show the estimate holds up when grouping rather than
    #    collapsing everything into one sketch.
    print("\n=== per-channel merge vs Druid ===")
    rows = list(druid_rows(
        f"SELECT channel, APPROX_COUNT_DISTINCT_DS_HLL({HLL_COLS[0][1]}, {LG_K}) AS u "
        f"FROM {SOURCE} GROUP BY 1 ORDER BY u DESC LIMIT 5"
    ))
    for channel, u in rows:
        chv = int(ch(f"SELECT uniqHLL12Merge(users_hll) FROM {TARGET} "
                     f"WHERE channel = {esc(channel)}"))
        delta = abs(chv - u) / u * 100
        ok &= delta < 8.0
        print(f"  {channel:<18} druid={u:<8} clickhouse={chv:<8} delta={delta:.2f}%")

    # 3. Transplanted states must union with each other correctly -- the property that
    #    makes native merging usable at all.
    print("\n=== transplanted states union correctly with each other ===")
    A = hll_sketch(LG_K, tgt_hll_type.HLL_8)
    B = hll_sketch(LG_K, tgt_hll_type.HLL_8)
    for i in range(1000):
        A.update(f"k{i}")
    for i in range(500, 1500):
        B.update(f"k{i}")
    u = hll_union(LG_K)
    u.update(A)
    u.update(B)

    ch("DROP TABLE IF EXISTS _tp_probe")
    ch("CREATE TABLE _tp_probe (label String, s AggregateFunction(uniqHLL12, String)) "
       "ENGINE = MergeTree ORDER BY label")
    for label, sk in (("A", A), ("B", B), ("A_dup", A)):
        ch("INSERT INTO _tp_probe FORMAT RowBinary",
           data=enc_string(label) + state_from_datasketches(sk.serialize_compact()))

    ab = int(ch("SELECT uniqHLL12Merge(s) FROM _tp_probe WHERE label IN ('A','B')"))
    abd = int(ch("SELECT uniqHLL12Merge(s) FROM _tp_probe"))
    print(f"  A u B          = {ab:<7} (truth 1500, datasketches union "
          f"{u.get_result(tgt_hll_type.HLL_8).get_estimate():.0f})")
    print(f"  A u B u A(dup) = {abd:<7} (duplicate must not inflate)")
    ok &= abs(ab - 1500) / 1500 < 0.05 and ab == abd
    print(f"  union correct and idempotent: {abs(ab - 1500) / 1500 < 0.05 and ab == abd}")

    # 4. The limitation, demonstrated rather than asserted.
    print("\n=== mixing with NATIVE states double-counts (do not do this) ===")
    ch("DROP TABLE IF EXISTS _mix_probe")
    ch("CREATE TABLE _mix_probe (label String, s AggregateFunction(uniqHLL12, String)) "
       "ENGINE = MergeTree ORDER BY label")
    ch("INSERT INTO _mix_probe FORMAT RowBinary",
       data=enc_string("transplanted") + state_from_datasketches(A.serialize_compact()))
    ch("INSERT INTO _mix_probe SELECT 'native', "
       "uniqHLL12State(concat('k', toString(number))) FROM numbers(1000)")
    t = int(ch("SELECT uniqHLL12Merge(s) FROM _mix_probe WHERE label='transplanted'"))
    n = int(ch("SELECT uniqHLL12Merge(s) FROM _mix_probe WHERE label='native'"))
    m = int(ch("SELECT uniqHLL12Merge(s) FROM _mix_probe"))
    ch("DROP TABLE _mix_probe")
    ch("DROP TABLE _tp_probe")
    print(f"  transplanted={t}  native={n}  merged={m}  (same 1000 values)")
    print(f"  merged is ~2x, confirming incompatibility: {m > 1.8 * 1000}")
    ok &= m > 1.8 * 1000  # the breakage is expected; if it ever stops, revisit the docs
    return bool(ok)


def main() -> int:
    print(f"=== transplanting {SOURCE} HLL columns -> ClickHouse {TARGET} ===")
    total = migrate()
    ok = verify(total)
    print(f"\n{'ALL CHECKS PASSED' if ok else 'CHECKS FAILED'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
