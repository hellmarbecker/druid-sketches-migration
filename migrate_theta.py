#!/usr/bin/env python
"""Theta sketch write path: Druid rollup table -> ClickHouse AggregateFunction(uniqTheta).

This path is *lossless in the bytes*: a Druid Theta sketch is a compact DataSketches
sketch, and a ClickHouse uniqTheta aggregate state is the same compact sketch behind a
LEB128 length prefix. Both sides use the DataSketches default seed (seedHash 37836) and
both hash the raw value, so transcoded sketches union with natively-built ClickHouse
sketches and genuinely dedupe. See spikes/inspect_sketches.py for the evidence.

The one lossy edge: ClickHouse's uniqTheta has a hard-wired nominal k=4096, so merging
across rows downsamples anything Druid built with a larger `size`. Stored states keep
their original precision; only merges degrade. Verified in verify() below.

Run:  .venv/bin/python migrate_theta.py
Needs Druid + ClickHouse running (see CLAUDE.md).
"""

from __future__ import annotations

import sys

from datasketches import update_theta_sketch

from sketch_io import (
    ch, decode_druid_sketch, druid_rows, enc_agg_state, enc_datetime_iso, enc_int64,
    enc_nullable_string, enc_string, enc_uint64, esc,
)

SOURCE = "wikipedia_rollup_sketches"
TARGET = "wikipedia_rollup_sketches"
BATCH_ROWS = 1000

# Druid column -> ClickHouse column, in RowBinary write order.
SELECT_COLS = [
    "__time", "channel", "countryName", "isRobot",
    '"count"', "sum_added", "users_theta_16384", "pages_theta_4096",
]

DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    ts          DateTime,
    channel     String,
    countryName Nullable(String),
    isRobot     String,
    -- Rollup measures must be aggregate types, or AggregatingMergeTree merges would keep
    -- an arbitrary row's value. SimpleAggregateFunction(sum) mirrors Druid's longSum and
    -- is byte-identical to the plain type in RowBinary.
    cnt         SimpleAggregateFunction(sum, UInt64),
    sum_added   SimpleAggregateFunction(sum, Int64),
    users_theta AggregateFunction(uniqTheta, String),
    pages_theta AggregateFunction(uniqTheta, String)
) ENGINE = AggregatingMergeTree
ORDER BY (channel, countryName, isRobot, ts)
SETTINGS allow_nullable_key = 1
"""


# ------------------------------------------------------------------ RowBinary encoding
def enc_row(row: list) -> bytes:
    ts, channel, country, is_robot, cnt, added, users, pages = row
    return (
        enc_datetime_iso(ts)
        + enc_string(channel or "")
        + enc_nullable_string(country)
        + enc_string(is_robot or "")
        + enc_uint64(cnt)
        + enc_int64(added)
        + enc_agg_state(decode_druid_sketch(users))
        + enc_agg_state(decode_druid_sketch(pages))
    )


# --------------------------------------------------------------------------- migration
def migrate() -> int:
    ch(f"DROP TABLE IF EXISTS {TARGET}")
    ch(DDL)

    query = f"SELECT {', '.join(SELECT_COLS)} FROM {SOURCE}"
    insert = f"INSERT INTO {TARGET} FORMAT RowBinary"

    batch, total = bytearray(), 0
    for row in druid_rows(query):
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


# ------------------------------------------------------------------------ verification
def verify(migrated: int) -> bool:
    def druid(q: str):
        return druid_rows(q).__next__()

    ok = True
    print("\n=== row count ===")
    src = druid(f"SELECT COUNT(*) FROM {SOURCE}")[0]
    dst = int(ch(f"SELECT count() FROM {TARGET}"))
    print(f"  druid={src}  clickhouse={dst}  migrated={migrated}  match={src == dst == migrated}")
    ok &= src == dst == migrated

    # 1. Per-row fidelity: one row per key, so no real merge happens on either side and the
    #    estimates must match exactly (while retained entries stay under k=4096).
    #    ClickHouse picks the largest sketches -- grouping is cheap there, whereas the same
    #    GROUP BY on this Druid broker exhausts its merge buffers.
    print("\n=== per-row estimates (no merge -> must be exact) ===")
    # isNull travels as its own column: a literal NULL would come back as TSV "\N" and
    # collide with a genuine empty-string value.
    top = ch(
        f"SELECT channel, isNull(countryName), ifNull(countryName, ''), isRobot, "
        f"toUnixTimestamp(ts) * 1000, "
        f"uniqThetaMerge(users_theta), uniqThetaMerge(pages_theta) "
        f"FROM {TARGET} GROUP BY 1, 2, 3, 4, 5 ORDER BY 6 DESC LIMIT 5"
    ).split("\n")

    for line in top:
        channel, cnull, country, is_robot, millis, cu_row, cp_row = line.split("\t")
        is_null = cnull == "1"
        cond = (
            f"channel = {esc(channel)} AND isRobot = {esc(is_robot)} "
            f"AND __time = MILLIS_TO_TIMESTAMP({millis}) AND "
            + ("countryName IS NULL" if is_null else f"countryName = {esc(country)}")
        )
        u, p = next(iter(druid_rows(
            f"SELECT THETA_SKETCH_ESTIMATE(users_theta_16384), "
            f"THETA_SKETCH_ESTIMATE(pages_theta_4096) FROM {SOURCE} WHERE {cond}"
        )))
        exact = int(cu_row) == round(u) and int(cp_row) == round(p)
        # Above k=4096 ClickHouse prunes on merge, so exactness is not expected there.
        under_k = max(u, p) <= 4096
        ok &= exact or not under_k
        label = f"{channel}/{'NULL' if is_null else country}/{is_robot}"
        note = "" if under_k else "  (>k=4096, pruning expected)"
        print(f"  {label:<38} druid=({u:.0f},{p:.0f}) ch=({cu_row},{cp_row}) exact={exact}{note}")

    # 2. Full merge: ClickHouse's fixed k=4096 downsamples, so expect a small delta.
    print("\n=== full merge (ClickHouse k=4096 caps precision) ===")
    du, dp = druid(
        f"SELECT APPROX_COUNT_DISTINCT_DS_THETA(users_theta_16384), "
        f"APPROX_COUNT_DISTINCT_DS_THETA(pages_theta_4096) FROM {SOURCE}"
    )
    cu, cp = ch(
        f"SELECT uniqThetaMerge(users_theta), uniqThetaMerge(pages_theta) FROM {TARGET}"
    ).split("\t")
    for name, d, c in (("users (druid size=16384)", du, int(cu)), ("pages (druid size=4096)", dp, int(cp))):
        err = abs(c - d) / d * 100
        print(f"  {name:<26} druid={d:<8} clickhouse={c:<8} delta={err:.2f}%")
        ok &= err < 5.0

    # 3. The property that makes this path worth it: cross-system dedup.
    #    Uses 1000 values (well under k=4096) so every count is exact -- a sketch in
    #    estimation mode scales each new value by 1/theta and could never land on a
    #    round number. Goes through enc_agg_state(), i.e. the real migration encoder.
    print("\n=== cross-system dedup (exact mode, via the real encoder) ===")
    ch("DROP TABLE IF EXISTS _dedup_probe")
    ch("CREATE TABLE _dedup_probe (label String, s AggregateFunction(uniqTheta, String)) "
       "ENGINE = MergeTree ORDER BY label")

    sk = update_theta_sketch(14)  # lg_k=14 == Druid size 16384
    for i in range(1000):
        sk.update(f"probe{i}")
    ch("INSERT INTO _dedup_probe FORMAT RowBinary",
       data=enc_string("transcoded") + enc_agg_state(sk.compact().serialize()))

    ch("INSERT INTO _dedup_probe SELECT 'native_same', "
       "uniqThetaState(concat('probe', toString(number))) FROM numbers(1000)")
    ch("INSERT INTO _dedup_probe SELECT 'native_shift', "
       "uniqThetaState(concat('probe', toString(number + 500))) FROM numbers(1000)")

    alone = int(ch("SELECT uniqThetaMerge(s) FROM _dedup_probe WHERE label = 'transcoded'"))
    same = int(ch("SELECT uniqThetaMerge(s) FROM _dedup_probe "
                  "WHERE label IN ('transcoded', 'native_same')"))
    shifted = int(ch("SELECT uniqThetaMerge(s) FROM _dedup_probe "
                     "WHERE label IN ('transcoded', 'native_shift')"))
    ch("DROP TABLE _dedup_probe")

    for label, got, want in (
        ("transcoded sketch alone", alone, 1000),
        ("union with identical native set (full dedup)", same, 1000),
        ("union with 50%-overlapping native set", shifted, 1500),
    ):
        print(f"  {label:<45} {got:<6} expected {want}  match={got == want}")
        ok &= got == want
    return bool(ok)


def main() -> int:
    print(f"=== migrating {SOURCE} -> ClickHouse {TARGET} ===")
    total = migrate()
    ok = verify(total)
    print(f"\n{'ALL CHECKS PASSED' if ok else 'CHECKS FAILED'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
