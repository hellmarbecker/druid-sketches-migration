#!/usr/bin/env python
"""HLL register transplant: Druid HLL -> a native ClickHouse HLL aggregate state.

The third option from the HLL analysis. DataSketches registers are written straight into a
native ClickHouse aggregate state, so the migrated column is queryable with a plain -Merge
combinator -- no UDF, no external process, and it merges across rows inside the engine.

Two targets, chosen with --target:

  uniqhll12       (default) AggregateFunction(uniqHLL12, String). Precision is fixed at 12,
                  so every column is folded to lgK=12 -- the fixture's lgK=14 `pages` column
                  loses half its precision. Codec: uniqhll12.py, reverse-engineered.
  uniqcombined64  AggregateFunction(uniqCombined64(K), String), with K taken from each
                  column's own lgK (12..20), so nothing is folded. Codec: uniqcombined64.py,
                  read from ClickHouse source. Writes a separate table, so both can coexist.

Why this works at all: HLL's estimator is symmetric in its buckets, so transplanting a
register array preserves the cardinality estimate even though the two systems hash
differently. Merging is a bucket-wise max, so transplanted states also union correctly
*with each other* -- they all share DataSketches' hashing.

THE RULE THIS PATH LIVES BY, for both targets: never merge a transplanted state with a
natively-built ClickHouse one. The same key lands in different buckets, so the union
double-counts instead of deduplicating. verify() demonstrates the breakage rather than just
asserting the rule. Practically: nothing may ever write to these columns with
uniqHLL12State() / uniqCombined64State().

Compared with migrate_hll.py (pre-merge + carried bytes + UDF), this trades exactness for
native queryability: estimates land within ~1-4% of the source rather than reproducing it,
because ClickHouse applies its own estimator to the same registers. Neither target fixes the
lgK=12 `users` drift -- it is ClickHouse's estimator overcounting just past 2.5 * 2^K, and
both functions share that estimator at K=12. See CLAUDE.md.

Run:  .venv/bin/python migrate_hll_transplant.py [--target uniqhll12|uniqcombined64]
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from typing import Callable

from datasketches import hll_sketch, hll_union, tgt_hll_type

import uniqcombined64
import uniqhll12
from sketch_io import (
    ch, decode_druid_sketch, druid_one, druid_rows, enc_datetime_iso,
    enc_nullable_string, enc_string, enc_uint64, esc, hll_lg_k,
)

SOURCE = "wikipedia_rollup_sketches"
BATCH_ROWS = 500

HLL_COLS = [("users_hll", "users_hll_k12_hll4"), ("pages_hll", "pages_hll_k14_hll8")]


@dataclass(frozen=True)
class Target:
    """Everything that differs between the two ClickHouse state types."""
    table: str
    k_for: Callable[[int], int]                 # source lgK -> column precision
    col_type: Callable[[int], str]
    merge: Callable[[str, int], str]            # (column, k) -> SQL merge expression
    native_state: Callable[[str, int], str]     # (expression, k) -> SQL -State call
    state: Callable[[bytes, int], bytes]        # (Druid sketch bytes, k) -> state bytes


TARGETS = {
    "uniqhll12": Target(
        table="wikipedia_rollup_hll_native",
        k_for=lambda lg_k: uniqhll12.LG_K,
        col_type=lambda k: "AggregateFunction(uniqHLL12, String)",
        merge=lambda col, k: f"uniqHLL12Merge({col})",
        native_state=lambda expr, k: f"uniqHLL12State({expr})",
        state=lambda b, k: uniqhll12.state_from_datasketches(b),
    ),
    "uniqcombined64": Target(
        table="wikipedia_rollup_hll_uc64",
        k_for=uniqcombined64.target_k,
        col_type=lambda k: f"AggregateFunction(uniqCombined64({k}), String)",
        merge=lambda col, k: f"uniqCombined64Merge({k})({col})",
        native_state=lambda expr, k: f"uniqCombined64State({k})({expr})",
        state=uniqcombined64.state_from_datasketches,
    ),
}


def column_ks(target: Target) -> list[int]:
    """Precision per HLL column, from the lgK in the first row's preamble.

    A column's K is part of its type, so it is fixed once here. A later row with a smaller
    lgK cannot be unfolded into it and makes the uniqcombined64 codec raise; a larger one
    is folded down.
    """
    row = druid_one(f"SELECT {', '.join(c for _, c in HLL_COLS)} FROM {SOURCE} LIMIT 1")
    return [target.k_for(hll_lg_k(decode_druid_sketch(v))) for v in row]


def ddl(target: Target, ks: list[int]) -> str:
    users_k, pages_k = ks
    return f"""
CREATE TABLE IF NOT EXISTS {target.table} (
    ts          DateTime,
    channel     String,
    countryName Nullable(String),
    isRobot     String,
    cnt         SimpleAggregateFunction(sum, UInt64),
    -- Argument type must match whatever a native state would declare, or ClickHouse
    -- refuses the conversion. Druid built these over string dimensions.
    users_hll   {target.col_type(users_k)},
    pages_hll   {target.col_type(pages_k)}
) ENGINE = AggregatingMergeTree
ORDER BY (channel, countryName, isRobot, ts)
SETTINGS allow_nullable_key = 1
"""


def enc_row(row: list, target: Target, ks: list[int]) -> bytes:
    ts, channel, country, is_robot, cnt, users, pages = row
    users_k, pages_k = ks
    return (
        enc_datetime_iso(ts)
        + enc_string(channel or "")
        + enc_nullable_string(country)
        + enc_string(is_robot or "")
        + enc_uint64(cnt)
        # AggregateFunction value is the serialize() output verbatim, with no length prefix
        # (unlike uniqTheta, which writes its own) -- for both targets.
        + target.state(decode_druid_sketch(users), users_k)
        + target.state(decode_druid_sketch(pages), pages_k)
    )


def migrate(target: Target, ks: list[int]) -> int:
    ch(f"DROP TABLE IF EXISTS {target.table}")
    ch(ddl(target, ks))
    cols = ["__time", "channel", "countryName", "isRobot", '"count"'] + [c for _, c in HLL_COLS]
    insert = f"INSERT INTO {target.table} FORMAT RowBinary"

    batch, total = bytearray(), 0
    for row in druid_rows(f"SELECT {', '.join(cols)} FROM {SOURCE}"):
        batch += enc_row(row, target, ks)
        total += 1
        if total % BATCH_ROWS == 0:
            ch(insert, data=bytes(batch))
            batch.clear()
            print(f"  ... {total} rows")
    if batch:
        ch(insert, data=bytes(batch))
    print(f"  migrated {total} rows")
    return total


def verify(migrated: int, target: Target, ks: list[int]) -> bool:
    ok = True
    table = target.table

    print("\n=== row count ===")
    src = druid_one(f"SELECT COUNT(*) FROM {SOURCE}")[0]
    dst = int(ch(f"SELECT count() FROM {table}"))
    ok &= src == dst == migrated
    print(f"  druid={src}  clickhouse={dst}  match={src == dst == migrated}")

    # 1. Native cross-row merge must reproduce the true union. This is the whole point:
    #    ClickHouse does the merging itself, in SQL, with no UDF. Druid merges at the same
    #    lgK the column landed at, since its default of 12 would understate a K=14 column.
    print("\n=== native merge across all rows vs Druid ===")
    for (out_col, src_col), k in zip(HLL_COLS, ks):
        druid_est = druid_one(
            f"SELECT APPROX_COUNT_DISTINCT_DS_HLL({src_col}, {k}) FROM {SOURCE}")[0]
        chv = int(ch(f"SELECT {target.merge(out_col, k)} FROM {table}"))
        delta = abs(chv - druid_est) / druid_est * 100
        ok &= delta < 5.0
        print(f"  {out_col:<10} druid(lgK={k})={druid_est:<8} clickhouse={chv:<8} delta={delta:.2f}%")

    # 2. Per-group merges, to show the estimate holds up when grouping rather than
    #    collapsing everything into one sketch.
    print("\n=== per-channel merge vs Druid ===")
    users_k = ks[0]
    rows = list(druid_rows(
        f"SELECT channel, APPROX_COUNT_DISTINCT_DS_HLL({HLL_COLS[0][1]}, {users_k}) AS u "
        f"FROM {SOURCE} GROUP BY 1 ORDER BY u DESC LIMIT 5"
    ))
    for channel, u in rows:
        chv = int(ch(f"SELECT {target.merge('users_hll', users_k)} FROM {table} "
                     f"WHERE channel = {esc(channel)}"))
        delta = abs(chv - u) / u * 100
        ok &= delta < 8.0
        print(f"  {channel:<18} druid={u:<8} clickhouse={chv:<8} delta={delta:.2f}%")

    # 3. Transplanted states must union with each other correctly -- the property that
    #    makes native merging usable at all. Probed at the `pages` column's precision, so
    #    the uniqcombined64 target is exercised at a K other than 12.
    k = ks[-1]
    col_type = target.col_type(k)
    print(f"\n=== transplanted states union correctly with each other (K={k}) ===")
    A = hll_sketch(k, tgt_hll_type.HLL_8)
    B = hll_sketch(k, tgt_hll_type.HLL_8)
    for i in range(1000):
        A.update(f"k{i}")
    for i in range(500, 1500):
        B.update(f"k{i}")
    u = hll_union(k)
    u.update(A)
    u.update(B)

    ch("DROP TABLE IF EXISTS _tp_probe")
    ch(f"CREATE TABLE _tp_probe (label String, s {col_type}) ENGINE = MergeTree ORDER BY label")
    for label, sk in (("A", A), ("B", B), ("A_dup", A)):
        ch("INSERT INTO _tp_probe FORMAT RowBinary",
           data=enc_string(label) + target.state(sk.serialize_compact(), k))

    ab = int(ch(f"SELECT {target.merge('s', k)} FROM _tp_probe WHERE label IN ('A','B')"))
    abd = int(ch(f"SELECT {target.merge('s', k)} FROM _tp_probe"))
    print(f"  A u B          = {ab:<7} (truth 1500, datasketches union "
          f"{u.get_result(tgt_hll_type.HLL_8).get_estimate():.0f})")
    print(f"  A u B u A(dup) = {abd:<7} (duplicate must not inflate)")
    ok &= abs(ab - 1500) / 1500 < 0.05 and ab == abd
    print(f"  union correct and idempotent: {abs(ab - 1500) / 1500 < 0.05 and ab == abd}")

    # 4. The limitation, demonstrated rather than asserted.
    print("\n=== mixing with NATIVE states double-counts (do not do this) ===")
    ch("DROP TABLE IF EXISTS _mix_probe")
    ch(f"CREATE TABLE _mix_probe (label String, s {col_type}) ENGINE = MergeTree ORDER BY label")
    ch("INSERT INTO _mix_probe FORMAT RowBinary",
       data=enc_string("transplanted") + target.state(A.serialize_compact(), k))
    native = target.native_state("concat('k', toString(number))", k)
    ch(f"INSERT INTO _mix_probe SELECT 'native', {native} FROM numbers(1000)")
    t = int(ch(f"SELECT {target.merge('s', k)} FROM _mix_probe WHERE label='transplanted'"))
    n = int(ch(f"SELECT {target.merge('s', k)} FROM _mix_probe WHERE label='native'"))
    m = int(ch(f"SELECT {target.merge('s', k)} FROM _mix_probe"))
    ch("DROP TABLE _mix_probe")
    ch("DROP TABLE _tp_probe")
    print(f"  transplanted={t}  native={n}  merged={m}  (same 1000 values)")
    print(f"  merged is ~2x, confirming incompatibility: {m > 1.8 * 1000}")
    ok &= m > 1.8 * 1000  # the breakage is expected; if it ever stops, revisit the docs
    return bool(ok)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--target", choices=sorted(TARGETS), default="uniqhll12",
                        help="ClickHouse state type to write (see module docstring)")
    args = parser.parse_args(argv)
    target = TARGETS[args.target]

    ks = column_ks(target)
    precisions = ", ".join(f"{c}: K={k}" for (c, _), k in zip(HLL_COLS, ks))
    print(f"=== transplanting {SOURCE} HLL columns -> ClickHouse {target.table} "
          f"(target: {args.target}; {precisions}) ===")
    total = migrate(target, ks)
    ok = verify(total, target, ks)
    print(f"\n{'ALL CHECKS PASSED' if ok else 'CHECKS FAILED'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
