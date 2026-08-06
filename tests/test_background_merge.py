"""Transplanted uniqHLL12 states must survive ClickHouse's own background part merges.

The rest of the suite only ever merges states at *query* time, via uniqHLL12Merge in a
SELECT. That is a different code path from what AggregatingMergeTree does when it compacts
parts in the background, and the background path is the one that rewrites the stored bytes.
If it mishandled a transplanted state, nothing else here would notice: the numbers would
simply drift at some unpredictable later moment, long after the migration reported success.

Shape of the test, per the scenario being covered:

  1. stop merges, so inserted parts stay separate
  2. migrate in two batches -> at least two active parts
  3. record per-channel estimates (query-time merge across parts)
  4. start merges and OPTIMIZE FINAL, and confirm the parts really did collapse
  5. record the same estimates again (now a single physically-merged state)
  6. the two must agree

Step 6 asserts *exact* equality rather than a tolerance. HLL merging is a bucket-wise max,
which is associative, commutative and idempotent, so the final register array cannot depend
on the order or grouping in which states were combined, and the estimate is a deterministic
function of those registers. A tolerance here would hide precisely the corruption the test
exists to catch.

The table deliberately uses a COARSER sorting key (channel) than the migration's real target
(channel, countryName, isRobot, ts). Under the real key every source row is unique, so a part
merge would have no same-key rows to combine and would never touch a sketch -- the test would
pass while proving nothing. Sorting by channel alone forces 2486 rows down to 51, so every
surviving row is the product of ClickHouse unioning many transplanted states itself.
"""

from __future__ import annotations

import pytest

from sketch_io import decode_druid_sketch, druid_rows, enc_string
from uniqhll12 import LG_K, state_from_datasketches

pytestmark = [pytest.mark.druid, pytest.mark.clickhouse]

SOURCE = "wikipedia_rollup_sketches"
TABLE = "_t_bgmerge"
HLL_COLUMN = "users_hll_k12_hll4"

DDL = f"""
CREATE TABLE {TABLE} (
    channel String,
    users   AggregateFunction(uniqHLL12, String)
) ENGINE = AggregatingMergeTree
ORDER BY channel
"""


def active_parts(ch_query) -> int:
    return int(ch_query(
        f"SELECT count() FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{TABLE}' AND active"))


def estimates(ch_query) -> dict[str, int]:
    rows = ch_query(
        f"SELECT channel, uniqHLL12Merge(users) FROM {TABLE} GROUP BY channel ORDER BY channel"
    ).split("\n")
    return {line.split("\t")[0]: int(line.split("\t")[1]) for line in rows if line}


@pytest.fixture
def bgmerge_table(ch_query):
    ch_query(f"DROP TABLE IF EXISTS {TABLE}")
    ch_query(DDL)
    yield
    # Always re-enable merges: leaving them stopped would silently affect later tests.
    ch_query(f"SYSTEM START MERGES {TABLE}")
    ch_query(f"DROP TABLE IF EXISTS {TABLE}")


def test_hll_estimates_survive_a_background_merge(ch_query, druid_query, bgmerge_table):
    source = [
        (r[0], decode_druid_sketch(r[1]))
        for r in druid_rows(f'SELECT channel, {HLL_COLUMN} FROM {SOURCE}')
    ]
    assert len(source) > 100, "need enough rows for two meaningful batches"

    # 1. Stop merges so the two batches cannot be compacted behind our back.
    ch_query(f"SYSTEM STOP MERGES {TABLE}")

    # 2. Migrate in two batches -> two parts.
    midpoint = len(source) // 2
    for batch in (source[:midpoint], source[midpoint:]):
        payload = bytearray()
        for channel, sketch in batch:
            payload += enc_string(channel or "") + state_from_datasketches(sketch)
        ch_query(f"INSERT INTO {TABLE} FORMAT RowBinary", data=bytes(payload))

    parts_before = active_parts(ch_query)
    assert parts_before >= 2, (
        f"expected the two batches to leave separate parts, found {parts_before}; "
        "SYSTEM STOP MERGES did not take effect and the test would be vacuous")

    # Both batches must contain the same channels, or a merge has no states to union.
    first_channels = {c for c, _ in source[:midpoint]}
    second_channels = {c for c, _ in source[midpoint:]}
    assert first_channels & second_channels, "batches share no key; merge would be a no-op"

    # 3. Estimates while the data is still spread across parts.
    before = estimates(ch_query)
    assert before, "no rows came back"

    # 4. Re-enable merges and force one, then prove it actually happened.
    ch_query(f"SYSTEM START MERGES {TABLE}")
    ch_query(f"OPTIMIZE TABLE {TABLE} FINAL SETTINGS optimize_throw_if_noop = 1")
    parts_after = active_parts(ch_query)
    assert parts_after == 1, f"expected a single part after OPTIMIZE FINAL, found {parts_after}"
    assert parts_after < parts_before, "no merge took place, so nothing was verified"

    # The merge must also have collapsed same-key rows, i.e. really combined sketch states.
    rows_after = int(ch_query(f"SELECT count() FROM {TABLE}"))
    assert rows_after == len(before), (
        f"expected one row per channel after merging, found {rows_after}")
    assert rows_after < len(source), "no rows were collapsed; no states were unioned"

    # 5 + 6. Same estimates, now from physically merged states.
    after = estimates(ch_query)
    assert after == before, (
        "background merge changed the HLL estimates: "
        + ", ".join(f"{ch} {before[ch]} -> {after[ch]}"
                    for ch in sorted(before) if before.get(ch) != after.get(ch))[:400])


def test_merged_estimates_still_track_druid(ch_query, druid_query, bgmerge_table):
    """A merge that corrupted every state identically would satisfy before == after. Anchor
    the post-merge numbers against Druid so the invariant cannot be met by uniform garbage."""
    source = [
        (r[0], decode_druid_sketch(r[1]))
        for r in druid_rows(f'SELECT channel, {HLL_COLUMN} FROM {SOURCE}')
    ]
    ch_query(f"SYSTEM STOP MERGES {TABLE}")
    midpoint = len(source) // 2
    for batch in (source[:midpoint], source[midpoint:]):
        payload = bytearray()
        for channel, sketch in batch:
            payload += enc_string(channel or "") + state_from_datasketches(sketch)
        ch_query(f"INSERT INTO {TABLE} FORMAT RowBinary", data=bytes(payload))

    ch_query(f"SYSTEM START MERGES {TABLE}")
    ch_query(f"OPTIMIZE TABLE {TABLE} FINAL SETTINGS optimize_throw_if_noop = 1")
    after = estimates(ch_query)

    druid = {r["channel"]: r["u"] for r in druid_query(
        f"SELECT channel, APPROX_COUNT_DISTINCT_DS_HLL({HLL_COLUMN}, {LG_K}) AS u "
        f"FROM {SOURCE} GROUP BY 1 ORDER BY u DESC LIMIT 8")}

    for channel, expected in druid.items():
        got = after[channel]
        # Loose: ClickHouse applies its own bias correction to the same registers. This is a
        # "not garbage" check, not a precision check -- that lives in migrate_hll_transplant.
        assert abs(got - expected) / expected < 0.10, (
            f"{channel}: clickhouse={got} druid={expected}")
