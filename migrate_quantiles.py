#!/usr/bin/env python
"""Quantiles path: Druid DataSketches quantiles -> ClickHouse.

ClickHouse has no DataSketches quantiles or KLL, so there is no byte-level transcode --
the same starting point as HLL. But quantiles turn out to be the *easy* case, because of
what the sketch keeps: a quantiles sketch retains actual data values, where an HLL sketch
retains only a max rank per bucket and has thrown the values away. Values can be replayed;
hashes cannot.

That single difference removes the constraint the HLL transplant lives under. A state built
here from replayed values is a genuine native ClickHouse t-digest, so it **merges correctly
with states built from raw data** -- no double counting, no "never mix" rule. Measured in
verify().

Two things are written per row:

  1. The sketch bytes verbatim in a String column. Lossless at rest, and re-readable by the
     Python binding, so nothing is foreclosed.
  2. A native AggregateFunction(quantileTDigestWeighted, Float64, UInt64), built by replaying
     the sketch's quantile function as (value, weight) pairs. Queryable and mergeable with
     plain SQL, no UDF.

ClickHouse builds the t-digest itself from those pairs rather than us constructing its state
bytes -- that keeps the result a real native state and avoids reverse-engineering a second
undocumented format (contrast uniqhll12.py, where there was no such option).

On accuracy, measured against exact quantiles computed from the raw wikiticker file:

    level   exact     druid k=256     this migration
    p50      18.0     18.0            18.0
    p90     356.0     343.0  (-3.7%)  355.3  (-0.2%)
    p99    3813.0    3191.0 (-16.3%)  3793.2 (-0.5%)

The migration is *more* accurate than the source it reads. That is not a trick: per-row
sketches here hold ~16 values against k=256, so they are exact, and the replay hands
ClickHouse what amounts to the original data. Druid's number comes from merging 2486
k=256 sketches, which is where the tail resolution goes. The practical consequence is that
Druid's own answer must not be treated as ground truth when checking this path -- an
earlier version of verify() compared against it by percentage and failed the better answer.

Run:  .venv/bin/python migrate_quantiles.py
Needs Druid + ClickHouse running (see CLAUDE.md).
"""

from __future__ import annotations

import math
import sys

from datasketches import kll_doubles_sketch, quantiles_doubles_sketch

from sketch_io import (
    ch, decode_druid_sketch, druid_one, druid_rows, enc_bytes, enc_datetime_iso,
    enc_float64, enc_nullable_string, enc_string, enc_uint64, esc,
)

SOURCE = "wikipedia_rollup_sketches"
TARGET = "wikipedia_rollup_quantiles"
STAGING = "_quantiles_staging"

CLASSIC_COL = "added_quantiles_k256"
KLL_COL = "added_kll_k200"

# How finely to sample the quantile function when replaying. The cap only bites on sketches
# holding more values than this; below it the replay is one sample per value, i.e. exact.
# 1000 samples is 0.1% rank resolution, comfortably finer than the sketches' own error
# (0.717% at classic k=256), so the discretisation is not the limiting term.
MAX_SAMPLES = 1000

# How far either side of a target rank the answer may fall before it counts as wrong. This is
# a correctness band, not a precision claim: on tied data the quantile *value* is ambiguous
# over a whole rank interval, so the band has to be wider than the sketch's own 0.717% rank
# error. It is still narrow enough that a distribution reconstructed wrongly would escape it.
RANK_BAND = 0.05

def valid_quantile(sketch, value: float, level: float, band: float = RANK_BAND):
    """Is `value` a legitimate `level`-quantile of the distribution `sketch` describes?

    The textbook definition is P(X < v) <= L <= P(X <= v). On tied data those two differ, so
    a quantile is an *interval* of ranks rather than a point -- which is precisely why
    comparing quantile values across two libraries fails on this dataset. The fixture's
    merged distribution puts 15% of its mass on the single value 18.0, giving it the rank
    interval [0.3951, 0.5444]; both 18.0 (t-digest) and 32.0 (DataSketches) are defensible
    medians, and the exact median of the raw data is in fact 18.0.

    DataSketches' get_rank(v) is the exclusive side, P(X < v), so the inclusive side needs
    the next representable double. `band` then allows for the sketch's own rank error.
    """
    lower = sketch.get_rank(value)
    upper = sketch.get_rank(math.nextafter(value, math.inf))
    return (lower - band <= level <= upper + band), lower, upper


DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    ts             DateTime,
    channel        String,
    countryName    Nullable(String),
    isRobot        String,
    cnt            SimpleAggregateFunction(sum, UInt64),
    -- Lossless carry. `any` because every staging row in a group repeats the same bytes;
    -- a plain String would be rejected by AggregatingMergeTree.
    classic_sketch SimpleAggregateFunction(any, String),
    kll_sketch     SimpleAggregateFunction(any, String),
    -- Native, mergeable, built by ClickHouse from the replayed sample.
    added_tdigest  AggregateFunction(quantileTDigestWeighted, Float64, UInt64)
) ENGINE = AggregatingMergeTree
ORDER BY (channel, countryName, isRobot, ts)
SETTINGS allow_nullable_key = 1
"""

STAGING_DDL = f"""
CREATE TABLE IF NOT EXISTS {STAGING} (
    ts DateTime, channel String, countryName Nullable(String), isRobot String,
    cnt UInt64, classic_sketch String, kll_sketch String,
    value Float64, weight UInt64
) ENGINE = MergeTree ORDER BY (channel, ts) SETTINGS allow_nullable_key = 1
"""


def replay(sketch) -> list[tuple[float, int]]:
    """Turn a quantiles sketch back into (value, weight) pairs totalling n.

    Sampled from the quantile function at evenly spaced ranks rather than read out of the
    sketch's internal levels, because the Python binding exposes no accessor for retained
    items and their weights -- only get_quantiles/get_pmf/get_rank. Sampling the quantile
    function is equivalent for reconstructing a distribution and does not depend on the
    internal layout of either sketch family.

    Weights are integers summing to exactly n, so the replayed total mass matches the source.
    """
    n = sketch.n
    if n == 0:
        return []
    # Ranks (i+0.5)/m tile [0,1] evenly, so each sample stands for an equal slice of mass and
    # the set spans the whole distribution, extremes included.
    #
    # Do NOT additionally pin get_min_value()/get_max_value() as extra samples. It looks like
    # a safeguard -- the sampled ranks stop short of 0 and 1 -- but the outermost samples
    # already represent those slices, so adding the extremes injects mass on top rather than
    # replacing any, over-weighting the tail. Measured on the fixture: doing so moved the
    # merged p99 from 3793 to 5487 against an exact value of 3813.
    m = min(n, MAX_SAMPLES)
    ranks = [(i + 0.5) / m for i in range(m)]
    values = sketch.get_quantiles(ranks)

    base, extra = divmod(n, m)
    return [(float(v), base + (1 if i < extra else 0)) for i, v in enumerate(values)]


def migrate() -> int:
    ch(f"DROP TABLE IF EXISTS {TARGET}")
    ch(f"DROP TABLE IF EXISTS {STAGING}")
    ch(DDL)
    ch(STAGING_DDL)

    cols = ["__time", "channel", "countryName", "isRobot", '"count"', CLASSIC_COL, KLL_COL]
    insert = f"INSERT INTO {STAGING} FORMAT RowBinary"

    batch, rows, samples = bytearray(), 0, 0
    for ts, channel, country, is_robot, cnt, classic_b64, kll_b64 in druid_rows(
            f"SELECT {', '.join(cols)} FROM {SOURCE}"):
        classic_bytes = decode_druid_sketch(classic_b64)
        kll_bytes = decode_druid_sketch(kll_b64)
        # Replay from the classic sketch; the KLL bytes ride along for the lossless carry.
        pairs = replay(quantiles_doubles_sketch.deserialize(classic_bytes))

        prefix = (enc_datetime_iso(ts) + enc_string(channel or "")
                  + enc_nullable_string(country) + enc_string(is_robot or "")
                  + enc_uint64(cnt) + enc_bytes(classic_bytes) + enc_bytes(kll_bytes))
        for value, weight in pairs:
            batch += prefix + enc_float64(value) + enc_uint64(weight)
        rows += 1
        samples += len(pairs)

        if len(batch) > 8_000_000:
            ch(insert, data=bytes(batch))
            batch.clear()
            print(f"  ... {rows} rows / {samples} samples")
    if batch:
        ch(insert, data=bytes(batch))

    # ClickHouse builds the t-digest itself, so the state is a real native one.
    ch(f"""INSERT INTO {TARGET}
           SELECT ts, channel, countryName, isRobot,
                  any(cnt), any(classic_sketch), any(kll_sketch),
                  quantileTDigestWeightedState(value, weight)
           FROM {STAGING}
           GROUP BY ts, channel, countryName, isRobot""")
    ch(f"DROP TABLE {STAGING}")
    print(f"  migrated {rows} rows from {samples} replayed samples")
    return rows


def verify(migrated: int) -> bool:
    ok = True
    print("\n=== row count ===")
    src = druid_one(f"SELECT COUNT(*) FROM {SOURCE}")[0]
    dst = int(ch(f"SELECT count() FROM {TARGET}"))
    ok &= src == dst == migrated
    print(f"  druid={src}  clickhouse={dst}  match={src == dst == migrated}")

    # 1. The carried bytes must still be a readable sketch.
    print("\n=== carried bytes round-trip ===")
    for label, col, cls in (("classic", "classic_sketch", quantiles_doubles_sketch),
                            ("kll", "kll_sketch", kll_doubles_sketch)):
        hexed = ch(f"SELECT hex({col}) FROM {TARGET} ORDER BY cnt DESC LIMIT 1")
        s = cls.deserialize(bytes.fromhex(hexed))
        print(f"  {label:<8} bytes={len(hexed)//2:<6} k={s.k} n={s.n} "
              f"p50={s.get_quantile(0.5)} readable=True")
        ok &= s.n > 0

    # 2. Replayed quantiles must track the source distribution at the stored grain.
    #
    #    Compared in RANK space, not by value. A quantile value is not well defined on a
    #    distribution with heavy ties, and this data has plenty: one rollup row is 43% the
    #    value 18 and 47% the value 36, so p50 falls in the gap between them. DataSketches
    #    returns a retained value from the gap (32); t-digest interpolates and returns 18.
    #    Neither is wrong. Requiring the values to match would be testing which convention
    #    each library picked at a discontinuity, not whether the migration preserved the
    #    distribution. So the check is that ClickHouse's answer lies between the source
    #    sketch's own quantiles a little either side of the target rank.
    print("\n=== per-row quantiles: replayed t-digest vs the source sketch (rank band) ===")
    top = ch(f"SELECT channel, isNull(countryName), ifNull(countryName,''), isRobot, "
             f"toUnixTimestamp(ts) * 1000, cnt, hex(classic_sketch) "
             f"FROM {TARGET} ORDER BY cnt DESC LIMIT 5").split("\n")
    for line in top:
        channel, cnull, country, is_robot, millis, cnt, hexed = line.split("\t")
        cond = (f"channel = {esc(channel)} AND isRobot = {esc(is_robot)} "
                f"AND toUnixTimestamp(ts) * 1000 = {millis} AND "
                + ("countryName IS NULL" if cnull == "1" else f"countryName = {esc(country)}"))
        # The carried bytes are Druid's own, so this is the source distribution itself.
        src = quantiles_doubles_sketch.deserialize(bytes.fromhex(hexed))

        got = [float(x) for x in ch(
            f"SELECT quantileTDigestWeightedMerge(0.5)(added_tdigest), "
            f"       quantileTDigestWeightedMerge(0.9)(added_tdigest) "
            f"FROM {TARGET} WHERE {cond}").split("\t")]

        parts = []
        for level, value in zip((0.5, 0.9), got):
            inside, lo, hi = valid_quantile(src, value, level)
            ok &= inside
            parts.append(f"p{int(level*100)}={value:.1f} rank[{lo:.3f},{hi:.3f}] "
                         f"{'ok' if inside else 'OUT'}")
        print(f"  {channel:<16} n={cnt:<6} " + "  ".join(parts))

    # 3. Merged across every row.
    #
    #    Checked in rank space against the source distribution, not as a percentage against
    #    Druid's own answer -- Druid's merged k=256 sketch is not ground truth. Measured
    #    against the raw wikiticker file, its p99 of 3191 sits 16% below the exact 3813,
    #    because merging per-row sketches at k=256 loses tail resolution. The replayed
    #    t-digest lands at 3793, i.e. nearer the truth than the source it came from. A
    #    percentage comparison against Druid would therefore have failed the more accurate
    #    answer. Druid's number is printed for context only.
    print("\n=== whole-datasource merge (rank-space check against the source) ===")
    levels = (0.5, 0.9, 0.99)
    got = [float(x) for x in ch(
        "SELECT " + ", ".join(f"quantileTDigestWeightedMerge({l})(added_tdigest)" for l in levels)
        + f" FROM {TARGET}").split("\t")]
    druid_vals = druid_one(
        "SELECT " + ", ".join(
            f"DS_GET_QUANTILE(DS_QUANTILES_SKETCH({CLASSIC_COL}, 256), {l})" for l in levels)
        + f" FROM {SOURCE}")
    merged_src = quantiles_doubles_sketch.deserialize(decode_druid_sketch(druid_one(
        f"SELECT DS_QUANTILES_SKETCH({CLASSIC_COL}, 256) FROM {SOURCE}")[0]))

    for level, value, dv in zip(levels, got, druid_vals):
        inside, lo, hi = valid_quantile(merged_src, value, level)
        ok &= inside
        print(f"  p{int(level*100):<3} clickhouse={value:<11.1f} valid for ranks "
              f"[{lo:.4f},{hi:.4f}], target {level}: {inside}   [druid says {dv}]")

    # 4. The property that makes this path unlike the HLL transplant: a replayed state and a
    #    state built from raw values describe the same distribution, so merging them is sound.
    print("\n=== replayed states merge with natively-built ones ===")
    ch("DROP TABLE IF EXISTS _q_mixcheck")
    ch("CREATE TABLE _q_mixcheck (label String, "
       "s AggregateFunction(quantileTDigestWeighted, Float64, UInt64)) "
       "ENGINE = MergeTree ORDER BY label")
    try:
        # Same underlying distribution, reached two different ways.
        sk = quantiles_doubles_sketch(256)
        for i in range(10000):
            sk.update(float(i))
        pairs = replay(sk)
        values = ",".join(f"({v},{w})" for v, w in pairs)
        ch(f"INSERT INTO _q_mixcheck SELECT 'replayed', "
           f"quantileTDigestWeightedState(t.1, toUInt64(t.2)) "
           f"FROM (SELECT arrayJoin([{values}]) AS t)")
        ch("INSERT INTO _q_mixcheck SELECT 'native', "
           "quantileTDigestWeightedState(toFloat64(number), toUInt64(1)) FROM numbers(10000)")

        rep = float(ch("SELECT quantileTDigestWeightedMerge(0.5)(s) FROM _q_mixcheck WHERE label='replayed'"))
        nat = float(ch("SELECT quantileTDigestWeightedMerge(0.5)(s) FROM _q_mixcheck WHERE label='native'"))
        mix = float(ch("SELECT quantileTDigestWeightedMerge(0.5)(s) FROM _q_mixcheck"))
        # Truth is 4999.5. A coordinate-space mismatch, as HLL suffers, would show up as a
        # merged value far outside the two inputs; here it must land between them.
        sane = min(rep, nat) - 1 <= mix <= max(rep, nat) + 1 and abs(mix - 4999.5) < 250
        ok &= sane
        print(f"  replayed p50={rep:.1f}  native p50={nat:.1f}  merged p50={mix:.1f} "
              f"(truth 4999.5)  sound={sane}")
        print("  -> unlike uniqHLL12, no 'never mix with native states' rule applies here")
    finally:
        ch("DROP TABLE IF EXISTS _q_mixcheck")
    return bool(ok)


def main() -> int:
    print(f"=== migrating {SOURCE} quantiles columns -> ClickHouse {TARGET} ===")
    rows = migrate()
    ok = verify(rows)
    print(f"\n{'ALL CHECKS PASSED' if ok else 'CHECKS FAILED'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
