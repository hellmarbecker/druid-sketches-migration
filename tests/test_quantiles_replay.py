"""Replaying a DataSketches quantiles sketch as (value, weight) pairs.

No server needed. The replay is the whole of the quantiles migration's lossy step, so these
pin it directly rather than through ClickHouse: total mass, sample count, monotonicity, and
that the reconstructed distribution answers quantile queries the same way the source does.

Quantile comparisons are made in rank space, never by value. On tied data a quantile is an
interval of ranks rather than a point -- the fixture puts 15% of its mass on the single value
18.0 -- so two libraries can return different, equally correct medians. Comparing values
would be testing which convention each picked at a discontinuity.
"""

from __future__ import annotations

import math
import random

import pytest
from datasketches import quantiles_doubles_sketch

from migrate_quantiles import MAX_SAMPLES, RANK_BAND, replay, valid_quantile

LEVELS = (0.1, 0.25, 0.5, 0.75, 0.9, 0.99)


def sketch(values, k=256):
    s = quantiles_doubles_sketch(k)
    for v in values:
        s.update(float(v))
    return s


def quantile_of_replay(pairs, level):
    """Quantile of the reconstructed weighted sample, computed independently of the sketch."""
    total = sum(w for _, w in pairs)
    target = level * total
    seen = 0
    for value, weight in sorted(pairs):
        seen += weight
        if seen >= target:
            return value
    return sorted(pairs)[-1][0]


def test_empty_sketch_replays_to_nothing():
    assert replay(sketch([])) == []


@pytest.mark.parametrize("n", [1, 2, 3, 17, 999, 1000, 1001, 5000, 40000])
def test_total_weight_equals_n(n):
    """The replayed mass must equal the number of observations the sketch saw, or every
    downstream weighted quantile is computed against the wrong denominator."""
    pairs = replay(sketch(range(n)))
    assert sum(w for _, w in pairs) == n
    assert all(w > 0 for _, w in pairs), "a zero-weight sample carries no information"


@pytest.mark.parametrize("n", [1, 17, 999, 1000, 1001, 40000])
def test_sample_count_is_capped_but_never_padded(n):
    """Below the cap there is one sample per observation, so the replay is exact; above it
    the sketch is summarised rather than expanded."""
    pairs = replay(sketch(range(n)))
    assert len(pairs) == min(n, MAX_SAMPLES)


@pytest.mark.parametrize("data", [
    [3.0, 1.0, 4.0, 1.5, 9.0, 2.6, 5.0],
    [float(i) for i in range(500)],
    [-7.0, 0.0, 7.0],
    [2.5],
])
def test_replay_is_exact_below_the_cap(data):
    """With distinct values and n <= MAX_SAMPLES the sketch is exact, and the replay returns
    the original observations -- not an approximation of them. This is the strongest thing
    the quantiles path has going for it: at the fixture's grain (~16 values per rollup row)
    the migration is carrying the actual data.

    It does NOT pin the midpoint rule, despite appearances. Sampling at rank i/m instead of
    (i+0.5)/m still reconstructs small sketches exactly, and on large ones shifts the result
    by half a slice -- 0.0005 in rank at m=1000, measured, which is far below both the
    sketch's own error and RANK_BAND. The midpoint is the right convention for representing
    equal-mass slices, but nothing here would catch its absence, and no test should claim to.
    """
    pairs = replay(sketch(data))
    expanded = sorted(v for v, w in pairs for _ in range(w))
    assert expanded == sorted(data)


def test_values_are_non_decreasing():
    """Samples come from the quantile function, which is monotone by definition. A violation
    would mean the ranks were built or ordered wrongly."""
    pairs = replay(sketch(random.Random(4).sample(range(1_000_000), 5000)))
    values = [v for v, _ in pairs]
    assert values == sorted(values)


@pytest.mark.parametrize("distribution,label", [
    (list(range(10_000)), "uniform"),
    ([i * i for i in range(5_000)], "quadratic, heavy right tail"),
    ([1.0] * 4_000 + [2.0] * 1_000 + [3.0] * 5_000, "three values, heavy ties"),
    ([42.0] * 1_000, "constant"),
    ([-500.0 + i for i in range(1_000)], "negative values"),
])
def test_replay_answers_quantiles_like_the_source(distribution, label):
    """The reconstructed distribution must answer quantile queries as the source sketch does.

    This is the property the migration depends on; everything downstream is ClickHouse's
    t-digest doing its own approximation on top.
    """
    src = sketch(distribution)
    pairs = replay(src)
    for level in LEVELS:
        got = quantile_of_replay(pairs, level)
        inside, lo, hi = valid_quantile(src, got, level)
        assert inside, (
            f"{label}: p{int(level*100)} replayed as {got}, valid only for ranks "
            f"[{lo:.4f},{hi:.4f}]")


def test_extremes_are_not_double_counted():
    """Regression guard for a fix that made things worse.

    Pinning get_min_value()/get_max_value() as extra samples looks prudent -- the sampled
    ranks stop short of 0 and 1 -- but the outermost samples already stand for those slices,
    so adding them injects mass rather than replacing it and inflates the tail. On the
    fixture it moved the merged p99 from 3793 to 5487 against an exact 3813. The replay must
    therefore emit exactly one sample per rank slice and no extras.
    """
    n = 5_000
    src = sketch([i * i for i in range(n)])           # heavy right tail
    pairs = replay(src)

    assert len(pairs) == MAX_SAMPLES, "extra anchor samples would push the count past the cap"
    assert sum(w for _, w in pairs) == n

    # The largest sample should sit near, not at, the maximum: it represents the top slice
    # of mass rather than the single most extreme observation.
    largest = max(v for v, _ in pairs)
    assert largest <= src.get_max_value()
    assert src.get_rank(largest) >= 1.0 - 2.0 / MAX_SAMPLES


def test_rank_interval_is_tie_aware():
    """valid_quantile must accept any value in a tie's rank interval, since that is what
    'quantile' means on tied data -- and reject one outside it."""
    src = sketch([1.0] * 40 + [2.0] * 20 + [3.0] * 40)
    # 2.0 spans ranks [0.4, 0.6]; both 0.45 and 0.55 are legitimately its quantile levels.
    for level in (0.45, 0.5, 0.55):
        inside, lo, hi = valid_quantile(src, 2.0, level, band=0.0)
        assert inside, f"2.0 should be a valid p{level} quantile, interval [{lo},{hi}]"
    # 1.0 spans [0.0, 0.4], so it is not a median of this distribution.
    inside, _, _ = valid_quantile(src, 1.0, 0.5, band=0.0)
    assert not inside, "1.0 must not pass as the median here"


def test_get_rank_convention_is_exclusive():
    """valid_quantile relies on get_rank being P(X < v), taking the inclusive side from the
    next representable double. If DataSketches ever changed that, the interval would collapse
    and tied comparisons would start failing for the wrong reason."""
    src = sketch([1.0] * 40 + [2.0] * 20 + [3.0] * 40)
    assert src.get_rank(2.0) == pytest.approx(0.4)
    assert src.get_rank(math.nextafter(2.0, math.inf)) == pytest.approx(0.6)


def test_rank_band_is_wider_than_the_sketch_error():
    """The band exists to absorb tie ambiguity and t-digest interpolation, so it has to
    exceed the sketch's own rank error or it would be measuring the wrong thing."""
    src = sketch(range(10_000))
    assert RANK_BAND > src.normalized_rank_error(False)
