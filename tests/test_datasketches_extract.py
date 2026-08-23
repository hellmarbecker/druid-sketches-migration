"""Reading registers and parameters out of DataSketches HLL sketches.

No server needed: sketches are built in-process with the same library version that produced
Druid's bytes (cross-language compatibility is settled -- see CLAUDE.md).

The load-bearing property is that the sparse (coupon) and dense (byte-array) extraction paths
agree, because Druid's stored rollup sketches are usually sparse while merges are dense, and
a transplant has to handle both identically.
"""

from __future__ import annotations

import pytest
from datasketches import hll_sketch, hll_union, tgt_hll_type

from sketch_io import hll_lg_k
from uniqhll12 import (
    BUCKETS, LG_K, MAX_RANK, datasketches_registers, decode_registers, state_from_datasketches,
)


def sketch(items, lg_k=LG_K, tgt=tgt_hll_type.HLL_8):
    s = hll_sketch(lg_k, tgt)
    for i in items:
        s.update(i)
    return s


def test_lg_k_read_from_preamble():
    """Per-column lgK varies in the fixture (12 and 14); it must be read, never assumed."""
    for lg in (4, 12, 14, 21):
        assert hll_lg_k(sketch(range(10), lg_k=lg).serialize_compact()) == lg


def test_lg_k_rejects_non_hll_family():
    from datasketches import update_theta_sketch
    theta = update_theta_sketch(12)
    theta.update("x")
    with pytest.raises(ValueError, match="not an HLL sketch"):
        hll_lg_k(theta.compact().serialize())


# "Sparse" is two distinct representations, not one, and LIST is the one that dominates real
# rollup data: in the fixture it is 84% of the users column and 74% of pages, while dense
# never occurs at all. Both are exercised below, with n chosen either side of the promotion
# threshold -- at lgK=12 a sketch is LIST up to 7 distinct values and SET from 8.
LIST_N, SET_N = 7, 100
DENSE_RANGE = range(1000, 20000)

# The coupon array is a hash table, so coupons sit at scattered positions rather than packed
# at the front -- which means the final slot is usually padding. At n=15 it happens to hold a
# real coupon, so this case is what catches a parser that stops one slot short. Without it,
# truncating the loop by one passes every other case (verified by mutation).
SET_LAST_SLOT_N = 15


def test_sparse_modes_are_where_the_tests_assume():
    """Guards the premise of everything below. If DataSketches moved the LIST/SET promotion
    threshold, the mode-specific tests would silently all run against the same code path."""
    assert sketch(range(LIST_N)).serialize_compact()[7] & 0x03 == 0, "expected LIST mode"
    assert sketch(range(SET_N)).serialize_compact()[7] & 0x03 == 1, "expected SET mode"
    assert sketch(DENSE_RANGE).serialize_compact()[7] & 0x03 == 2, "expected dense HLL mode"


@pytest.mark.parametrize("mode,n", [("LIST", LIST_N), ("SET", SET_N),
                                    ("SET last slot occupied", SET_LAST_SLOT_N)])
def test_sparse_and_dense_extraction_agree(mode, n):
    """registers(A u B) must equal the elementwise max of the two register arrays -- with A
    taken through the coupon path and B through the dense path.

    Run for both sparse representations. They differ in preamble size (LIST is preInts=2,
    SET is preInts=3), which is exactly the offset the coupon parser has to get right.
    """
    a, b = sketch(range(n)), sketch(DENSE_RANGE)
    u = hll_union(LG_K)
    u.update(a)
    u.update(b)

    ra = datasketches_registers(a.serialize_compact())
    rb = datasketches_registers(b.serialize_compact())
    ru = datasketches_registers(u.get_result(tgt_hll_type.HLL_8).serialize_compact())
    assert [max(x, y) for x, y in zip(ra, rb)] == ru
    assert sum(1 for v in ra if v), f"{mode} sketch set no registers"


def read_coupons(serialized: bytes) -> list[int]:
    """Independent re-read of a sparse sketch's coupon array, written from the format spec
    rather than by calling the parser under test: preInts*4 bytes of preamble, then UInt32
    coupons. Empty slots are zero (updatable serialisations are padded out to their full
    allocation), and a real coupon can never be zero because rank 0 is not a valid value.
    """
    pre = (serialized[0] & 0x3F) * 4
    words = [int.from_bytes(serialized[pre + 4 * i:pre + 4 * i + 4], "little")
             for i in range((len(serialized) - pre) // 4)]
    return [c for c in words if c]


@pytest.mark.parametrize("mode,n", [("LIST", LIST_N), ("SET", SET_N),
                                    ("SET last slot occupied", SET_LAST_SLOT_N)])
def test_sparse_extraction_finds_every_coupon(mode, n):
    """Every coupon in the sketch must land in a register.

    Checked against an independent read of the same bytes rather than against n, because
    coupons collide: 100 distinct values occupy fewer than 100 registers once two of them
    share a bucket. Comparing to n would either fail on collisions or need a fudge factor
    loose enough to hide a parser dropping coupons -- which is the bug this exists to catch,
    since a wrong preamble offset loses the leading coupon and still looks plausible.
    """
    compact = sketch(range(n)).serialize_compact()
    coupons = read_coupons(compact)
    assert len(coupons) == n, f"{mode}: expected one coupon per distinct value"

    expected_indices = {c & (BUCKETS - 1) for c in coupons}
    regs = datasketches_registers(compact)
    assert {i for i, v in enumerate(regs) if v} == expected_indices, (
        f"{mode}: registers set do not match the coupons present")


def test_updatable_padding_is_not_mistaken_for_coupons():
    """Sparse updatable sketches pad their coupon array to the full allocation -- n=1
    occupies 1 of 8 slots -- and datasketches_registers() parses updatable bytes, because it
    normalises through hll_union. This pins that the registers it produces are exactly the
    ones the real coupons call for, with padding contributing nothing.

    Note the skip in the parser is defensive rather than load-bearing *here*: a zero word
    decodes to index 0 with rank 0, and max(register, 0) is a no-op, so removing the skip
    changes no output (verified by mutation). It is load-bearing in the SQL transplant,
    which counts map entries to derive the histogram -- there a stray zero coupon would put
    index 0 in the map and leave the counters summing to 4095.
    """
    for n in (1, LIST_N, SET_N):
        u = hll_union(LG_K)
        u.update(sketch(range(n)))
        updatable = u.get_result(tgt_hll_type.HLL_8).serialize_updatable()

        pre = (updatable[0] & 0x3F) * 4
        slots = (len(updatable) - pre) // 4
        coupons = read_coupons(updatable)
        assert slots > len(coupons), (
            f"n={n}: expected padding, got {slots} slots for {len(coupons)} coupons")

        regs = datasketches_registers(sketch(range(n)).serialize_compact())
        set_indices = {i for i, v in enumerate(regs) if v}
        assert set_indices == {c & (BUCKETS - 1) for c in coupons}
        assert 0 not in set_indices or any(c & (BUCKETS - 1) == 0 for c in coupons), (
            f"n={n}: register 0 was set with no coupon mapping to it -- padding was counted")


def test_extraction_returns_full_register_array():
    for n in (1, 17, 100, 5000, 100000):
        regs = datasketches_registers(sketch(range(n)).serialize_compact())
        assert len(regs) == BUCKETS
        assert all(v >= 0 for v in regs)
        assert any(regs), "a non-empty sketch must set at least one register"


def test_empty_sketch_yields_all_zero_registers():
    assert datasketches_registers(sketch([]).serialize_compact()) == [0] * BUCKETS


@pytest.mark.parametrize("tgt", [tgt_hll_type.HLL_4, tgt_hll_type.HLL_6, tgt_hll_type.HLL_8])
def test_all_target_types_extract_identically(tgt):
    """HLL_4 stores nibbles offset by curMin plus an exceptions table, HLL_6 is bit-packed.
    Normalising to HLL_8 first must make target type irrelevant to the result."""
    items = range(50000)
    reference = datasketches_registers(sketch(items, tgt=tgt_hll_type.HLL_8).serialize_compact())
    assert datasketches_registers(sketch(items, tgt=tgt).serialize_compact()) == reference


def test_larger_lg_k_is_folded_to_clickhouse_precision():
    """The fixture's pages column is lgK=14; ClickHouse is fixed at 12, so it must fold --
    and folding must go through hll_union, not by dropping index bits."""
    s = sketch(range(20000), lg_k=14)
    assert hll_lg_k(s.serialize_compact()) == 14
    regs = datasketches_registers(s.serialize_compact())
    assert len(regs) == BUCKETS == 4096


def test_folding_preserves_the_estimate_within_error():
    """Folding 14 -> 12 costs precision but must not shift the estimate materially."""
    s = sketch(range(20000), lg_k=14)
    folded = hll_union(LG_K)
    folded.update(s)
    before = s.get_estimate()
    after = folded.get_result(tgt_hll_type.HLL_8).get_estimate()
    assert abs(after - before) / before < 0.05


def test_state_from_datasketches_is_a_valid_clickhouse_state():
    s = sketch(range(50000))
    state = state_from_datasketches(s.serialize_compact())
    expected = [min(v, MAX_RANK) for v in datasketches_registers(s.serialize_compact())]
    assert decode_registers(state) == expected


def test_transplant_clamps_out_of_range_ranks():
    """Nothing above 21 may reach the state, whatever the source sketch holds."""
    state = state_from_datasketches(sketch(range(200000)).serialize_compact())
    assert max(decode_registers(state)) <= MAX_RANK
