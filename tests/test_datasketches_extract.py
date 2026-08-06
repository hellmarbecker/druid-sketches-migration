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


def test_sparse_and_dense_sources_stay_sparse_and_dense():
    """Guards the premise of the next test: if both sketches took the same code path, the
    agreement check below would prove nothing."""
    sparse = sketch(range(100)).serialize_compact()
    dense = sketch(range(1000, 20000)).serialize_compact()
    assert sparse[7] & 0x03 != 2, "expected LIST/SET mode for a small sketch"
    assert dense[7] & 0x03 == 2, "expected dense HLL mode for a large sketch"


def test_sparse_and_dense_extraction_agree():
    """registers(A u B) must equal the elementwise max of the two register arrays -- with A
    taken through the coupon path and B through the dense path."""
    a, b = sketch(range(100)), sketch(range(1000, 20000))
    u = hll_union(LG_K)
    u.update(a)
    u.update(b)

    ra = datasketches_registers(a.serialize_compact())
    rb = datasketches_registers(b.serialize_compact())
    ru = datasketches_registers(u.get_result(tgt_hll_type.HLL_8).serialize_compact())
    assert [max(x, y) for x, y in zip(ra, rb)] == ru


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
