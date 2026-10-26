"""The uniqCombined64(K) codec, offline.

Same split as test_uniqhll12.py: these tests pin the codec's invariants without a server, and
the byte-exact check against states ClickHouse produced lives in test_integration.py.

What is new relative to uniqHLL12 is that the layout varies with K -- register count, rank
ceiling, histogram width, and the zero counter's width, which changes at K=16. So most tests
here run across K rather than at one fixed precision.
"""

from __future__ import annotations

import random
import struct

import pytest
from datasketches import hll_sketch, hll_union, tgt_hll_type

from uniqcombined64 import (
    INTHASH64_SQL_SALT, LARGE, MAX_K, MIN_K, datasketches_registers, decode_registers,
    decode_trailer, encode_state, int_hash64, layout, native_registers,
    state_from_datasketches, target_k,
)

ALL_K = list(range(MIN_K, MAX_K + 1))
# Spread across the zero-counter switch at 16, without paying for K=20's million registers
# in every test.
SOME_K = [12, 14, 15, 16, 17]

# State sizes measured from ClickHouse 26.10.1.288 (uniqCombined64State over enough values
# to reach the LARGE form). Hard-coded rather than recomputed so a wrong formula in layout()
# cannot pass by agreeing with itself.
MEASURED_STATE_BYTES = {
    12: 3291, 13: 6359, 14: 12499, 15: 24783, 16: 49357,
    17: 98505, 18: 196805, 19: 393409, 20: 786621,
}


def zeros(k: int) -> list[int]:
    return [0] * layout(k)["buckets"]


@pytest.mark.parametrize("k", ALL_K)
def test_layout_matches_measured_state_sizes(k):
    assert layout(k)["state_bytes"] == MEASURED_STATE_BYTES[k]


@pytest.mark.parametrize("k", ALL_K)
def test_registers_pack_with_no_slack(k):
    """2^K x 6 bits lands on a byte boundary for every K >= 2; any padding would shift the
    histogram offset and corrupt every field after it."""
    L = layout(k)
    assert L["buckets"] * 6 % 8 == 0
    assert L["reg_bytes"] == L["buckets"] * 6 // 8


def test_zero_counter_widens_at_16():
    """2^16 zero registers do not fit in a UInt16, so the counter becomes a UInt32."""
    assert layout(15)["zeros_fmt"] == "<H"
    assert layout(16)["zeros_fmt"] == "<I"


@pytest.mark.parametrize("k", [MIN_K - 1, MAX_K + 1, 0, 21])
def test_layout_rejects_out_of_range_precision(k):
    with pytest.raises(ValueError, match="precision"):
        layout(k)


@pytest.mark.parametrize("k", SOME_K)
def test_empty_state(k):
    state = encode_state(zeros(k), k)
    hist, zero_count = decode_trailer(state, k)
    assert state[0] == LARGE, "always LARGE: SMALL and MEDIUM hold exact hashes"
    assert len(state) == MEASURED_STATE_BYTES[k]
    assert hist[0] == zero_count == 1 << k
    assert sum(hist[1:]) == 0


@pytest.mark.parametrize("k", SOME_K)
def test_roundtrip_random_registers(k):
    rng = random.Random(k)
    max_rank = layout(k)["max_rank"]
    regs = [rng.randint(0, max_rank) for _ in range(1 << k)]
    assert decode_registers(encode_state(regs, k), k) == regs


@pytest.mark.parametrize("k", [12, 16])
def test_isolated_write_does_not_bleed_into_neighbours(k):
    """6-bit fields straddle byte boundaries in a 4-register cycle; the high byte must be
    OR-ed in without clobbering the register that follows."""
    max_rank = layout(k)["max_rank"]
    for index in list(range(16)) + [(1 << k) - 2, (1 << k) - 1]:
        regs = zeros(k)
        regs[index] = max_rank
        decoded = decode_registers(encode_state(regs, k), k)
        assert decoded[index] == max_rank
        assert sum(decoded) == max_rank, f"register {index} disturbed a neighbour"


@pytest.mark.parametrize("k", SOME_K)
def test_histogram_matches_registers(k):
    rng = random.Random(99 + k)
    L = layout(k)
    regs = [rng.randint(0, L["max_rank"]) for _ in range(L["buckets"])]
    hist, zero_count = decode_trailer(encode_state(regs, k), k)
    expected = [0] * L["hist_slots"]
    for v in regs:
        expected[v] += 1
    assert hist == expected
    assert zero_count == hist[0]


def test_histogram_is_derived_not_trusted():
    with pytest.raises(TypeError):
        encode_state(zeros(12), 12, [0] * layout(12)["hist_slots"])


@pytest.mark.parametrize("k", [12, 14, 20])
def test_ranks_above_the_ceiling_are_clamped(k):
    """DataSketches coupon values reach 63; uniqCombined64's ceiling is 65-K."""
    max_rank = layout(k)["max_rank"]
    regs = zeros(k)
    regs[0] = 63
    state = encode_state(regs, k)
    hist, _ = decode_trailer(state, k)
    assert decode_registers(state, k)[0] == max_rank
    assert hist[max_rank] == 1
    assert sum(hist) == 1 << k


def test_encode_rejects_wrong_register_count():
    for bad in ([], [0] * 4095, [0] * 4097, [0] * (1 << 14)):
        with pytest.raises(ValueError, match="registers"):
            encode_state(bad, 12)


def test_encode_rejects_negative_register():
    regs = zeros(12)
    regs[7] = -1
    with pytest.raises(ValueError):
        encode_state(regs, 12)


def test_decode_rejects_small_medium_and_mis_sized_states():
    with pytest.raises(ValueError, match="LARGE"):
        decode_registers(b"\x01\x02" + struct.pack("<QQ", 111, 222), 12)
    with pytest.raises(ValueError, match="LARGE"):
        decode_registers(b"\x02" + bytes(64), 12)
    with pytest.raises(ValueError):
        decode_registers(b"", 12)
    with pytest.raises(ValueError, match="bytes"):      # a K=12 state read as K=14
        decode_registers(encode_state(zeros(12), 12), 14)


def test_trailer_offsets_are_where_the_format_says():
    """Pin absolute offsets: a shifted field would still round-trip through this codec while
    being unreadable by ClickHouse."""
    regs = zeros(14)
    regs[0] = 3
    state = encode_state(regs, 14)
    # type byte + 16384 x 6 bits = 12289; then 52 = 65-14+1 histogram slots
    hist = struct.unpack_from("<52I", state, 12289)
    assert hist[0] == (1 << 14) - 1 and hist[3] == 1
    assert struct.unpack_from("<H", state, 12289 + 52 * 4)[0] == (1 << 14) - 1


# ------------------------------------------------------------------ the hash model
def test_int_hash64_maps_zero_to_zero():
    """The bare MurmurHash3 finalizer fixes 0 -- which is why bucket 0 of a native state
    over numbers(n) always holds the maximum rank."""
    assert int_hash64(0) == 0


def test_int_hash64_salted_matches_sql_inthash64():
    """Value of SQL `SELECT intHash64(0)` on 26.10.1.288. SQL salts its input, so this pins
    both the port and the salt, and that the two functions really do differ."""
    assert int_hash64(0 ^ INTHASH64_SQL_SALT) == 4761183170873013810


def test_native_registers_cap_a_zero_tail():
    k = 12
    regs = native_registers([0, 5 | (1 << k), 7 | (0b1000 << k)], k)
    assert regs[0] == layout(k)["max_rank"]
    assert regs[5] == 1, "tail ...1 -> zero trailing zeros -> rank 1"
    assert regs[7] == 4, "tail ...1000 -> three trailing zeros -> rank 4"


# ----------------------------------------------------- DataSketches -> uniqCombined64
def hll(items, lg_k):
    s = hll_sketch(lg_k, tgt_hll_type.HLL_8)
    for i in items:
        s.update(i)
    return s


@pytest.mark.parametrize("lg_k,expected", [(12, 12), (14, 14), (20, 20), (21, 20)])
def test_target_k_keeps_source_precision_where_allowed(lg_k, expected):
    assert target_k(lg_k) == expected


@pytest.mark.parametrize("lg_k", [4, 10, 11])
def test_target_k_rejects_precision_below_12(lg_k):
    with pytest.raises(ValueError, match="minimum"):
        target_k(lg_k)


def test_registers_are_not_folded_when_k_matches():
    """The point of this target: an lgK=14 sketch keeps all 16384 registers."""
    sk = hll((f"v{i}" for i in range(50000)), 14)
    regs = datasketches_registers(sk.serialize_compact(), 14)
    assert len(regs) == 1 << 14
    assert sum(1 for r in regs if r) > 1 << 13, "a dense lgK=14 sketch fills most registers"


def test_cannot_unfold_a_smaller_source():
    sk = hll(range(1000), 12)
    with pytest.raises(ValueError, match="unfolded"):
        datasketches_registers(sk.serialize_compact(), 14)


def test_folding_a_larger_source_is_a_max_over_shared_low_bits():
    """lgK 14 -> 12 via hll_union equals the manual fold, as for uniqhll12."""
    sk = hll(range(30000), 14)
    full = datasketches_registers(sk.serialize_compact(), 14)
    manual = [max(full[i::4096]) for i in range(4096)]
    assert datasketches_registers(sk.serialize_compact(), 12) == manual


@pytest.mark.parametrize("tgt", [tgt_hll_type.HLL_4, tgt_hll_type.HLL_6, tgt_hll_type.HLL_8])
def test_every_source_type_gives_the_same_registers(tgt):
    """Druid columns come as HLL_4 or HLL_8; the codec must not care."""
    ref = hll(range(20000), 14)
    sk = hll_sketch(14, tgt)
    for i in range(20000):
        sk.update(i)
    assert datasketches_registers(sk.serialize_compact(), 14) == \
        datasketches_registers(ref.serialize_compact(), 14)


def test_sparse_and_dense_sources_agree():
    """Rollup rows are mostly LIST/SET; registers of a union must equal the elementwise max
    of a sparse-derived and a dense-derived array."""
    sparse, dense = hll(range(100), 14), hll(range(1000, 200000), 14)
    u = hll_union(14)
    u.update(sparse)
    u.update(dense)
    rs = datasketches_registers(sparse.serialize_compact(), 14)
    rd = datasketches_registers(dense.serialize_compact(), 14)
    ru = datasketches_registers(u.get_result(tgt_hll_type.HLL_8).serialize_compact(), 14)
    assert [max(a, b) for a, b in zip(rs, rd)] == ru


def test_state_from_tiny_sketch_is_large_form():
    """Even a 3-value sketch must become a LARGE state -- the exact-hash tiers are unwritable."""
    state = state_from_datasketches(hll(["a", "b", "c"], 14).serialize_compact(), 14)
    assert state[0] == LARGE and len(state) == MEASURED_STATE_BYTES[14]
    assert sum(1 for r in decode_registers(state, 14) if r) == 3
