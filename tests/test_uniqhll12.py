"""The reverse-engineered ClickHouse uniqHLL12 codec.

This is the most fragile code in the repo: an undocumented, version-specific binary format.
These tests pin its invariants without needing a server. The complementary check -- that the
codec reproduces states ClickHouse itself produced, byte for byte -- lives in
test_integration.py, because only a live server can supply those.

A wrong histogram does not raise: ClickHouse rebuilds its denominator from it and silently
returns a wrong estimate. So the histogram gets more attention here than its size suggests.
"""

from __future__ import annotations

import random
import struct

import pytest

from uniqhll12 import (
    BUCKETS, HIST_SLOTS, MAX_RANK, REG_BYTES, STATE_BYTES, clamp_rank, decode_histogram,
    decode_registers, encode_state,
)


def zeros() -> list[int]:
    return [0] * BUCKETS


def test_layout_constants_are_self_consistent():
    assert BUCKETS * 5 // 8 == REG_BYTES == 2560
    assert HIST_SLOTS == MAX_RANK + 1 == 22
    assert STATE_BYTES == 1 + REG_BYTES + HIST_SLOTS * 4 + 2 == 2651


def test_registers_pack_with_no_slack():
    """4096 x 5 bits lands exactly on the byte boundary; any padding would shift the
    histogram offset and corrupt every field after it."""
    assert BUCKETS * 5 % 8 == 0


def test_encode_state_shape():
    state = encode_state(zeros())
    assert len(state) == STATE_BYTES
    assert state[0] == 1, "is_large flag must be set; the sparse form is unwritable"


def test_empty_state_histogram():
    hist, zero_count = decode_histogram(encode_state(zeros()))
    assert hist[0] == BUCKETS
    assert sum(hist[1:]) == 0
    assert zero_count == BUCKETS


def test_roundtrip_random_registers():
    rng = random.Random(1234)
    regs = [rng.randint(0, MAX_RANK) for _ in range(BUCKETS)]
    assert decode_registers(encode_state(regs)) == regs


def test_roundtrip_every_rank_at_least_once():
    """Cover all 22 representable values, including the boundaries."""
    regs = [i % HIST_SLOTS for i in range(BUCKETS)]
    assert decode_registers(encode_state(regs)) == regs


@pytest.mark.parametrize("index", [0, 1, 4094, BUCKETS - 1])
def test_single_register_at_edges(index):
    """Registers straddle byte boundaries at 5 bits, so first and last are the risky ones."""
    regs = zeros()
    regs[index] = MAX_RANK
    decoded = decode_registers(encode_state(regs))
    assert decoded[index] == MAX_RANK
    assert sum(decoded) == MAX_RANK, "no neighbouring register may be disturbed"


def test_isolated_write_does_not_bleed_into_neighbours():
    """A 5-bit field spanning two bytes must OR into the high byte without clobbering the
    register that follows."""
    for index in range(0, 64):        # covers every bit-offset phase (5 and 8 cycle at 40)
        regs = zeros()
        regs[index] = MAX_RANK
        decoded = decode_registers(encode_state(regs))
        assert decoded[index] == MAX_RANK
        assert all(v == 0 for i, v in enumerate(decoded) if i != index)


def test_histogram_matches_registers():
    rng = random.Random(99)
    regs = [rng.randint(0, MAX_RANK) for _ in range(BUCKETS)]
    hist, zero_count = decode_histogram(encode_state(regs))

    expected = [0] * HIST_SLOTS
    for v in regs:
        expected[v] += 1
    assert hist == expected
    assert sum(hist) == BUCKETS
    assert zero_count == hist[0], "the trailing UInt16 duplicates histogram[0]"


def test_histogram_is_derived_not_trusted():
    """encode_state takes only registers -- there is no way to pass an inconsistent
    histogram in. Guard the signature so a future refactor cannot reintroduce one."""
    with pytest.raises(TypeError):
        encode_state(zeros(), [0] * HIST_SLOTS)


def test_ranks_above_max_are_clamped():
    """DataSketches ranks come off a 64-bit tail and can exceed ClickHouse's ceiling of 21.
    Writing one unclamped would overflow the 22-slot histogram."""
    assert clamp_rank(MAX_RANK) == MAX_RANK
    assert clamp_rank(MAX_RANK + 1) == MAX_RANK
    assert clamp_rank(63) == MAX_RANK          # largest a 6-bit coupon value can hold

    regs = zeros()
    regs[0] = 40
    state = encode_state(regs)
    hist, _ = decode_histogram(state)
    assert decode_registers(state)[0] == MAX_RANK
    assert hist[MAX_RANK] == 1
    assert sum(hist) == BUCKETS, "an unclamped value would land outside the histogram"


def test_all_registers_at_max():
    regs = [MAX_RANK] * BUCKETS
    state = encode_state(regs)
    hist, zero_count = decode_histogram(state)
    assert decode_registers(state) == regs
    assert hist[MAX_RANK] == BUCKETS
    assert zero_count == 0


def test_encode_rejects_wrong_register_count():
    for bad in ([], [0] * (BUCKETS - 1), [0] * (BUCKETS + 1)):
        with pytest.raises(ValueError, match="registers"):
            encode_state(bad)


def test_encode_rejects_negative_register():
    regs = zeros()
    regs[7] = -1
    with pytest.raises(ValueError):
        encode_state(regs)


def test_decode_rejects_sparse_and_empty_states():
    """The small-set form has is_large=0 and holds raw ClickHouse hashes, not registers."""
    sparse = b"\x00\x02" + struct.pack("<QQ", 111, 222)
    with pytest.raises(ValueError, match="dense"):
        decode_registers(sparse)
    with pytest.raises(ValueError):
        decode_registers(b"")


def test_histogram_offsets_are_where_the_format_says():
    """Pin the absolute offsets, not just the round-trip: a shifted field would still
    round-trip through this codec while being unreadable by ClickHouse."""
    regs = zeros()
    regs[0] = 3
    state = encode_state(regs)

    hist_at = struct.unpack_from(f"<{HIST_SLOTS}I", state, 1 + REG_BYTES)
    zeros_at = struct.unpack_from("<H", state, 1 + REG_BYTES + HIST_SLOTS * 4)[0]
    assert hist_at[0] == BUCKETS - 1
    assert hist_at[3] == 1
    assert zeros_at == BUCKETS - 1
    assert 1 + REG_BYTES == 2561 and 1 + REG_BYTES + HIST_SLOTS * 4 == 2649
