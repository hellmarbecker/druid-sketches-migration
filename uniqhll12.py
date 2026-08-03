"""Codec for ClickHouse's `uniqHLL12` aggregate-function state.

Reverse-engineered against ClickHouse 26.8.1.120 by differential analysis of
`hex(toString(uniqHLL12State(...)))`; `spikes/reverse_uniqhll12.py` re-derives and
re-validates every claim below, including a byte-exact round-trip of states ClickHouse
produced itself. This is NOT a documented format -- treat it as version-specific and
re-run that spike after any ClickHouse upgrade.

Layout, dense ("large") form, 2651 bytes total:

    offset  size  meaning
    0       1     is_large flag, = 1
    1       2560  4096 registers, 5 bits each, LSB-first, register k at bits [5k, 5k+5)
    2561    88    22 x UInt32: histogram of register values, index = rank 0..21
    2649    2     UInt16: number of zero registers (duplicates histogram[0])

Sparse ("small") form, used up to 16 distinct values:

    offset  size  meaning
    0       1     is_large flag, = 0
    1       1     count of stored hashes, <= 16
    2       8*n   raw UInt64 hash values

The register width is 5 bits and ranks top out at 21 because ClickHouse hashes to 32 bits
and spends 12 of them on the bucket index, leaving a 20-bit tail (20 leading zeros + 1).
DataSketches ranks come from a 64-bit tail and can exceed that, so transplanted values
must be clamped -- see clamp_rank(). The probability of a rank above 21 is ~2^-21 per
item, so the clamp is statistically irrelevant but must not be omitted: a value above 21
would overflow the histogram and corrupt the state.

Writing the sparse form is not possible from a DataSketches sketch: it stores raw
ClickHouse hash values, which cannot be recovered from an HLL sketch (or from
DataSketches coupons). Always emit the dense form.
"""

from __future__ import annotations

import struct

from datasketches import hll_sketch, hll_union, tgt_hll_type

BUCKETS = 4096          # 2^12, ClickHouse precision 12
REG_WIDTH = 5           # bits per register
MAX_RANK = 21           # 32-bit hash - 12 index bits => 20-bit tail => rank <= 21
REG_BYTES = BUCKETS * REG_WIDTH // 8            # 2560
HIST_SLOTS = MAX_RANK + 1                       # 22
STATE_BYTES = 1 + REG_BYTES + HIST_SLOTS * 4 + 2  # 2651


def clamp_rank(v: int) -> int:
    return v if v <= MAX_RANK else MAX_RANK


def decode_registers(state: bytes) -> list[int]:
    """Unpack the 4096 5-bit registers from a dense uniqHLL12 state."""
    if not state or state[0] != 1:
        raise ValueError("not a dense uniqHLL12 state (is_large flag != 1)")
    regs = []
    for k in range(BUCKETS):
        bit = k * REG_WIDTH
        byte, off = (bit >> 3) + 1, bit & 7   # +1 to skip the flag byte
        chunk = state[byte] | (state[byte + 1] << 8 if byte + 1 < len(state) else 0)
        regs.append((chunk >> off) & 0x1F)
    return regs


def encode_state(registers: list[int]) -> bytes:
    """Build a dense uniqHLL12 state from 4096 register values.

    The histogram is derived here rather than taken on trust: ClickHouse rebuilds its
    denominator from it, so an inconsistent histogram yields silently wrong estimates.
    """
    if len(registers) != BUCKETS:
        raise ValueError(f"expected {BUCKETS} registers, got {len(registers)}")

    packed = bytearray(REG_BYTES)
    hist = [0] * HIST_SLOTS
    for k, raw in enumerate(registers):
        v = clamp_rank(raw)
        if v < 0:
            raise ValueError(f"negative register value at {k}")
        hist[v] += 1
        bit = k * REG_WIDTH
        byte, off = bit >> 3, bit & 7
        chunk = v << off
        packed[byte] |= chunk & 0xFF
        if chunk >> 8:
            packed[byte + 1] |= (chunk >> 8) & 0xFF

    state = b"\x01" + bytes(packed) + struct.pack(f"<{HIST_SLOTS}I", *hist) \
        + struct.pack("<H", hist[0])
    assert len(state) == STATE_BYTES, len(state)
    return state


def decode_histogram(state: bytes) -> tuple[list[int], int]:
    hist = list(struct.unpack_from(f"<{HIST_SLOTS}I", state, 1 + REG_BYTES))
    zeros = struct.unpack_from("<H", state, 1 + REG_BYTES + HIST_SLOTS * 4)[0]
    return hist, zeros


# ------------------------------------------------- DataSketches -> ClickHouse transplant
LG_K = 12  # ClickHouse precision is fixed at 12, so sources must be folded to match


def datasketches_registers(sketch_bytes: bytes) -> list[int]:
    """Extract 4096 HLL registers from a Druid/DataSketches sketch, folded to lgK=12.

    Folding is delegated to hll_union rather than done by hand: dropping index bits
    turns them into rank bits, which is not a plain bucket-wise max.

    Handles both sketch representations, and `spikes/reverse_uniqhll12.py` proves the two
    agree (registers of a union == elementwise max of a sparse-derived and a dense-derived
    register array):
      - dense HLL: 40-byte preamble then one byte per register
      - LIST/SET:  a UInt32 coupon array, slot in the low 26 bits, value in the top 6
    """
    u = hll_union(LG_K)
    u.update(hll_sketch.deserialize(sketch_bytes))
    b = u.get_result(tgt_hll_type.HLL_8).serialize_updatable()

    if b[7] & 0x03 == 2:                        # dense
        regs = list(b[40:40 + BUCKETS])
        if len(regs) != BUCKETS:
            raise ValueError(f"short dense register array: {len(regs)}")
        return regs

    pre = (b[0] & 0x3F) * 4                     # sparse coupon array
    regs = [0] * BUCKETS
    for i in range((len(b) - pre) // 4):
        coupon = struct.unpack_from("<I", b, pre + 4 * i)[0]
        if not coupon:
            continue
        idx = (coupon & 0x3FFFFFF) & (BUCKETS - 1)
        regs[idx] = max(regs[idx], coupon >> 26)
    return regs


def state_from_datasketches(sketch_bytes: bytes) -> bytes:
    """Transplant a DataSketches HLL sketch into a ClickHouse uniqHLL12 state.

    Registers carry over as-is. The estimate survives because HLL's estimator is
    symmetric in its buckets, but the bucket->key correspondence does NOT survive: the
    two systems hash differently, so a transplanted state must never be merged with a
    natively-built ClickHouse one. Doing so double-counts silently (measured: 2022 for
    two sketches over the same 1000 values). See migrate_hll_transplant.py.
    """
    return encode_state(datasketches_registers(sketch_bytes))
