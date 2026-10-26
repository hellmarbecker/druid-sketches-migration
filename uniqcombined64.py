"""Codec for ClickHouse's `uniqCombined64(K)` aggregate-function state.

Unlike uniqhll12.py, this format was read from ClickHouse source (CombinedCardinalityEstimator.h,
HyperLogLogCounter.h, CompactArray.h; unchanged between v26.8.1.2041-lts and v26.10.1.1-new)
and then confirmed against 26.10.1.288 by `spikes/reverse_uniqcombined64.py`, including a
byte-exact round-trip of states ClickHouse produced at every K. It is still an internal
aggregate-state layout with no stability contract -- re-run that spike after any upgrade.

Why this exists alongside uniqhll12.py: uniqHLL12 is fixed at 4096 buckets, so an lgK=14
Druid column has to be folded to 12, halving its precision. uniqCombined64 takes K in
[12, 20], so a transplant can keep the source's lgK.

Layout, LARGE (HyperLogLog) form, for K in [12, 20]:

    size              meaning
    1                 container type: 1 = SMALL, 2 = MEDIUM, 3 = LARGE
    2^K * 6 / 8       registers, 6 bits each, LSB-first, register k at bits [6k, 6k+6)
    4 * (66 - K)      UInt32 histogram of register values, index = rank 0..65-K
    2 or 4            zero-register count (duplicates histogram[0]): UInt16 below K=16,
                      UInt32 from K=16, since 2^16 zeros no longer fit in a UInt16

Registers are 6 bits because uniqCombined64 hashes to 64 bits; the rank is the trailing
zeros of the 64-K bits above the bucket index, plus one, capped at 65-K. DataSketches ranks
come from a separate 64-bit lane and can exceed that cap, so they are clamped, exactly as
uniqhll12.py clamps at 21. Above the cap only by a 2^-(64-K) chance, but a value past it would
land outside the histogram.

SMALL (up to 16 values) and MEDIUM (up to 2^(K-5) values) hold exact ClickHouse hashes, which
an HLL sketch has discarded, so a transplant always emits LARGE. A LARGE state holding only a
handful of nonzero registers is valid: ClickHouse reads it and applies linear counting.

Each K is a distinct aggregate type. A uniqCombined64(14) state cannot be merged with a
uniqCombined64(12) state, nor with uniqCombined (the 32-bit variant, which has 5-bit registers).

Estimator: ClickHouse applies its HLL++ bias table only at K=17. Every other K uses linear
counting up to 2.5 * 2^K and the raw HLL estimate above it, which overcounts by ~1.5-2.5% just
past that switch. That is the ClickHouse side, not the transplant -- see CLAUDE.md.
"""

from __future__ import annotations

import struct

from datasketches import hll_sketch, hll_union, tgt_hll_type

MIN_K, MAX_K = 12, 20       # the range uniqCombined64 accepts
REG_WIDTH = 6
SMALL, MEDIUM, LARGE = 1, 2, 3
M64 = (1 << 64) - 1
INTHASH64_SQL_SALT = 0x4CF2D2BAAE6DA887


def layout(k: int) -> dict:
    """Field sizes for precision k. Every offset in the state derives from these."""
    if not MIN_K <= k <= MAX_K:
        raise ValueError(f"uniqCombined64 precision must be in [{MIN_K}, {MAX_K}], got {k}")
    buckets = 1 << k
    max_rank = 65 - k
    reg_bytes = buckets * REG_WIDTH // 8
    hist_slots = max_rank + 1
    zeros_fmt = "<H" if k < 16 else "<I"
    return {
        "buckets": buckets, "max_rank": max_rank, "reg_bytes": reg_bytes,
        "hist_slots": hist_slots, "zeros_fmt": zeros_fmt,
        "state_bytes": 1 + reg_bytes + 4 * hist_slots + struct.calcsize(zeros_fmt),
    }


def decode_registers(state: bytes, k: int) -> list[int]:
    """Unpack the 2^k 6-bit registers from a LARGE uniqCombined64(k) state."""
    L = layout(k)
    if not state or state[0] != LARGE:
        raise ValueError("not a LARGE uniqCombined64 state (container type != 3)")
    if len(state) != L["state_bytes"]:
        raise ValueError(f"expected {L['state_bytes']} bytes for K={k}, got {len(state)}")
    regs = []
    for i in range(1, 1 + L["reg_bytes"], 3):   # 4 six-bit registers per 3 bytes
        w = state[i] | (state[i + 1] << 8) | (state[i + 2] << 16)
        regs += (w & 63, (w >> 6) & 63, (w >> 12) & 63, (w >> 18) & 63)
    return regs


def decode_trailer(state: bytes, k: int) -> tuple[list[int], int]:
    """The register histogram and the zero-register count."""
    L = layout(k)
    off = 1 + L["reg_bytes"]
    hist = list(struct.unpack_from(f"<{L['hist_slots']}I", state, off))
    zeros = struct.unpack_from(L["zeros_fmt"], state, off + 4 * L["hist_slots"])[0]
    return hist, zeros


def encode_state(registers: list[int], k: int) -> bytes:
    """Build a LARGE uniqCombined64(k) state from 2^k register values.

    Ranks above 65-k are clamped. The histogram is derived from the registers rather than
    accepted as an argument, for the same reason as uniqhll12.encode_state(): a byte-exact
    round-trip against ClickHouse's own states is the regression gate for the format.
    Only nonzero registers are visited, because Druid's stored rollup sketches are sparse.
    """
    L = layout(k)
    if len(registers) != L["buckets"]:
        raise ValueError(f"expected {L['buckets']} registers, got {len(registers)}")
    packed = bytearray(L["reg_bytes"] + 1)      # +1: room for the last two-byte write
    hist = [0] * L["hist_slots"]
    nonzero = 0
    for idx, raw in enumerate(registers):
        if raw == 0:
            continue
        if raw < 0:
            raise ValueError(f"negative register value at {idx}")
        v = min(raw, L["max_rank"])
        hist[v] += 1
        nonzero += 1
        bit = idx * REG_WIDTH
        chunk = v << (bit & 7)
        packed[bit >> 3] |= chunk & 0xFF
        packed[(bit >> 3) + 1] |= chunk >> 8
    hist[0] = L["buckets"] - nonzero
    state = bytes([LARGE]) + bytes(packed[:-1]) \
        + struct.pack(f"<{L['hist_slots']}I", *hist) + struct.pack(L["zeros_fmt"], hist[0])
    assert len(state) == L["state_bytes"], len(state)
    return state


# ------------------------------------------------------------- ClickHouse's hash model
def int_hash64(x: int) -> int:
    """ClickHouse's C++ intHash64 (the MurmurHash3 finalizer), which uniqCombined64 applies
    to integer arguments. NOT the SQL function of the same name: that one hashes
    x ^ INTHASH64_SQL_SALT, so it cannot stand in for the aggregate's hash. Strings are
    hashed with CityHash64, which the SQL cityHash64 does match."""
    x ^= x >> 33
    x = (x * 0xFF51AFD7ED558CCD) & M64
    x ^= x >> 33
    x = (x * 0xC4CEB9FE1A85EC53) & M64
    x ^= x >> 33
    return x


def native_registers(hashes, k: int) -> list[int]:
    """The registers ClickHouse's HyperLogLogCounter::insert builds from 64-bit hashes:
    bucket = low k bits, rank = trailing zeros of the rest + 1, a zero tail scores the cap.
    Used to prove the hash model, not by the migration."""
    L = layout(k)
    regs = [0] * L["buckets"]
    for h in hashes:
        tail = h >> k
        rank = L["max_rank"] if tail == 0 else min((tail & -tail).bit_length(), L["max_rank"])
        b = h & (L["buckets"] - 1)
        if rank > regs[b]:
            regs[b] = rank
    return regs


# ------------------------------------------------- DataSketches -> ClickHouse transplant
def target_k(lg_k: int) -> int:
    """The uniqCombined64 precision for a source sketch's lgK.

    Keep the source's lgK where uniqCombined64 allows it. DataSketches allows lgK up to 21,
    one past ClickHouse's ceiling, so 21 folds to 20. Below 12 there is no target: registers
    cannot be unfolded, so a column like that belongs on migrate_hll.py instead.
    """
    if lg_k < MIN_K:
        raise ValueError(f"source lgK={lg_k} is below uniqCombined64's minimum of {MIN_K}")
    return min(lg_k, MAX_K)


def datasketches_registers(sketch_bytes: bytes, k: int) -> list[int]:
    """2^k registers from a Druid/DataSketches HLL sketch, unclamped.

    hll_union normalises every representation (HLL_4/6/8, dense or LIST/SET) and folds a
    source with lgK > k. It cannot unfold one with lgK < k, so that case raises rather than
    returning a mis-sized array: a column's K is fixed by its type, and a row with a smaller
    lgK cannot be written into it.
    """
    u = hll_union(k)
    u.update(hll_sketch.deserialize(sketch_bytes))
    b = u.get_result(tgt_hll_type.HLL_8).serialize_updatable()
    if b[3] != k:
        raise ValueError(f"source lgK={b[3]} is below target K={k}; registers cannot be unfolded")
    buckets = 1 << k
    if b[7] & 0x03 == 2:                        # dense: 40-byte preamble, a byte per register
        regs = list(b[40:40 + buckets])
        if len(regs) != buckets:
            raise ValueError(f"short dense register array: {len(regs)}")
        return regs
    pre = (b[0] & 0x3F) * 4                     # LIST/SET: UInt32 coupons
    regs = [0] * buckets
    for i in range((len(b) - pre) // 4):
        coupon = struct.unpack_from("<I", b, pre + 4 * i)[0]
        if coupon:
            idx = (coupon & 0x3FFFFFF) & (buckets - 1)
            regs[idx] = max(regs[idx], coupon >> 26)
    return regs


def state_from_datasketches(sketch_bytes: bytes, k: int) -> bytes:
    """Transplant a DataSketches HLL sketch into a uniqCombined64(k) state.

    Same contract as uniqhll12.state_from_datasketches(): the estimate survives, the
    key->bucket map does not, so a transplanted state must never be merged with one built
    by uniqCombined64State(). Measured: 1990 for two states over the same 1000 values.
    """
    return encode_state(datasketches_registers(sketch_bytes, k), k)
