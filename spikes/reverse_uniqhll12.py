#!/usr/bin/env python
"""Spike: re-derive and re-validate ClickHouse's uniqHLL12 state format.

uniqhll12.py encodes an undocumented, version-specific binary format. This script is the
evidence behind it -- run it after any ClickHouse upgrade before trusting
migrate_hll_transplant.py.

How the layout was found, and what each step below proves:

  1. Size probing separates the sparse and dense forms (small set up to 16 values, then a
     fixed 2651 bytes regardless of cardinality).
  2. A one-element diff (n=17 vs n=18) shows exactly four byte positions changing: one in
     the register region and three in a trailer -- which is what identified the trailer as
     bookkeeping rather than register data.
  3. Interpreting the trailer as 22 UInt32 counters makes them sum to exactly 4096 (the
     bucket count) at every cardinality, and its first entry always equals the trailing
     UInt16. That pins it down as a register-value histogram plus a zero count.
  4. Recomputing that histogram from 5-bit-unpacked registers reproduces ClickHouse's
     bytes, confirming the register width and bit order.
  5. Byte-exact round-trip of states ClickHouse produced itself -- the strongest check.
  6. The DataSketches side: registers of a union equal the elementwise max of a
     sparse-derived and a dense-derived register array, validating both extraction paths.
  7. End-to-end oracle: transplanted states, read back by ClickHouse, track the source
     estimates.

Run:  .venv/bin/python spikes/reverse_uniqhll12.py
"""

from __future__ import annotations

import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datasketches import hll_sketch, hll_union, tgt_hll_type  # noqa: E402

from sketch_io import ch, enc_string  # noqa: E402
from uniqhll12 import (  # noqa: E402
    BUCKETS, HIST_SLOTS, MAX_RANK, REG_BYTES, STATE_BYTES, datasketches_registers,
    decode_histogram, decode_registers, encode_state, state_from_datasketches,
)


def state(n: int) -> bytes:
    return bytes.fromhex(
        ch(f"SELECT hex(toString(uniqHLL12State(number))) FROM numbers({n})"))


def main() -> int:
    ok = True

    print("=== 1. sparse vs dense, by size ===")
    for n in (1, 5, 16, 17, 100, 100000):
        s = state(n)
        form = "dense" if s[0] == 1 else f"sparse(count={s[1]})"
        print(f"  n={n:>7} bytes={len(s):>5} flag={s[0]} {form}")
    print(f"  -> dense is a fixed {STATE_BYTES} bytes; sparse is 2 + 8*count")

    print("\n=== 2. one-element diff (n=17 -> n=18) ===")
    a, b = state(17), state(18)
    diff = [i for i in range(len(a)) if a[i] != b[i]]
    for i in diff:
        region = "registers" if 1 <= i < 1 + REG_BYTES else "trailer"
        print(f"  offset {i:<5} {a[i]:#04x} -> {b[i]:#04x}   ({region})")

    print("\n=== 3. trailer as 22 UInt32 counters + UInt16 ===")
    for n in (17, 100, 1000, 100000):
        hist, zeros = decode_histogram(state(n))
        good = sum(hist) == BUCKETS and zeros == hist[0]
        ok &= good
        print(f"  n={n:>7} sum(hist)={sum(hist):<6} zeros={zeros:<6} "
              f"hist[0]={hist[0]:<6} consistent={good}")

    print("\n=== 4+5. registers unpack correctly, and states round-trip byte-exactly ===")
    for n in (17, 18, 50, 100, 1000, 10000, 100000, 1000000):
        s = state(n)
        regs = decode_registers(s)
        hist_ch, _ = decode_histogram(s)
        hist_mine = [0] * HIST_SLOTS
        for v in regs:
            hist_mine[v] += 1
        exact = encode_state(regs) == s
        ok &= exact and hist_mine == hist_ch
        print(f"  n={n:>8} max_rank={max(regs):<3} histogram_match={hist_mine == hist_ch} "
              f"round_trip_exact={exact}")
    print(f"  -> {BUCKETS} registers x 5 bits = {REG_BYTES} bytes; ranks cap at {MAX_RANK}")

    print("\n=== 6. DataSketches register extraction (sparse and dense paths agree) ===")
    S, D = hll_sketch(12, tgt_hll_type.HLL_8), hll_sketch(12, tgt_hll_type.HLL_8)
    for i in range(100):
        S.update(i)
    for i in range(1000, 20000):
        D.update(i)
    u = hll_union(12)
    u.update(S)
    u.update(D)
    rS = datasketches_registers(S.serialize_compact())
    rD = datasketches_registers(D.serialize_compact())
    rU = datasketches_registers(u.get_result(tgt_hll_type.HLL_8).serialize_compact())
    agree = [max(x, y) for x, y in zip(rS, rD)] == rU
    ok &= agree
    print(f"  max(registers(sparse), registers(dense)) == registers(union): {agree}")

    print("\n=== 7. oracle: ClickHouse reads transplanted states ===")
    ch("DROP TABLE IF EXISTS _spike_tp")
    ch("CREATE TABLE _spike_tp (label String, s AggregateFunction(uniqHLL12, String)) "
       "ENGINE = MergeTree ORDER BY label")
    print(f"  {'n':>8} {'datasketches':>14} {'clickhouse':>12} {'delta':>8}")
    for n in (20, 100, 1000, 10000, 100000):
        sk = hll_sketch(12, tgt_hll_type.HLL_8)
        for i in range(n):
            sk.update(f"v{i}")
        label = f"n{n}"
        ch("INSERT INTO _spike_tp FORMAT RowBinary",
           data=enc_string(label) + state_from_datasketches(sk.serialize_compact()))
        got = float(ch(f"SELECT uniqHLL12Merge(s) FROM _spike_tp WHERE label = '{label}'"))
        ds = sk.get_estimate()
        delta = abs(got - ds) / ds * 100
        ok &= delta < 5.0
        print(f"  {n:>8} {ds:>14.1f} {got:>12.0f} {delta:>7.2f}%")
    ch("DROP TABLE _spike_tp")

    print(f"\n{'FORMAT CONFIRMED' if ok else 'FORMAT MISMATCH -- do not trust uniqhll12.py'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
