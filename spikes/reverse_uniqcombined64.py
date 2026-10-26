#!/usr/bin/env python
"""Spike: derive and validate ClickHouse's uniqCombined64(K) state format.

uniqcombined64.py encodes an internal ClickHouse format; this script is the evidence behind
it. Run it after any ClickHouse upgrade before trusting
`migrate_hll_transplant.py --target uniqcombined64`.

Motivation: uniqHLL12 is fixed at 4096 buckets, so the uniqHLL12 transplant folds the lgK=14
`pages` column down to 12 and halves its precision. uniqCombined64 takes K in [12, 20].
The layout was read from ClickHouse source rather than reverse-engineered; the codec's
docstring has it field by field. This script confirms it against the running build, and
measures whether keeping lgK actually pays.

What each step below proves:

  1. The state size matches the layout formula at every K in [12, 20].
  2. The tier boundaries: SMALL to 16, MEDIUM to 2^(K-5), then LARGE.
  3. The trailer is a register histogram plus a zero count.
  4. Byte-exact round-trip of states ClickHouse produced itself.
  5. The hash model: registers recomputed in Python from ClickHouse's own hash values
     match the decoded registers exactly, for UInt64 and String arguments.
  6. DataSketches register extraction at lgK=14 is consistent across sparse and dense.
  7. Oracle: ClickHouse reads transplanted states and tracks the source estimates.
  8. Merge semantics: transplanted states union correctly with each other, and double-count
     against native states, as with the uniqHLL12 transplant.
  9. Payoff on the Druid fixture (skipped if Druid is down): the lgK=14 `pages` column
     without the fold, against the uniqHLL12 transplant and the exact count.
 10. The same comparison over repeated trials, since one dataset is one draw.

Run:  .venv/bin/python spikes/reverse_uniqcombined64.py
"""

from __future__ import annotations

import gzip
import json
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datasketches import hll_sketch, hll_union, tgt_hll_type  # noqa: E402

from sketch_io import ch, decode_druid_sketch, druid_one, druid_rows, enc_string  # noqa: E402
from uniqcombined64 import (  # noqa: E402
    INTHASH64_SQL_SALT, LARGE, datasketches_registers, decode_registers, decode_trailer,
    encode_state, int_hash64, layout, native_registers, state_from_datasketches,
)
from uniqhll12 import state_from_datasketches as uniqhll12_state  # noqa: E402


# ------------------------------------------------------------------------- helpers
def state(k: int, n: int, arg: str = "number") -> bytes:
    return bytes.fromhex(
        ch(f"SELECT hex(toString(uniqCombined64State({k})({arg}))) FROM numbers({n})"))


def ds_sketch(values, lg_k: int) -> hll_sketch:
    sk = hll_sketch(lg_k, tgt_hll_type.HLL_8)
    for v in values:
        sk.update(v)
    return sk


TRIALS = 40


def raw_truth() -> dict | None:
    """Exact distinct users/pages from the raw wikiticker file the fixture was built from."""
    path = os.path.join(os.environ.get("DRUID_HOME", ""),
                        "quickstart/tutorial/wikiticker-2015-09-12-sampled.json.gz")
    if not os.path.exists(path):
        return None
    users, pages = set(), set()
    with gzip.open(path, "rt") as f:
        for line in f:
            row = json.loads(line)
            users.add(row["user"])
            pages.add(row["page"])
    return {"users": len(users), "pages": len(pages)}


def druid_up() -> bool:
    try:
        druid_one("SELECT COUNT(1) FROM wikipedia_rollup_sketches")
        return True
    except Exception:
        return False


def main() -> int:
    ok = True

    print("=== 1. state size matches the layout formula, K = 12..20 ===")
    for k in range(12, 21):
        s = state(k, (1 << (k - 5)) + 1000)
        good = s[0] == LARGE and len(s) == layout(k)["state_bytes"]
        ok &= good
        print(f"  K={k:<2} bytes={len(s):>7} expected={layout(k)['state_bytes']:>7} "
              f"type={s[0]} ok={good}")

    print("\n=== 2. tier boundaries (SMALL <= 16, MEDIUM <= 2^(K-5), then LARGE) ===")
    for k in (12, 14):
        med = 1 << (k - 5)
        for n, want in ((16, 1), (17, 2), (med, 2), (med + 1, 3)):
            got_type = state(k, n)[0]
            est = int(ch(f"SELECT uniqCombined64({k})(number) FROM numbers({n})"))
            good = got_type == want and (want == 3 or est == n)
            ok &= good
            print(f"  K={k} n={n:<5} type={got_type} (want {want}) estimate={est:<5} ok={good}")
    print("  -> SMALL and MEDIUM count exactly (up to hash collisions)")

    print("\n=== 3. trailer is a register histogram + zero count ===")
    for k, n in ((12, 10000), (14, 100000), (17, 1000000), (20, 100000)):
        s = state(k, n)
        hist, zeros = decode_trailer(s, k)
        mine = [0] * layout(k)["hist_slots"]
        for v in decode_registers(s, k):
            mine[v] += 1
        good = sum(hist) == 1 << k and zeros == hist[0] and hist == mine
        ok &= good
        print(f"  K={k} n={n:<7} sum(hist)={sum(hist):<8} zeros={zeros:<7} "
              f"matches_registers={hist == mine}")

    print("\n=== 4. byte-exact round-trip of ClickHouse's own states ===")
    for k, n, arg in ((12, 5000, "number"), (12, 1000000, "number"),
                      (14, 100000, "number"), (14, 100000, "toString(number)"),
                      (17, 1000000, "number"), (20, 2000000, "number")):
        s = state(k, n, arg)
        regs = decode_registers(s, k)
        exact = encode_state(regs, k) == s
        ok &= exact
        print(f"  K={k:<2} n={n:<8} arg={arg:<17} max_rank={max(regs):<3} "
              f"round_trip_exact={exact}")

    print("\n=== 5. hash model: registers recomputed from ClickHouse's hash values ===")
    n = 200000
    sql_hashes = [int(x) for x in ch(f"SELECT intHash64(number) FROM numbers({n})").split()]
    port_ok = sql_hashes == [int_hash64(i ^ INTHASH64_SQL_SALT) for i in range(n)]
    ok &= port_ok
    print(f"  Python int_hash64 port, salted, == SQL intHash64: {port_ok}")
    int_hashes = [int_hash64(i) for i in range(n)]
    str_hashes = [int(x) for x in ch(f"SELECT cityHash64(toString(number)) FROM numbers({n})").split()]
    for k, arg, hashes, desc in ((12, "number", int_hashes, "unsalted intHash64(number)"),
                                 (17, "number", int_hashes, "unsalted intHash64(number)"),
                                 (14, "toString(number)", str_hashes, "cityHash64(toString(number))")):
        match = native_registers(hashes, k) == decode_registers(state(k, n, arg), k)
        ok &= match
        print(f"  K={k:<2} {desc:<29} registers_match={match}")
    print("  -> bucket = low K bits, rank = ctz(tail) + 1, a zero tail scores the cap;")
    print("     integers via the unsalted intHash64, strings via cityHash64")

    print("\n=== 6. DataSketches extraction at lgK=14 (sparse and dense agree) ===")
    S, D = ds_sketch(range(100), 14), ds_sketch(range(1000, 200000), 14)
    u = hll_union(14)
    u.update(S)
    u.update(D)
    rS = datasketches_registers(S.serialize_compact(), 14)
    rD = datasketches_registers(D.serialize_compact(), 14)
    rU = datasketches_registers(u.get_result(tgt_hll_type.HLL_8).serialize_compact(), 14)
    agree = [max(x, y) for x, y in zip(rS, rD)] == rU
    ok &= agree
    print(f"  max(registers(sparse), registers(dense)) == registers(union): {agree}")

    print("\n=== 7. oracle: ClickHouse reads transplanted states ===")
    print("  (n = 2.75 * 2^K sits where non-17 precisions carry ClickHouse's ~1.5% bias)")
    print(f"  {'K':>3} {'n':>8} {'datasketches':>14} {'clickhouse':>12} {'delta':>8}")
    for k in (12, 14, 17):
        ch("DROP TABLE IF EXISTS _spike_uc")
        ch(f"CREATE TABLE _spike_uc (label String, s AggregateFunction(uniqCombined64({k}), "
           "String)) ENGINE = MergeTree ORDER BY label")
        for n in (20, 1000, 100000, (11 << k) // 4, 1000000):
            sk = ds_sketch((f"v{i}" for i in range(n)), k)
            label = f"n{n}"
            ch("INSERT INTO _spike_uc FORMAT RowBinary",
               data=enc_string(label) + state_from_datasketches(sk.serialize_compact(), k))
            got = float(ch(f"SELECT uniqCombined64Merge({k})(s) FROM _spike_uc "
                           f"WHERE label = '{label}'"))
            ds = sk.get_estimate()
            delta = (got - ds) / ds * 100
            ok &= abs(delta) < 5.0
            print(f"  {k:>3} {n:>8} {ds:>14.1f} {got:>12.0f} {delta:>+7.2f}%")
    ch("DROP TABLE _spike_uc")

    print("\n=== 8. merge semantics, K=14 ===")
    k = 14
    ch("DROP TABLE IF EXISTS _spike_uc")
    ch(f"CREATE TABLE _spike_uc (label String, s AggregateFunction(uniqCombined64({k}), "
       "String)) ENGINE = MergeTree ORDER BY label")
    for label, lo, hi in (("A", 0, 1000), ("B", 500, 1500)):
        sk = ds_sketch((f"v{i}" for i in range(lo, hi)), k)
        ch("INSERT INTO _spike_uc FORMAT RowBinary",
           data=enc_string(label) + state_from_datasketches(sk.serialize_compact(), k))
    ch(f"INSERT INTO _spike_uc SELECT 'N', uniqCombined64State({k})(concat('v', toString(number))) "
       "FROM numbers(1000)")

    def merged(labels: str) -> int:
        return int(ch(f"SELECT uniqCombined64Merge({k})(s) FROM _spike_uc "
                      f"WHERE label IN ({labels})"))

    ab, aa, an = merged("'A','B'"), merged("'A'"), merged("'A','N'")
    good = abs(ab - 1500) / 1500 < 0.05 and abs(an - 2000) / 2000 < 0.05
    ok &= good
    print(f"  transplanted A ∪ B  (truth 1500): {ab}")
    print(f"  transplanted A      (truth 1000): {aa}")
    print(f"  transplanted A ∪ native, same 1000 values (truth 1000): {an}  <- double-count")
    print("  -> the uniqHLL12 rule carries over: never write these columns with "
          "uniqCombined64State()")
    ch("DROP TABLE _spike_uc")

    print("\n=== 9. payoff on the Druid fixture: no fold for lgK=14 ===")
    if not druid_up():
        print("  SKIPPED: Druid or wikipedia_rollup_sketches unavailable")
    else:
        cols = (("users", "users_hll_k12_hll4", 12), ("pages", "pages_hll_k14_hll8", 14))
        ch("DROP TABLE IF EXISTS _spike_uc_druid")
        ch("CREATE TABLE _spike_uc_druid ("
           "users_uc AggregateFunction(uniqCombined64(12), String), "
           "pages_uc AggregateFunction(uniqCombined64(14), String), "
           "users_h12 AggregateFunction(uniqHLL12, String), "
           "pages_h12 AggregateFunction(uniqHLL12, String)) ENGINE = MergeTree ORDER BY tuple()")
        buf = bytearray()
        rows = 0
        for users, pages in druid_rows(
                f"SELECT {cols[0][1]}, {cols[1][1]} FROM wikipedia_rollup_sketches"):
            u, p = decode_druid_sketch(users), decode_druid_sketch(pages)
            buf += state_from_datasketches(u, 12) + state_from_datasketches(p, 14) \
                + uniqhll12_state(u) + uniqhll12_state(p)
            rows += 1
        ch("INSERT INTO _spike_uc_druid FORMAT RowBinary", data=bytes(buf))
        uc = ch("SELECT uniqCombined64Merge(12)(users_uc), uniqCombined64Merge(14)(pages_uc), "
                "uniqHLL12Merge(users_h12), uniqHLL12Merge(pages_h12) FROM _spike_uc_druid").split()
        ch("DROP TABLE _spike_uc_druid")
        truth = raw_truth()
        print(f"  {rows} rows transplanted; error is against the exact count from the raw "
              "file" if truth else f"  {rows} rows transplanted; raw file not found, "
              "so error is against Druid")
        print(f"  {'column':<7} {'exact':>7} {'druid (own lgK)':>18} {'uniqCombined64':>17} "
              f"{'uniqHLL12':>15}")
        for (name, col, lg_k), uc_est, h12_est in zip(cols, uc[:2], uc[2:]):
            druid = float(druid_one(
                f"SELECT APPROX_COUNT_DISTINCT_DS_HLL({col}, {lg_k}) "
                "FROM wikipedia_rollup_sketches")[0])
            ref = truth[name] if truth else druid

            def err(x) -> str:
                return f"{(float(x) - ref) / ref * 100:>+6.2f}%"

            ok &= abs(float(uc_est) - druid) / druid < 0.05
            print(f"  {name:<7} {truth[name] if truth else '-':>7} {druid:>10.0f} {err(druid)} "
                  f"{uc_est:>9} {err(uc_est)} {h12_est:>7} {err(h12_est)}")
        print("  (one dataset is one draw -- see section 10 for the distribution)")

    print("\n=== 10. accuracy over repeated trials, at the fixture's cardinalities ===")
    print(f"  {TRIALS} independent sets each; error against the exact count")
    for name, lg_k, n in (("users-like", 12, 10531), ("pages-like", 14, 35143)):
        ch("DROP TABLE IF EXISTS _spike_uc_trials")
        ch(f"CREATE TABLE _spike_uc_trials (trial UInt32, "
           f"uc AggregateFunction(uniqCombined64({lg_k}), String), "
           "h12 AggregateFunction(uniqHLL12, String)) ENGINE = MergeTree ORDER BY trial")
        buf = bytearray()
        ds_est = []
        for t in range(TRIALS):
            b = ds_sketch((f"t{t}-{i}" for i in range(n)), lg_k).serialize_compact()
            ds_est.append(hll_sketch.deserialize(b).get_estimate())
            buf += struct.pack("<I", t) + state_from_datasketches(b, lg_k) + uniqhll12_state(b)
        ch("INSERT INTO _spike_uc_trials FORMAT RowBinary", data=bytes(buf))
        res = [line.split() for line in ch(
            "SELECT finalizeAggregation(uc), finalizeAggregation(h12) "
            "FROM _spike_uc_trials ORDER BY trial").splitlines()]
        ch("DROP TABLE _spike_uc_trials")
        print(f"  {name}: source lgK={lg_k}, n={n} (= {n / (1 << lg_k):.2f} x 2^{lg_k})")
        print(f"    {'method':<22} {'mean err':>9} {'rmse':>7}")
        for method, ests in (("DataSketches", ds_est),
                             (f"uniqCombined64({lg_k})", [float(r[0]) for r in res]),
                             ("uniqHLL12 (lgK 12)", [float(r[1]) for r in res])):
            errs = [(e - n) / n for e in ests]
            mean = sum(errs) / len(errs) * 100
            rmse = (sum(x * x for x in errs) / len(errs)) ** 0.5 * 100
            print(f"    {method:<22} {mean:>+8.2f}% {rmse:>6.2f}%")

    print(f"\n{'FORMAT CONFIRMED' if ok else 'FORMAT MISMATCH -- do not build on this layout'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
