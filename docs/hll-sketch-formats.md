# HLL sketch formats: Apache DataSketches (Druid) vs ClickHouse `uniqHLL12`

Reference for the HLL side of the migration. Covers both binary formats, what differs, and
the exact reason a Druid sketch can be transplanted into a ClickHouse state and merged with
other transplanted sketches, but must never be merged with a natively-built ClickHouse one.

## Provenance of these facts

Measured against **Druid 37.0.0** (`datasketches-java` 4.2.0), the **`datasketches` 5.2.0**
Python binding, and **ClickHouse 26.8.1.120**. Reproducible with:

- `spikes/inspect_sketches.py` — Druid's real sketch bytes and preamble fields
- `spikes/reverse_uniqhll12.py` — the ClickHouse format, step by step, including a
  byte-exact round-trip of states ClickHouse produced itself

The DataSketches format is a documented, versioned, cross-language contract. **The ClickHouse
format is not** — it is an internal aggregate-state layout with no stability guarantee, decoded
here by differential analysis. Anything in the ClickHouse section may change on upgrade; re-run
`spikes/reverse_uniqhll12.py` before trusting it. Where a claim below is inferred rather than
directly observed, it says so.

---

## 1. Apache DataSketches HLL (what Druid stores)

### Preamble

The first 8 bytes are common to every representation:

| byte | meaning |
| --- | --- |
| 0 | `preInts` — preamble size in 4-byte words (2, 3, or 10) |
| 1 | `serVer` = 1 |
| 2 | `familyId` = 7 (HLL) |
| 3 | **`lgK`** — log2 of the bucket count |
| 4 | `lgArr` — log2 of the coupon array size (sparse modes) |
| 5 | flags |
| 6 | `listCount` (LIST mode) / `curMin` (HLL_4 dense) |
| 7 | low 2 bits = **current mode**; bits 2–3 = configured **target type** |

Byte 7 decodes as mode `0=LIST, 1=SET, 2=HLL` and target type
`(byte7 >> 2) & 3 → 0=HLL_4, 1=HLL_6, 2=HLL_8`.

**`lgK` is recoverable from the bytes**, which matters: the fixture's two columns are `lgK=12`
(users) and `lgK=14` (pages), and code must read byte 3 rather than assume Druid's default.

`lgK` accepts **4–21** (verified: 3 and 22 are rejected with `Invalid value of k`).

### Three representations

A sketch changes representation as it fills. All three can appear in one Druid column.

**LIST** (`preInts=2`, 8-byte preamble) and **SET** (`preInts=3`, 12-byte preamble) are *sparse*:
the payload is an array of 4-byte **coupons**, one per distinct value seen.

A coupon packs the bucket and the rank into one `UInt32`:

```
bits 0..25   slot   (register index = slot & (2^lgK - 1))
bits 26..31  value  (the rank)
```

The value field is 6 bits, so ranks up to 63 are representable — which requires a 64-bit hash
lane (see *Hashing* below).

Measured compact sizes at `lgK=12`: n=1 → 12 B, n=10 → 52 B, n=100 → 412 B. Druid's stored
per-row rollup sketches were **sparse** (248–632 B); only a merge across the whole datasource
went dense. Sparse is the common case in a rollup table, not an edge case.

**HLL** (`preInts=10`, 40-byte preamble) is *dense*: a full register array in one of three
widths, chosen by `tgtHllType`.

| target | register width | compact size at lgK=12 | layout |
| --- | --- | --- | --- |
| `HLL_8` | 8 bits | 4136 B | 40 + 4096 — one plain byte per register |
| `HLL_6` | 6 bits | 3113 B | 40 + 3072 — bit-packed |
| `HLL_4` | 4 bits | 2088–2092 B | 40 + 2048 + an exceptions table |

`HLL_4` cannot hold a rank in 4 bits, so it stores each register as an offset from `curMin`
(byte 6) and spills anything that does not fit into an auxiliary **exceptions table**. Verified
at `lgK=12`: n=5000 → `curMin=0`, no aux; n=50 000 → `curMin=1`, 4 B aux; n=500 000 →
`curMin=4`, 4 B aux. So **`HLL_8` is the only form whose payload is a flat register array** —
convert before reading registers.

Bytes 8–39 of the dense preamble hold estimator accounting (HIP accumulator, KxQ sums, and the
`numAtCurMin` / `auxCount` counters). Verified indirectly: inserting the same 50 000 items in
reverse order leaves bytes 0–7 and the whole register array **identical** but changes bytes 8–39.

### Hashing and rank derivation

MurmurHash3, 128-bit, with the DataSketches default seed (9001). One 64-bit lane supplies the
slot, the other supplies the rank as *leading zeros + 1*. The HLL preamble carries **no seed
hash** field (unlike Theta, where bytes 6–7 are a seed hash — the fact that made the Theta path
cross-compatible).

Because the rank comes off a 64-bit lane, ranks are not bounded by 21 the way ClickHouse's are.

### Estimator

DataSketches keeps two estimators, and the distinction has bitten this project:

- **HIP** (Historical Inverse Probability) — used by a sketch built through `update()`. It is
  accumulated incrementally and is therefore **order-dependent**. Measured: the same 50 000
  items inserted forwards vs backwards give `50026.8248` vs `50358.8835` from identical
  register arrays.
- **Composite / raw** — used once HIP is no longer valid, i.e. after a union. Order-independent:
  `union(a,b)` and `union(b,a)` both give `49795.7802`.

So two sketches over the same set can report different estimates while being *byte-identical in
their registers*. Compare registers, or compare post-union estimates — not update-sketch
estimates.

---

## 2. ClickHouse `uniqHLL12`

Only one HLL function exists in this build (`SELECT name FROM system.functions WHERE name ILIKE
'%hll%'` → `uniqHLL12`), and its precision is **fixed at 12** — 4096 buckets, not configurable.

### Sparse ("small set") form, up to 16 distinct values

| offset | size | meaning |
| --- | --- | --- |
| 0 | 1 | `is_large` = 0 |
| 1 | 1 | count of stored hashes (≤ 16) |
| 2 | 8 × count | raw `UInt64` hash values |

Measured totals: n=1 → 10 B, n=5 → 42 B, n=16 → 130 B. At the 17th distinct value it converts
to the dense form.

**This form is unwritable from a DataSketches sketch.** It stores ClickHouse's own hash values,
and an HLL sketch has discarded the hashes it saw — it keeps only a max rank per bucket.
Transplants must always emit the dense form.

### Dense ("large") form, a fixed 2651 bytes at any cardinality

| offset | size | meaning |
| --- | --- | --- |
| 0 | 1 | `is_large` = 1 |
| 1 | 2560 | 4096 registers, **5 bits each**, LSB-first, register *k* at bits `[5k, 5k+5)` |
| 2561 | 88 | 22 × `UInt32` — histogram of register values, index = rank 0..21 |
| 2649 | 2 | `UInt16` — count of zero registers (duplicates `histogram[0]`) |

4096 × 5 bits = 2560 bytes exactly, with no padding: the last register ends flush on the byte
boundary.

The histogram is **not decoration** — ClickHouse rebuilds its denominator from it, so a state
whose histogram disagrees with its registers yields silently wrong estimates. `encode_state()`
in `uniqhll12.py` therefore derives it from the registers written rather than accepting one.
The counters sum to exactly 4096 at every cardinality measured.

### Hashing and rank derivation

Ranks top out at **21** and the histogram has exactly **22** slots. Both are consistent with a
**32-bit** hash: 12 bits go to the bucket index, leaving a 20-bit tail, so rank = leading zeros
of that tail + 1 ≤ 21. (Inferred from the observed rank ceiling and histogram width, not read
from ClickHouse source.) A rank of 21 means an all-zero tail — which is why `uniqHLL12State`
over `numbers(n)` always shows exactly one register at 21: the value `0` hashes to `0`.

The 5-bit field could hold up to 31, but **values above 21 must never be written** — they would
overflow the 22-slot histogram and corrupt the state.

---

## 3. Side by side

| | DataSketches HLL (Druid) | ClickHouse `uniqHLL12` |
| --- | --- | --- |
| Precision | `lgK` configurable 4–21, per column | **fixed 12** (4096 buckets) |
| Sparse form | coupon array (hash-derived, portable) | raw `UInt64` hashes, ≤ 16 |
| Dense register width | 4 / 6 / 8 bits (`tgtHllType`) | **5 bits**, fixed |
| Dense size at lgK=12 | 2088 / 3113 / 4136 B | **2651 B**, always |
| Sub-byte packing | HLL_6 packed; HLL_4 nibbles + `curMin` + exceptions | LSB-first 5-bit fields |
| Rank ceiling | ~63 (6-bit coupon field, 64-bit hash lane) | **21** (32-bit hash, 20-bit tail) |
| Hash | MurmurHash3-128, seed 9001 | ClickHouse's own, 32-bit |
| Estimator bookkeeping | HIP accumulator + KxQ sums in preamble | rank histogram + zero count |
| Estimator | HIP (order-dependent) or composite | own bias correction + linear counting |
| Parameters in the bytes | `lgK` and target type recoverable | nothing to recover — all fixed |
| Format stability | documented, versioned, cross-language | **internal, no guarantee** |

The structural similarity is real — both are 4096 one-value-per-bucket register arrays at
`lgK`/precision 12 — and that is exactly what makes a transplant possible. Everything that
differs is either mechanical (bit packing, bookkeeping) or, in the case of the hash function,
the thing that makes cross-merging impossible.

---

## 4. Why transplant and merge work — but not with native sketches

### The estimator does not care which bucket is which

An HLL estimate depends only on the *multiset* of register values:

```
E  =  α_m · m²  /  Σ_j 2^(−M[j])
```

Bucket labels `j` appear only as summation indices. **Permuting the registers leaves the estimate
unchanged.** So copying a DataSketches register array into a ClickHouse state preserves the
cardinality estimate even though the two systems would have put any given key in a different
bucket. The register array is a valid HLL sketch of the same set; it is simply written in a
different coordinate system.

That is the whole basis of the transplant, and it is why `migrate_hll_transplant.py` gets within
a few percent of Druid (measured: 3.49% on users, 0.61% on pages across all 2486 rows;
0.25%–1.31% per channel). The residual is not the permutation — it is that ClickHouse applies its
own bias correction and linear-counting thresholds to the same registers.

### Merging is a bucket-wise max, and that needs one shared key→bucket map

Union of two HLL sketches is elementwise:

```
M_union[j]  =  max(M_A[j], M_B[j])
```

This is correct precisely because a key `k` deterministically produces the same pair
`(bucket(k), rank(k))` in both sketches. Deduplication is a *consequence* of that determinism:
seeing `k` again writes the same rank into the same bucket, and `max` is idempotent, so it
contributes nothing new.

**Transplanted sketches all share DataSketches' hashing**, so they all use the same map. Their
registers are permuted relative to ClickHouse's convention, but permuted *identically*, and a
bucket-wise max over a common permutation is still the correct union. Verified: A (1000 keys) and
B (1000 keys, 500 shared) merge inside ClickHouse to **1509** against a truth of 1500 and a
DataSketches union of 1508 — and re-adding A leaves it at **1509**, so duplicates still vanish.

### A native sketch uses a different map, so the union double-counts

Mix the two and the shared-map premise breaks. Key `k` lands in bucket `b₁` with rank `r₁` under
MurmurHash3, and in an unrelated bucket `b₂` with rank `r₂` under ClickHouse's hash. Those two
placements are effectively independent, so the same key occupies two unrelated slots. Taking the
max no longer recognises them as the same element — it behaves like unioning two *disjoint* sets.

Measured on the same 1000 values:

| | estimate |
| --- | --- |
| transplanted alone | 989 |
| native alone | 1001 |
| **merged** | **2022** |

Truth is 1000. The overlap was total and none of it was detected. Note the failure is *silent*:
the merged state is structurally valid, the query succeeds, and the number is simply wrong.

The practical rule follows directly: **nothing may ever write a transplanted column with
`uniqHLL12State()`.** A single native insert corrupts every historical number in that column,
with no error and no way to tell from the data which rows came from where. If a column may
receive native writes, use `migrate_hll.py` instead.

### Why Theta escapes this and HLL cannot

The contrast is instructive. A Theta sketch stores **the hash values themselves**, and both
systems hash with MurmurHash3 under the DataSketches default seed — verified by comparing
retained hash sets (1000/1000 identical) and by matching seed hashes (37836 on both sides). Since
Theta compares hashes rather than bucket positions, a transcoded Theta sketch unions correctly
with a native ClickHouse one: measured 1000 for an identical set and 1500 for a 50%-overlapping
one, i.e. true deduplication.

HLL has no equivalent escape, and not because of an implementation detail: HLL keeps only a
max rank per bucket and has **thrown the hashes away**. There is nothing left to compare across
systems, and no way to re-derive the original keys to re-hash them. The same asymmetry is why
Theta → HLL is possible but HLL → Theta is not.

---

## 5. Consequences for the migration

- **Fold to lgK=12 with `hll_union`, never by hand.** Reducing precision turns dropped index bits
  into rank bits; it is not a bucket-wise max over grouped registers. Folding the fixture's
  `lgK=14` column halves its precision (~0.81% → ~1.63% RSE), which is a cost of the transplant
  path that `migrate_hll.py` avoids.
- **Convert to `HLL_8` before reading registers**, so the payload is a flat byte array rather
  than `HLL_4`'s nibbles-plus-exceptions.
- **Clamp ranks to 21.** Statistically irrelevant (P(rank > 21) ≈ 2⁻²¹ per item) but structurally
  mandatory.
- **Handle the sparse forms.** Druid's stored rollup sketches are usually LIST/SET, so a
  register extractor that only understands dense sketches will fail on the common case.
- **Re-validate after any ClickHouse upgrade.** `spikes/reverse_uniqhll12.py` is the gate.
- **Do not compare update-sketch estimates** when checking fidelity — they are HIP-based and
  order-dependent. Compare registers, or post-union estimates, or use the sketch's own error
  bounds as the tolerance.
