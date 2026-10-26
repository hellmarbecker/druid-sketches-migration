# Migrating Druid sketches to ClickHouse

*How theta and HLL sketches work, how each engine stores aggregation state, and how to move pre-aggregated sketch columns from Apache Druid to ClickHouse without going back to the raw data.*

If you run Apache Druid with rollup, some of your most valuable columns are probably sketches. A rollup table discards the raw events at ingestion time. What's left of "how many distinct users did we see per hour and channel" is a sketch column. If you migrate to ClickHouse, you can't re-ingest those events, because they no longer exist. The sketches have to come across as they are.

This post walks through the whole problem: what the sketches are, how both engines keep aggregation state, and the two very different stories for theta and HLL. All the findings come from a working migration toolkit, with tests, verified against Druid 37 and ClickHouse 26.8, and re-validated on 26.10: [github.com/hellmarbecker/druid-sketches-migration](https://github.com/hellmarbecker/druid-sketches-migration).

---

## A short glossary

A few terms come up again and again, so here they are up front.

**HLL terms**

- **lgK**: log2 of the number of buckets K. lgK = 12 means 4096 registers. A higher lgK means more accuracy (error ≈ 1.04/√K) and more memory. Druid lets you choose 4–21 per column. In ClickHouse, `uniqHLL12` fixes it at 12, while `uniqCombined64` lets you choose 12–20.
- **Register**: one small counter per bucket. It holds the highest rank seen for any value that hashed into that bucket. The full array of registers is the dense sketch. Druid stores registers 4, 6 or 8 bits wide. ClickHouse uses 5 bits in `uniqHLL12` and 6 bits in `uniqCombined64`.
- **Rank**: the number of leading zeros in a value's hash bits, plus one. (ClickHouse counts trailing zeros instead, which is statistically the same thing.) A rank of *r* turns up with probability 2⁻ʳ, so high ranks are evidence of many distinct values. Druid allows ranks up to about 63. ClickHouse caps them at 21 in `uniqHLL12`, and at 65 − lgK in `uniqCombined64`.
- **Slot**: the hash bits that pick a value's bucket. In a Druid coupon it is a 26-bit field, and the register index is its low lgK bits, `slot & (2^lgK − 1)`. That's why an lgK 14 sketch can be folded down to lgK 12.
- **Coupon**: Druid's sparse-mode entry. One `UInt32` packs the slot (bits 0–25) and the rank (bits 26–31). Small sketches store a list of coupons instead of a full register array. A coupon is not a raw hash, so the original keys can't be recovered from it.

**Theta and ClickHouse terms**

- **θ (theta)**: the sampling threshold of a theta sketch. Only hashes below θ are kept, and the estimate is retained / θ.
- **k (nominal entries)**: how many hashes a theta sketch aims to keep. Druid calls it `size`; ClickHouse fixes it at 4096.
- **Seed hash**: a 16-bit fingerprint of the hash seed, stored in the theta preamble. The value 37836 corresponds to the DataSketches default seed, 9001.
- **Sparse vs dense**: sparse means a coupon list (Druid's LIST/SET modes), used while few values have been seen. Dense means a full register array (HLL mode).
- **Aggregate state**: a partial aggregation result stored in a ClickHouse `AggregateFunction` column. You build it with `-State` and finalize it with `-Merge`.
- **LEB128**: a variable-length integer encoding, 7 bits per byte. It is the length prefix of a `uniqTheta` state.

---

## 1. What are datasketches?

### How they are used

A sketch is a fixed-size probabilistic summary of a set. [Apache DataSketches](https://datasketches.apache.org/) provides a whole family of them. For counting distinct values, the two that matter are **theta** and **HLL** (HyperLogLog). Both do three jobs in an analytics database:

1. **Count distinct.** Users, sessions or pages per time bucket, in kilobytes instead of the full set of IDs, with error bounds attached to the estimate.
2. **Merge across rows.** The union of two sketches is the sketch of the union. Hourly rows roll up to daily ones, per-channel rows to a total, and overlap is never double-counted. That is the whole point: you can't get a daily distinct count by adding up hourly counts.
3. **Set operations.** Theta supports union, intersection and A-not-B, so it can answer questions like "users seen on both days". HLL supports union only.

### How theta sketches work

Theta is the easier one to picture.

1. **Hash** each value to a point in [0, 1). Duplicates hash to the same point, so they count once.
2. **Keep** only the hashes below a threshold θ, at most k of them.
3. **Estimate** the distinct count as retained / θ. The points are uniformly distributed, so the density below θ extrapolates to the whole interval.
4. **Store** the retained hashes themselves, plus θ.

```
 θ = 0.31
 ├──────────┤
 ●  ●  ●  ●  │ ○   ○   ○   ○   ○   ○   ○   ○   ○
 0                                                1

 4 retained / 0.31 ≈ 13 distinct   (truth: 13)
```

Storing real hash values is what makes theta so flexible. To combine two sketches, take the smaller θ and keep every hash below it from both sides. Hashes that appear in both collapse into one, so the overlap is detected rather than double-counted:

| sketch | θ | retained hashes |
|---|---|---|
| A | 0.5 | 0.06, 0.18, 0.30, 0.42 |
| B | 0.4 | 0.06, 0.13, 0.30, 0.36 |
| A ∪ B | min = 0.4 | 0.06, 0.13, 0.18, 0.30, 0.36 → 5 / 0.4 ≈ 12.5 |

A's hash 0.42 is dropped because it's above the new θ, and the shared hashes 0.06 and 0.30 count once.

Intersection works the same way, keeping only the hashes present in both. A-not-B keeps the hashes present only in A.

### How HLL sketches work

HLL keeps far less: one small counter per bucket.

1. **Hash** each value. The first p bits pick one of m = 2^p buckets (p is lgK).
2. **Rank** is the number of leading zeros in the remaining bits, plus one. Long runs of zeros are rare, so a high rank suggests many values.
3. **Register**: set M[j] = max(M[j], rank). Seeing a duplicate changes nothing.
4. **Estimate** with a harmonic mean over all registers:

$$
E = \frac{\alpha_m \, m^2}{\displaystyle\sum_{j=1}^{m} 2^{-M[j]}}
$$

Here αₘ is the **bias-correction factor**, about 0.721 for m = 4096. The raw harmonic mean is systematically biased, and αₘ corrects for that.

A worked example with 16 buckets: `hash("alice") = 0110 0001 0110 1011`. The first four bits, `0110`, select bucket 6. They are followed by three zeros and a one, so the rank is 4, and register 6 becomes max(M[6], 4).

Only the maximum rank per bucket survives. The hashes themselves are gone. That is why HLL can't do intersections, and why there is no way back from an HLL sketch to the keys that built it.

### How HLL sketches are merged

Merging two HLL sketches means taking the maximum, register by register:

```
           0  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15
 A         2  0  1  3  0  2  4  1  0  2  1  5  0  1  2  3
 B         1  3  1  0  2  2  4  2  0  0  3  2  1  0  2  1
                     max(A[j], B[j]) for every j
 A ∪ B     2  3  1  3  2  2  4  2  0  2  3  5  1  1  2  3
```

This works because a given key always lands in the same bucket with the same rank, so the max over two sketches is exactly the sketch of the combined input. A few properties fall out of that:

- **Duplicates vanish.** max is idempotent: seeing a key twice, in one sketch or spread across many, changes nothing. "alice" is in both sketches above, and register 6 stays at 4.
- **Order doesn't matter.** max is commutative and associative, so rows can roll up in any order or grouping.
- **Preconditions:** both sides must use the same hash function and seed, and the same lgK. A sketch with a larger lgK can be folded down first.

In SQL this is `DS_HLL(col)` in Druid, and `uniqHLL12Merge(state)` or `uniqCombined64Merge(lgK)(state)` in ClickHouse.

Keep that "same hash function" precondition in mind. It's the source of the biggest trap in this whole migration.

### When to use each type

| | Theta | HLL |
|---|---|---|
| What it stores | Retained hash values + θ | Max rank per bucket |
| Size at ~1.6% error | ~32–64 KB (k = 4096) | ~2–4 KB (lgK = 12) |
| Union | Yes | Yes |
| Intersection, A-not-B | Yes | No |
| Convert to the other | Theta → HLL possible | HLL → theta impossible |
| Native ClickHouse merge after migration | **Yes, byte-compatible** | Only via transplant, with caveats |

HLL is smaller per unit of accuracy. Theta is more flexible and, as we'll see, far more portable. The asymmetry in the "convert" row is permanent. Theta keeps the hashes, so you can always build an HLL sketch from it. HLL has thrown them away, so you can never go the other way.

---

## 2. Aggregating data in Druid and ClickHouse

Both engines store partial aggregation state rather than final numbers. They just do it in different places.

### Druid: rollup and how it stores aggregation state

In Druid, rollup happens at ingestion. You declare a `queryGranularity` and a `metricsSpec`, and Druid collapses all events that share a truncated timestamp and the same dimension values into one row:

| time | channel | user |
|---|---|---|
| 10:02 | #en | alice |
| 10:17 | #en | bob |
| 10:41 | #en | alice |
| 10:55 | #en | carol |

becomes, at hourly granularity:

| __time | channel | count | users_theta |
|---|---|---|---|
| 10:00 | #en | 4 | `COMPLEX<thetaSketch>` |

The three distinct users now live inside the sketch. A few details matter for migration:

- The `metricsSpec` defines the aggregators: `count`, `longSum`, `HLLSketchBuild` (with `lgK` and `tgtHllType`) and `thetaSketch` (with `size`).
- Sketch columns are typed `COMPLEX<HLLSketch>` or `COMPLEX<thetaSketch>` and hold serialized DataSketches bytes.
- If you build the table with SQL-based ingestion (MSQ), set `finalizeAggregations: false`. Without it, the sketch columns silently land as plain numbers, which destroys exactly what you're trying to migrate.
- Druid SQL returns sketch columns as base64 inside a *quoted* JSON string, so strip the inner quotes before decoding.

### ClickHouse: AggregatingMergeTree and the -State and -Merge modifiers

ClickHouse keeps aggregation state in ordinary columns of type `AggregateFunction(f, T)`. Any aggregate function `f` can be asked for its intermediate state with the `-State` modifier, and states can be combined and finalized with `-Merge`:

```sql
CREATE TABLE rollup (
  ts          DateTime,
  channel     String,
  cnt         SimpleAggregateFunction(sum, UInt64),
  users_theta AggregateFunction(uniqTheta, String)
) ENGINE = AggregatingMergeTree
ORDER BY (channel, ts);

-- write: build partial state
INSERT INTO rollup
SELECT toStartOfHour(t), channel, count(), uniqThetaState(user)
FROM events GROUP BY 1, 2;

-- read: merge states across rows
SELECT channel, uniqThetaMerge(users_theta)
FROM rollup GROUP BY channel;
```

`AggregatingMergeTree` then merges rows that share the same `ORDER BY` key in the background, which is ClickHouse's equivalent of rollup. One gotcha: plain measure columns must be declared as `SimpleAggregateFunction(sum, …)`. The engine rejects bare numeric columns, because merging would otherwise keep an arbitrary row's value.

So the migration question becomes: can we turn Druid's sketch bytes into ClickHouse aggregate states? For theta the answer is a clean yes. For HLL it's complicated.

---

## 3. How to migrate theta sketches from Druid to ClickHouse

### Basically the same format

ClickHouse's `uniqTheta` is built on Apache DataSketches too, and the formats line up almost perfectly:

- **Same seed hash, 37836, on both sides.** Both hash with MurmurHash3 and the DataSketches default seed of 9001, so the same key produces the same hash value in both systems.
- **Same preamble:** `preLongs = 2`, `serVer = 3`, family 3 (COMPACT), flags `0x1a`.
- **Real cross-system dedup.** A transcoded Druid sketch unioned with a ClickHouse-built sketch over the *same* 1000 values gives 1000. Over two sets of 1000 that share 500 values, it gives exactly 1500.

No rebuild, no re-hashing: the bytes Druid stores are already a valid theta sketch as far as ClickHouse is concerned.

### Adding the header

A `uniqTheta` aggregate state is exactly a LEB128 length prefix followed by the compact sketch bytes. Nothing else.

```
┌─────────────┬──────────────────────────────────┬──────────────────────────────┐
│ LEB128(len) │ preamble                         │ retained hashes              │
│             │ preLongs=2 · serVer=3 · family=3 │ (UInt64 × N, sorted)         │
│             │ flags=0x1a · seed hash 37836     │                              │
└─────────────┴──────────────────────────────────┴──────────────────────────────┘
  added for     Druid's compact theta sketch, untouched
  ClickHouse
```

LEB128 encodes 7 bits per byte, with the high bit meaning "more follows": 8016 becomes `D0 3E`, 16384 becomes `80 80 01`. In Python the whole transcode is `varint(len(b)) + b`. In SQL it's `concat(leb128(length(s)), s)`.

Two things to watch out for:

- **Use 64-bit (or String) argument types only.** DataSketches widens every input to 64 bits before hashing, while ClickHouse hashes values at their declared width. A `uniqTheta` column over `UInt32` therefore hashes differently, and a migrated state merged with a native one silently double-counts. Measured: 2000 instead of 1000. `String`, `UInt64`, `Int64` and `Float64` are safe.
- **k is fixed at 4096 in ClickHouse.** Druid columns built with `size = 16384` keep their full precision at rest, but are downsampled to k ≈ 4096 on any cross-row merge. Expect ~1.6% relative error instead of ~0.8%.

### Doing it in pure SQL

The enabling trick is that `CAST(<String> AS AggregateFunction(...))` deserializes aggregate-state bytes. It isn't documented as a conversion, but it works. That means ClickHouse can do the entire migration itself:

```sql
INSERT INTO rollup_theta
WITH
  (L -> multiIf(L < 128,   char(L),
                L < 16384, char(bitOr(bitAnd(L,127),128), bitShiftRight(L,7)),
                           char(bitOr(bitAnd(L,127),128),
                                bitOr(bitAnd(bitShiftRight(L,7),127),128),
                                bitShiftRight(L,14))))           AS leb128,
  (b -> base64Decode(trim(BOTH '"' FROM b)))                     AS unwrap,
  (s -> CAST(concat(leb128(length(s)), s)
           AS AggregateFunction(uniqTheta, String)))             AS to_state
SELECT parseDateTimeBestEffort(__time), channel, countryName, isRobot,
       `count`, sum_added, to_state(unwrap(users_theta_16384))
FROM executable('druid_query.py', 'JSONEachRow', '<schema>',
     (SELECT $$SELECT * FROM wikipedia_rollup_sketches$$));
```

Reading from Druid needs a small helper. Druid's SQL endpoint only accepts POST, and ClickHouse's `url()` table function only issues GET. `druid_query.py` bridges that gap through `executable()`. Alternatives are a stock nginx shim that turns a GET into a POST, or plain `file()` over an exported JSONL file. All three routes produce states byte-identical to the Python migration.

---

## 4. Anatomy of an HLL sketch

Theta was easy because both engines speak DataSketches. For HLL they don't. ClickHouse has two HLL functions, `uniqHLL12` and `uniqCombined64`, and both are its own implementation. (`uniqCombined64` is easy to miss: its name doesn't say HLL.) To see what that means, we need to look at both formats byte by byte.

### The different formats in Druid

**One preamble.** The first 8 bytes are common to every DataSketches HLL representation:

| byte | meaning |
|---|---|
| 0 | `preInts`: preamble size in 4-byte words |
| 1 / 2 | `serVer` = 1 / `familyId` = 7 |
| **3** | **lgK: log2 of the bucket count (4–21)** |
| 4 / 5 | `lgArr` / flags |
| 6 | `listCount` (LIST) or `curMin` (HLL_4) |
| **7** | **bits 0–1: current mode; bits 2–3: target type** |

The parameters you need are recoverable from the bytes, so read them per column rather than assuming Druid's defaults.

**Three modes.** A sketch changes representation as it fills, and all three can appear in the same column:

- **LIST** (`preInts` 2, 8-byte preamble): a sparse array of 4-byte coupons
- **SET** (`preInts` 3, 12-byte preamble): a hash set of coupons
- **HLL** (`preInts` 10, 40-byte preamble): a dense register array

In the test fixture, every stored rollup row was sparse (248–632 bytes). Only merges across the whole datasource went dense. In a rollup table, sparse is the common case, not an edge case.

**Three dense register widths.** Once dense, `tgtHllType` decides how registers are packed (sizes at lgK = 12):

- **HLL_8**, 4136 bytes: a 40-byte preamble plus 4096 one-byte registers. This is the only form that is a flat array.
- **HLL_6**, 3113 bytes: registers bit-packed at 6 bits each.
- **HLL_4**, about 2090 bytes: 4-bit offsets from `curMin`, with anything that doesn't fit spilled into an auxiliary exceptions table. This is Druid's default.

Bytes 8–39 of the dense preamble hold estimator bookkeeping (the HIP accumulator and KxQ sums). HIP is order-dependent: two sketches with identical registers can report different estimates. Convert to HLL_8 before reading registers.

### How the format in ClickHouse differs from all of them

`uniqHLL12`'s state format is undocumented. The repository decodes it by differential analysis and validates it with a byte-exact round-trip of states ClickHouse produced itself. The dense ("large") form is always **2651 bytes**:

```
┌──────────┬──────────────────────────────────┬────────────────────┬──────────────┐
│ is_large │ 4096 registers × 5 bits,         │ histogram          │ zero count   │
│ 1 B      │ LSB-first · 2560 B               │ 22 × UInt32 · 88 B │ UInt16 · 2 B │
└──────────┴──────────────────────────────────┴────────────────────┴──────────────┘
```

The sparse ("small") form, used for up to 16 values, stores raw `UInt64` ClickHouse hashes. You can't write it from a DataSketches sketch, which no longer has the hashes, so a migration always emits the dense form.

| | DataSketches HLL (Druid) | ClickHouse `uniqHLL12` |
|---|---|---|
| Precision | lgK 4–21, per column | fixed 12 (4096 buckets) |
| Register width | 4 / 6 / 8 bits | 5 bits |
| Rank ceiling | ~63 | 21 |
| Format contract | documented, versioned | internal, no guarantee |

#### The other ClickHouse HLL: `uniqCombined64`

`uniqCombined64(lgK)` takes the precision as a parameter, from 12 to 20. Its state starts with a type byte for one of three tiers: a small array of exact hashes, then a hash set, then a real HLL. Only the HLL tier can be written from a DataSketches sketch, for the same reason as `uniqHLL12`'s sparse form. That tier is the `uniqHLL12` layout scaled up:

```
┌──────────┬──────────────────────────────────┬────────────────────────┬────────────────┐
│ type = 3 │ K registers × 6 bits,            │ histogram              │ zero count     │
│ 1 B      │ LSB-first                        │ (66 − lgK) × UInt32    │ UInt16/UInt32  │
└──────────┴──────────────────────────────────┴────────────────────────┴────────────────┘
```

That's 3291 bytes at lgK 12 and 12499 at lgK 14. Each precision is a separate aggregate type, so states with different lgK can't be merged with each other. This layout was read from the ClickHouse source rather than reverse-engineered, then checked with the same byte-exact round-trip.

The big win is that it can hold an lgK 14 column without folding it.

#### Buckets and coupons

A Druid coupon is a single `UInt32`:

```
┌──────────────────────────────────────────┬───────────────────┐
│ slot · bits 0–25                         │ rank · bits 26–31 │
└──────────────────────────────────────────┴───────────────────┘
register index = slot & (2^lgK − 1)        rank = coupon >> 26
```

ClickHouse packs its registers into 5-bit fields, LSB-first, so registers straddle byte boundaries. One output byte can draw on up to three registers. Ranks above 21 must never be written: they would overflow the 22-slot histogram and corrupt the state.

#### Hash functions

This is the deepest difference, and the one that can't be bridged.

- **DataSketches** hashes with 128-bit MurmurHash3, seed 9001. One 64-bit lane supplies the slot; the other supplies the rank (leading zeros + 1). Slot and rank are therefore independent, and ranks can go up to about 63. That independence is also why folding lgK 14 down to 12 is just a max over registers that share their low bits.
- **ClickHouse** uses its own hash. In `uniqHLL12` it's 32 bits: the low 12 bits pick the bucket, leaving a 20-bit tail for the rank, which caps it at 21. `uniqCombined64` uses a 64-bit hash, so its ranks go up to 65 − lgK. In both, the rank is the trailing zeros of the tail plus one.

Neither HLL format carries a seed hash. Unlike theta, there is nothing in the bytes that would even tell you the two systems disagree.

---

## 5. Migrating HLL sketches from Druid to ClickHouse

There are two working paths. You can export the answers, or you can transplant the registers.

### The obvious way: export the finalized aggregates you are going to need

Pre-merge the Druid sketches in Python with `hll_union` at every grain your warehouse needs, and write the estimate plus lower and upper bounds into plain numeric columns:

```
hour_dims     · 2486 rows
  hour_channel  ·  909 rows
    day_channel   ·   51 rows
      day_total     ·    1 row
```

Every number is exactly what Druid would have reported at that grain. What you lose is query-time mergeability, and pre-merging at the grains you need is what buys it back.

What you must **never** do is sum estimates across rows to fake a union. On the test fixture, that inflates the daily total by 1.6× (17193 instead of 10564).

As a companion, you can also carry the raw sketch bytes verbatim in a `String` column. ClickHouse can't merge them natively, but an executable UDF (`hllMergeEstimate`) can union them via DataSketches at query time. This reproduces the pre-merged values bit for bit. It's lossless at rest and keeps the option to re-roll later.

One trap when validating: Druid's `APPROX_COUNT_DISTINCT_DS_HLL` merges at lgK 12 by default, regardless of the column's actual lgK. Pass the real lgK, or a faithful migration will look broken.

### Exporting aggregation state

If you want native merging in plain SQL, you can build real ClickHouse states from Druid's registers. There are two targets: `uniqHLL12`, which needs every column folded to lgK 12, and `uniqCombined64(lgK)`, which keeps the column's own lgK. The steps below are for `uniqHLL12`; for `uniqCombined64` you skip the fold, clamp at 65 − lgK, and pack 6-bit registers.

#### How to map the coupons and buckets

1. **Read the header**: lgK from byte 3, mode and target type from byte 7.
2. **Get the registers**: sparse sketches give you a coupon list; dense ones should be converted to HLL_8 first.
3. **Fold to 4096 buckets**: bucket = `slot & 4095`, rank = `coupon >> 26`, keeping the max per bucket. Masking with 4095 also folds lgK 14 to 12.
4. **Clamp**: rank = min(rank, 21). It's statistically irrelevant (the probability of a rank above 21 is about 2⁻²¹ per item), but structurally mandatory.
5. **Pack**: 5-bit registers, plus histogram and zero count, then `CAST` to `AggregateFunction(uniqHLL12, String)`.

The heart of the SQL-only version looks like this:

```sql
-- preamble length = preInts * 4
SELECT *, bitAnd(reinterpretAsUInt8(substring(sk, 1, 1)), 63) * 4 AS pre

-- coupons: UInt32 each, after the preamble
arrayMap(i -> reinterpretAsUInt32(substring(sk, pre + 1 + i*4, 4)),
         range(intDiv(length(sk) - pre, 4)))                      AS cps

-- max rank per bucket; masking with 4095 also folds lgK 14 -> 12
arrayReduce('maxMap',
  [arrayMap(c -> toUInt16(bitAnd(c, 4095)), cps)],
  [arrayMap(c -> toUInt8(least(bitShiftRight(c, 26), 21)), cps)]) AS mm

-- then: pack 4096 x 5 bits, append histogram + zero count,
-- unhex(...) and CAST AS AggregateFunction(uniqHLL12, String)
```

The SQL version handles sparse input only, so run the guard query first. It also has to be chunked (about 96 rows per run); otherwise ClickHouse tries to allocate 24 GiB broadcasting the register array.

How well does it work? Native `uniqHLL12Merge` across all rows lands within 3.46% (users) and 0.59% (pages) of Druid.

The users gap isn't random noise. ClickHouse estimates small cardinalities with linear counting, then switches to the raw HLL formula at 2.5 × K. Just past that switch the raw formula runs high. HLL++ fixes this with a table of empirical bias corrections, but ClickHouse only ships that table for lgK 17. With about 10,500 distinct users in 4096 buckets, the users column sits at 2.57 × K. Over 40 trials at that cardinality, ClickHouse overcounted by 2.22% on average, while DataSketches averaged +0.12% over exactly the same registers. An lgK 12 column can't be raised to lgK 17, so neither target fixes this.

Precision is where the two targets differ. Folding the lgK 14 pages column to 12 for `uniqHLL12` halves its precision. `uniqCombined64(14)` keeps all 16384 registers. Over 40 trials at the pages cardinality, measured against the exact count:

| | RMSE |
|---|---|
| DataSketches (source) | 0.50% |
| `uniqCombined64(14)`, unfolded | 0.92% |
| `uniqHLL12`, folded to 12 | 1.59% |

The test fixture on its own pointed the other way: +1.31% for `uniqCombined64` against +0.71% for `uniqHLL12`, both compared with the exact count. That single draw was about 1.4 standard deviations out, which is why one dataset isn't enough to compare estimators.

Both formats are internal to ClickHouse, so re-validate them after every upgrade.

#### Caveat: you can never merge with ClickHouse native data

Transplanted states and states built natively with `uniqHLL12State()` have the same type, so ClickHouse will happily merge them. The same goes for `uniqCombined64`. The result is wrong, and nothing warns you. Measured on the same 1000 values:

| | estimate |
|---|---|
| Truth | 1000 |
| Transplanted (Druid sketch, registers copied into `uniqHLL12`) | 989 |
| Native (`uniqHLL12State()` over the raw values) | 1001 |
| **Merged** | **2022** |

The overlap was total, and none of it was detected.

**Why?** Go back to how HLL merging works: a register-by-register max is only correct if both sides share one key → bucket map.

```
                    ┌─ MurmurHash3     → bucket 1873, rank 3
 key "alice" ───────┤
                    └─ ClickHouse hash → bucket  402, rank 1
                                         (illustrative bucket numbers)
```

The same key lands in two unrelated slots, so the max no longer recognizes it as the same element. The merge behaves like a union of two disjoint sets.

That also explains why the transplant works at all. The HLL estimate depends only on the *multiset* of register values, not on which bucket holds which value. A Druid register array copied into a ClickHouse state is still a valid sketch of the same set, written in a different coordinate system. Transplanted states all share DataSketches' hashing, so they merge correctly with *each other*: A ∪ B = 1509 against a truth of 1500, and re-adding A leaves it unchanged. It's only the mix with native states that breaks.

Theta escapes all of this because it stores the hashes, and both sides use the same hash. HLL threw them away.

The practical rule: **nothing may ever write a transplanted column with `uniqHLL12State()` or `uniqCombined64State()`.** For `uniqCombined64` we measured the same thing: 1000 shared values merged to 2012. A single native insert corrupts every historical number in that column, with no error and no way to tell afterwards which rows came from where. If a column has to accept native writes, use the finalized-values path instead.

---

## 6. Conclusion

Choose the path by what the column has to do after migration.

1. **Theta → `uniqTheta`.** Lossless: add a LEB128 header and you're done. The result merges correctly with native ClickHouse data. Use 64-bit or `String` argument types. Wherever you can still rebuild a column, prefer theta.
2. **HLL → finalized values.** Exact Druid numbers at every grain you pre-merge, mergeable with anything, optionally backed by the raw bytes and a UDF.
3. **HLL → transplant.** Native merging in plain SQL. Use `uniqCombined64(lgK)` to keep the column's own lgK, or `uniqHLL12` if everything is lgK 12 anyway. Expect ~1–4% drift; at lgK 12 part of it is a systematic overcount by ClickHouse's estimator. Never mix with native states, and re-validate after every upgrade.

Code, tests and the full format notes are in the repository: [github.com/hellmarbecker/druid-sketches-migration](https://github.com/hellmarbecker/druid-sketches-migration).
