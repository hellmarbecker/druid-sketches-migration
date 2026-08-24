# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project goal

A Python adapter that migrates **Apache Druid rollup tables to ClickHouse**, with the hard part
being the **DataSketches columns**: HLL sketches and Theta sketches.

The adapter reads existing sketch values out of Druid and transforms them into a sketch
representation ClickHouse understands — the point is to preserve the pre-aggregated rollup
state, not to re-ingest raw events (which in a rollup table no longer exist).

Both sides are assumed to be current releases.

## Commands

Plain **pip + `requirements.txt`** — do not introduce uv/poetry/pdm.

```bash
python3 -m venv .venv                     # Python 3.14.6 on PATH
.venv/bin/pip install -r requirements.txt
.venv/bin/python …                        # or `source .venv/bin/activate`
```

`datasketches` has no cp314 wheel on PyPI and builds from source on install (~1 min, needs a
C++ toolchain). Add new direct dependencies to `requirements.txt` with a pinned version and a
comment saying why; leave transitive deps out.

### Tests

**pytest**, configured in `pytest.ini`, tests in `tests/`. No linter is chosen yet — ask before
picking one.

```bash
.venv/bin/pytest                      # everything; integration tests self-skip if servers are down
.venv/bin/pytest -m "not integration" # offline only, ~0.3 s
.venv/bin/pytest -m clickhouse        # one system's integration tests
.venv/bin/pytest tests/test_uniqhll12.py -k clamp   # single file / single test
```

The suite splits along one line: **offline tests need nothing running**, and cover the fragile
pure logic — the reverse-engineered `uniqhll12.py` codec and the RowBinary encoders in
`sketch_io.py`. **Integration tests** are marked `druid` / `clickhouse` (plus an umbrella
`integration`) and *skip themselves* when their server is unreachable, so a bare `pytest` run
always works. `conftest.py` probes reachability once per session and caches it; the Druid probe
also requires the fixture datasource to exist, so a bare Druid does not produce failures that
have nothing to do with the code.

Integration tests own the claims no offline test can reach: that the `uniqHLL12` state format
still matches this ClickHouse build (byte-exact round-trip of states ClickHouse produced), that
the DataSketches seed hash still matches across systems, and the two HLL merge behaviours.
`test_transplanted_and_native_states_must_not_be_mixed` asserts the breakage *stays* broken — if
it ever starts passing, the hashes have converged and the docs need revisiting.

These do not replace the `verify()` functions in the migration scripts, which check a full
end-to-end migration against live data. The suite pins the units those scripts are built from.

Tests that touch ClickHouse create and drop their own `_t_*` tables and never write to the
migration targets.

### Druid test fixture

`druid-datasketches` is already in the `loadList` of `$DRUID_HOME/conf/druid/auto`, so no config
change is needed.

```bash
cd $DRUID_HOME && bin/start-druid -m 6g            # ~30 s to healthy; logs in $DRUID_HOME/log
curl -s http://localhost:8888/status/health         # -> true

# Build the rollup datasource (idempotent: rerun to rebuild).
curl -s -X POST -H 'Content-Type: application/json' \
  -d @druid/rollup-sketches-index.json \
  http://localhost:8888/druid/indexer/v1/task
# Poll: /druid/indexer/v1/task/<id>/status -> SUCCESS (a few seconds on this data)

.venv/bin/python spikes/inspect_sketches.py         # introspect the real sketch bytes
```

### ClickHouse fixture

The single binary needs a config; `clickhouse/config.xml` is a minimal one (HTTP 8123, data under
`$HOME/.local/share/clickhouse-druid-migration`, no auth, localhost only — fixture only).

```bash
clickhouse server --config-file=clickhouse/config.xml    # background it
curl -s 'http://localhost:8123/?query=SELECT%20version()'
```

Then any of the migrations (each migrates and self-verifies, ending in `ALL CHECKS PASSED`):

```bash
.venv/bin/python migrate_theta.py           # Theta -> uniqTheta (lossless)
.venv/bin/python migrate_hll.py             # HLL  -> pre-merged estimates + carried bytes
.venv/bin/python migrate_hll_transplant.py  # HLL  -> native uniqHLL12 states
.venv/bin/python migrate_quantiles.py       # quantiles -> carried bytes + native t-digest
.venv/bin/python spikes/reverse_uniqhll12.py   # re-validate the uniqHLL12 format
```

Each migration recreates its target table, so all are safe to re-run.

`druid/rollup-sketches-index.sql` is a verified SQL-based (MSQ) translation of the same spec,
kept for reference; it writes `wikipedia_rollup_sketches_sql` so it cannot clobber the fixture.
Its header documents the submit recipe and two context settings that matter —
`finalizeAggregations: false` (else sketch columns land as plain numbers) and
`maxNumTasks: 2` (exceeding the middleManager's `druid.worker.capacity` deadlocks silently).

`druid/rollup-sketches-index.json` builds `wikipedia_rollup_sketches` from the bundled wikiticker
sample: HOUR rollup, 3 dimensions, 39,244 events → 2,486 rows (15.8x). It carries **six** sketch
columns with deliberately different parameters (`lgK` 12/14, `HLL_4`/`HLL_8`, theta `size`
16384/4096) so code cannot get away with assuming defaults, plus **two quantiles columns over
the same field** — classic `quantilesDoublesSketch` at k=256 and `KllDoublesSketch` at k=200 —
so the two families can be compared directly.

## Current state

Four working migration paths, each a self-verifying script at the repo root:
`migrate_theta.py`, `migrate_hll.py`, `migrate_hll_transplant.py`, `migrate_quantiles.py`. Shared plumbing lives in
`sketch_io.py` (Druid reads, ClickHouse writes, RowBinary encoders) and `uniqhll12.py` (the
reverse-engineered ClickHouse HLL codec). `spikes/` holds the investigations that produced the
format findings; `druid/` and `clickhouse/` hold fixture config.

Still undecided (do not assume): whether this ships as a library or a CLI — the scripts are
currently hard-coded to the fixture datasource. Verification comes in two layers: the pytest
suite in `tests/` (offline tests run anywhere; integration tests self-skip), and the `verify()`
function inside each migration, which needs both servers running.

## Local environment

Paths live in `.env` (gitignored — `.gitignore:151`). `.env.example` is the committed template;
keep the two in sync when adding a key.

| What | Where / version |
| --- | --- |
| Druid | `$DRUID_HOME` = `/Users/hellmarbecker/apache-druid-37.0.0` (v37.0.0) |
| Druid sketch extension | `$DRUID_HOME/extensions/druid-datasketches/druid-datasketches-37.0.0.jar` |
| **Sketch binary format producer** | `$DRUID_HOME/lib/datasketches-java-4.2.0.jar` (+ `datasketches-memory-2.2.0.jar`) |
| ClickHouse | `/Users/hellmarbecker/.local/bin/clickhouse`, v26.8.1.120 |
| Java | OpenJDK 21.0.7 (Homebrew) |

`datasketches-java` **4.2.0** wrote the sketch bytes in Druid; the Python `datasketches` 5.2.0
binding reads them **exactly** — `spikes/inspect_sketches.py` compares Python's estimate against
Druid's own `HLL_SKETCH_ESTIMATE`/`THETA_SKETCH_ESTIMATE` for the same sketch and they agree to
within 1e-6 on every column. Cross-language binary compatibility is settled; don't re-litigate it.

Use `clickhouse local` to probe ClickHouse functions and formats without running a server.

## Reading sketches out of Druid

Druid SQL returns a `COMPLEX<HLLSketch>` / `COMPLEX<thetaSketch>` column as **base64 inside a
quoted JSON string** — i.e. the JSON value is `"\"AgEHDAMIAwAE…\""`, so strip the inner quotes
before `base64.b64decode`. See `decode_sketch()` in the spike. `SELECT <sketch_col>` works
directly; `DS_HLL(col)` / `DS_THETA(col)` return the merged sketch for a group.

What the bytes tell you, measured on the fixture:

- **HLL parameters are recoverable.** Preamble byte 3 = `lgK`, byte 2 = family (7 = HLL), byte 7
  low 2 bits = current mode (LIST/SET/HLL), bits 2-3 = configured `tgtHllType`. The spike reads
  `lgK=12/HLL_4` and `lgK=14/HLL_8` straight from the two columns.
- **Theta's configured `size` is NOT recoverable.** Compact serialized Theta reports
  `lgNomLongs=0` for both the 16384 and the 4096 column — `size` is a build-time property that
  compaction discards. Get it from the ingestion spec or segment metadata, never from the bytes.
- **Stored rollup sketches are sparse, not dense.** Per-row sketches came back in LIST/SET mode
  (248–632 bytes); only the merged `DS_HLL` over the whole datasource was dense (2088 bytes,
  HLL_4, registers at offset 40). Sparse handling is the common case here, not an edge case.

## The core architectural constraint

Verified against the installed ClickHouse 26.8.1.120:

```
SELECT name FROM system.functions
WHERE name ILIKE '%theta%' OR name ILIKE '%hll%' OR name ILIKE '%sketch%'
-- uniqHLL12, uniqTheta, uniqThetaIntersect, uniqThetaNot, uniqThetaUnion
```

**Theta and HLL are not symmetric problems. Treat them as two separate workstreams.**

- **Theta → `uniqTheta` is viable, and the seeds match.** A `uniqTheta` aggregate state is a
  LEB128 length prefix followed by a standard **compact DataSketches Theta sketch** — same
  preamble Druid emits (`preLongs=2`, `serVer=3`, family 3/COMPACT, `flags=0x1a`). Decisively:
  the **seed hash is 37836 on both sides**, i.e. both use the DataSketches default seed (9001).
  Same seed means the same key hashes to the same value in both systems, so a transcoded sketch
  **unions correctly with a natively-built ClickHouse one** — real dedup, not double-counting.
  `spikes/inspect_sketches.py` asserts this at the end of its run; re-run it after any
  ClickHouse or Druid upgrade, because it is the assumption the whole Theta path rests on.
  **The write path is built and verified — see the section below.**

- **HLL has no DataSketches-compatible target.** `uniqHLL12` is ClickHouse's own HLL
  implementation, not Apache DataSketches HLL. Druid HLL bytes cannot be transcoded losslessly
  into it — see the strategy section below. Do not write code that implies HLL round-trips
  cleanly; surface the accuracy cost to the user.

**Prefer Theta wherever you have the choice.** The conversion asymmetry is permanent: Theta
retains the hash values below its threshold, so Theta → HLL is possible, but HLL keeps only a
max-rank per bucket and has discarded the hashes, so HLL → Theta is impossible. For any table
where raw data still exists or can be re-derived, rebuild the column as a Theta sketch and skip
the HLL problem entirely.

## Theta write path (working — `migrate_theta.py`)

Druid Theta → `AggregateFunction(uniqTheta, String)`, end-to-end with self-verification.
All checks pass on the fixture; re-run it after upgrading either system.

**The encoding.** A `uniqTheta` aggregate state in `RowBinary` is exactly
`LEB128(len(sketch)) + <compact DataSketches Theta bytes>` — nothing else, no wrapper. So the
whole transcode is `varint(len(b)) + b` over the base64-decoded Druid bytes; no sketch rebuild,
no re-hashing. Other `RowBinary` bits this depends on: `DateTime` = `<I` epoch seconds,
`Nullable(T)` = 1 flag byte then the value *only* when non-null, and
`SimpleAggregateFunction(sum, T)` is byte-identical to plain `T`.

**Verified properties** (from `verify()`):

- Per-row estimates match Druid **exactly** (302/505, 298/416, … on the five largest rows).
- Cross-system dedup is real, tested in exact mode through the production encoder: a transcoded
  sketch unioned with a natively-built ClickHouse sketch over the *same* 1000 values yields
  1000, and over a 50%-overlapping set yields 1500.
- Full-datasource merge drifts 0.14% (users) / 1.63% (pages) — see the k cap below.

**ClickHouse `uniqTheta` has a hard-wired nominal k=4096** and no way to configure it (measured:
retained entries settle around 4096–5724 and theta falls to 0.0116 at n=200k). Consequences:
Druid's `size=16384` columns keep full precision *at rest* but are downsampled to k≈4096 on any
cross-row merge, so expect ~1.6% RSE instead of ~0.8%. Building a Theta column in Druid with
`size` > 4096 buys nothing once the data lands in ClickHouse.

**Gotchas hit while building this, worth not rediscovering:**

- `AggregatingMergeTree` *rejects* non-aggregate measure columns outright — rollup measures must
  be `SimpleAggregateFunction(sum, …)`, else merges would silently keep an arbitrary row's value.
- Nullable dimensions in `ORDER BY` need `SETTINGS allow_nullable_key = 1`.
- Druid cannot `ORDER BY` a non-time column on a scan query ("requires ordering a table by
  non-time column"). Either `GROUP BY` first or sort elsewhere.
- This Druid runs with small merge buffers, so `GROUP BY` over 4 columns with two Theta aggs
  fails with `ResourceLimitExceededException`. Push grouping to ClickHouse where possible.
- Never assert "+N exactly" against a sketch in estimation mode: each new value moves the
  estimate by ~1/theta. Exactness checks are only valid below k.

## Reference docs

`docs/hll-sketch-formats.md` — both HLL binary formats field by field, a side-by-side comparison,
and the argument for why transplanted sketches merge with each other but not with native
ClickHouse ones. Read it before touching `uniqhll12.py` or either HLL migration.

`clickhouse/migrate-sketches-sqlonly.sql` — all three migrations expressed as ClickHouse SQL,
with no Python touching the sketch bytes. Every state it produces is byte-identical to the
Python migrations' (2486 rows, all columns, zero differences). The enabling trick is that
**`CAST(<String> AS AggregateFunction(...))` deserializes aggregate-state bytes** — undocumented
as a conversion, but it works for `uniqTheta` and `uniqHLL12` alike. One caveat lives in that
file's header: the HLL transplant is sparse-input-only and must be chunked, or it tries to
allocate 24 GiB.

**Querying Druid from ClickHouse SQL.** `url()` cannot — Druid's `/druid/v2/sql` is POST-only
and returns 405 for GET, while `url()` issues GET and has no method or body parameter.
`clickhouse/druid_query.py` closes the gap via `executable()`: it takes Druid SQL on stdin,
POSTs it, and returns `objectLines`, which is already valid `JSONEachRow`. That makes Druid
addressable from SQL — the whole Theta migration runs as one statement with no intermediate
file (verified byte-identical), and Druid can be joined live against a ClickHouse table.
Needs `user_scripts_path` (already set for the HLL UDF) and the executable bit.
`nginx/druid-get-to-post.conf` is the other working route: a stock-nginx shim
(`proxy_method POST` + `proxy_set_body`) that turns a GET into a Druid query, so plain `url()`
works with nothing deployed inside ClickHouse. Also verified byte-identical end to end. Pick
the shim when enabling script execution in ClickHouse is unattractive or several clients want
Druid over GET; pick `executable()` for ad-hoc SQL, which needs no URL-encoding or escaping.
Three traps are documented in that config: a regex `location` cannot carry a URI in
`proxy_pass` (use `rewrite … break`), nginx eats one level of backslash escaping so JSON needs
`\\"`, and ClickHouse globs `*` in URLs (use `COUNT(1)` or `%2A`).

`jdbc()` would work in principle — Druid bundles the Avatica driver — but needs the separate
clickhouse-jdbc-bridge daemon, and whether Avatica exposes `COMPLEX<thetaSketch>` usefully is
unverified.

## ClickHouse `uniqHLL12` state format (reverse-engineered)

Codec in `uniqhll12.py`; evidence and re-validation in `spikes/reverse_uniqhll12.py`. **This
format is undocumented and version-specific — re-run that spike after any ClickHouse upgrade
before trusting `migrate_hll_transplant.py`.** Derived against 26.8.1.120.

Dense ("large") form, a fixed **2651 bytes** at any cardinality:

| offset | size | meaning |
| --- | --- | --- |
| 0 | 1 | `is_large` flag, = 1 |
| 1 | 2560 | 4096 registers, **5 bits each**, LSB-first, register *k* at bits `[5k, 5k+5)` |
| 2561 | 88 | 22 × `UInt32` — histogram of register values, index = rank 0..21 |
| 2649 | 2 | `UInt16` — count of zero registers (duplicates `histogram[0]`) |

Sparse ("small") form, used up to 16 distinct values: `is_large=0`, one count byte, then
`count × UInt64` raw ClickHouse hashes. **Not writable from a DataSketches sketch** — it stores
ClickHouse's own hash values, which an HLL sketch has discarded. Always emit the dense form.

Registers are 5 bits and ranks cap at **21** because ClickHouse hashes to 32 bits and spends 12
on the bucket index, leaving a 20-bit tail. DataSketches ranks come from a 64-bit tail and can
exceed that, so they must be clamped — a value above 21 would overflow the histogram and corrupt
the state. Probability of a rank above 21 is ~2⁻²¹ per item, so the clamp is statistically
irrelevant but not optional.

**This build ignores the stored histogram on read** — measured with four states holding
identical registers but different histograms (correct, all-zero, "all empty", "all at max"),
which all return the same estimate. ClickHouse recomputes the denominator from the registers.
Still write a correct histogram: `encode_state()` derives it from the registers, because a
byte-exact round-trip against ClickHouse's own states is the regression gate for the format,
and the read path ignoring the field is unspecified behaviour rather than a contract.
Validation is that round-trip, at every cardinality from 17 to 1e6.

## HLL transplant path (working — `migrate_hll_transplant.py`)

Druid HLL → `AggregateFunction(uniqHLL12, String)`, queryable with plain `uniqHLL12Merge`.
Registers are folded to lgK=12 via `hll_union`, extracted from either the dense array at byte 40
or the sparse coupon list, then packed. A manual fold — max over registers sharing the low lgK
bits — is also valid here and is what the SQL-only path uses; DataSketches takes the rank from a
different hash lane than the slot, so it does not depend on the index bits. Prefer `hll_union`
in Python regardless: correct by construction, and representation-agnostic.

**Verified:**

- Native `uniqHLL12Merge` across all 2486 rows tracks Druid: 3.49% (users), 0.61% (pages).
  Per-channel merges land 0.25%–1.31%.
- Transplanted states **union correctly with each other**: A∪B = 1509 against a truth of 1500
  and a DataSketches union of 1508, and re-adding A leaves it unchanged.

**The rule this path lives by: never merge a transplanted state with a natively-built ClickHouse
one.** Measured: the same 1000 values give transplanted=989, native=1001, merged=**2022**. The
same key lands in different buckets, so the union double-counts silently. Practically, nothing
may ever write these columns with `uniqHLL12State()` — one native insert corrupts every
historical number in the column. `verify()` demonstrates the breakage rather than just asserting
the rule.

**Choosing between this and `migrate_hll.py`:** transplant gives native in-engine merging and no
external process, but estimates drift ~1–3% from source because ClickHouse applies its own bias
correction to the same registers. The pre-merge + UDF path reproduces source values exactly and
is mergeable with anything, but needs a UDF process and pre-materialised grains. Also note the
lgK=14 `pages` column is folded to 12 here, halving its precision (~0.81% → ~1.63% RSE);
`migrate_hll.py` preserves lgK=14.

## Quantiles path (working — `migrate_quantiles.py`)

Druid `quantilesDoublesSketch` / `KllDoublesSketch` → carried bytes **and** a native
`AggregateFunction(quantileTDigestWeighted, Float64, UInt64)`.

ClickHouse has no DataSketches quantiles or KLL, so there is no byte-level transcode — the
same starting point as HLL. **But quantiles are the easy case**, because of what the sketch
keeps: actual data values, where HLL keeps only a max rank per bucket. Values can be replayed;
hashes cannot.

**The consequence is that the HLL transplant's hard rule does not apply here.** A state built
from replayed values is a genuine native t-digest, so it merges correctly with states built
from raw data — verified in `verify()`. No "never mix with native states" constraint.

**How.** Sample the sketch's quantile function at up to 1000 evenly spaced ranks
(`(i+0.5)/m`), giving `(value, weight)` pairs whose weights sum to exactly `n`, then let
**ClickHouse** build the t-digest from them via `quantileTDigestWeightedState`. Building the
state in SQL rather than constructing its bytes keeps it a real native state and avoids
reverse-engineering a second undocumented format.

**Below the sample cap the replay is exact** — it returns the original observations. At the
fixture's grain (~16 values per rollup row, against k=256) that means the migration is
carrying the actual data, not an approximation.

**Accuracy, against exact quantiles computed from the raw wikiticker file:**

| level | exact | Druid k=256 | this migration |
| --- | --- | --- | --- |
| p50 | 18.0 | 18.0 | 18.0 |
| p90 | 356.0 | 343.0 (−3.7%) | 355.3 (−0.2%) |
| p99 | 3813.0 | 3191.0 (−16.3%) | 3793.2 (−0.5%) |

The migration is *more accurate than the source it reads*, because Druid's number comes from
merging 2486 k=256 sketches and that is where the tail resolution goes. **So Druid's own
answer is not ground truth for this path** — an earlier `verify()` compared against it by
percentage and failed the better answer.

**Two traps worth not rediscovering:**

- **Do not pin `get_min_value()`/`get_max_value()` as extra samples.** It looks like a
  safeguard for the untouched outer slices, but the outermost samples already stand for them,
  so it injects mass rather than replacing it. Measured: merged p99 went 3793 → 5487.
- **Compare quantiles in rank space, never by value.** On tied data a quantile is an interval
  of ranks, not a point: the fixture puts 15% of its mass on the single value 18.0, giving it
  the interval [0.3951, 0.5444]. DataSketches returns 32.0 there and t-digest returns 18.0;
  both are correct, and the exact median is 18.0. `valid_quantile()` implements the textbook
  test `P(X<v) <= L <= P(X<=v)`; note `get_rank(v)` is the exclusive side, so the inclusive
  side needs `math.nextafter`.

**KLL is carried but not replayed.** Druid 37 has no SQL aggregator for KLL — `DS_KLL_SKETCH`
and friends do not exist, and passing a KLL column to `DS_QUANTILES_SKETCH` throws a
`ClassCastException`. It is readable as a raw column and via the native ingestion spec, so
the bytes are carried losslessly, but `druid/rollup-sketches-index.sql` cannot reproduce that
column at all.

## HLL migration strategy

Verified sketch layouts (all reproducible with the probes below):

| Sketch | Dense size | Structure |
| --- | --- | --- |
| DataSketches HLL_8, lgK=12 | 4136 B | 40 B preamble + **4096 one-byte registers** |
| DataSketches HLL_6, lgK=12 | 3113 B | 40 B preamble + 4096×6 bits |
| DataSketches HLL_4, lgK=12 | 2216 B | 40 B preamble + 4096×4 bits + 128 B exceptions table |
| ClickHouse `uniqHLL12` | 2651 B | 2 B varint length + 1 B large-flag + **2648 B opaque packed** |

DataSketches preamble bytes: `serVer=1`, `familyId=7`, byte 3 = `lgK`, last header byte encodes
mode + target type. Below roughly 100–200 distinct values the sketch is in **LIST/SET (coupon)
mode**, not dense (12/52/412 B at n=1/10/100) — handle sparse sources explicitly. Coupons are
hashed slot+rank pairs, so **there is no path back to the original keys**: re-hashing into a
native ClickHouse sketch is impossible from a rollup table. `uniqHLL12` keeps raw 64-bit hashes
in a small-set mode up to 16 values, then switches to its dense encoding.

Ranked options, highest fidelity first among those that are actually buildable. **Options 1 and 2
are built and verified in `migrate_hll.py` — see the section after this one.**

1. **Pre-merge in Python, land estimates (recommended default).** Union the Druid sketches with
   `hll_union` at every granularity the warehouse needs, then write `get_estimate()` plus
   `get_lower_bound(n)`/`get_upper_bound(n)` into plain `Float64`/`UInt64` columns. Every number
   is exactly what Druid would have reported at that grain (~1.6% RSE at lgK=12). The loss is
   *query-time* mergeability only, and pre-merging is what buys that back — never sum estimates
   across rows to fake a union.
2. **Carry the sketch bytes verbatim as `String`, merge outside the engine.** `serialize_compact()`
   into a ClickHouse column: lossless at rest, and preserves the option to re-roll later.
   ClickHouse has no user-defined *aggregate* functions, so merging is
   `groupArray(sketch)` → executable UDF that unions via DataSketches. Slower and memory-hungry
   for wide groups, but correct. Good as a companion to option 1 rather than a replacement.
3. **Register transplant into `uniqHLL12` — no longer blocked; built in
   `migrate_hll_transplant.py`.** The state format was reverse-engineered (see below). HLL's
   estimator is symmetric in its buckets, so copying a register array preserves the cardinality
   estimate even though the hash functions differ. Gives native `uniqHLL12Merge` in plain SQL
   with no UDF, at the cost of ~1–3% estimate drift and one hard rule: transplanted states must
   never be merged with natively-built ones.
4. **Refilling a `uniqHLL12` state with synthetic keys is an anti-pattern.** It yields a state
   that estimates ≈N and merges *mechanically*, but synthetic keys never collide, so unions
   return `N1+N2` and silently lose all overlap detection. Only defensible when the grains are
   disjoint by construction — in which case summing estimates (option 1) is simpler and honest.

Reproduce the layout probes:

```bash
.venv/bin/python -c "
from datasketches import hll_sketch, tgt_hll_type
s = hll_sketch(12, tgt_hll_type.HLL_8)
for i in range(100000): s.update(i)
print(len(s.serialize_compact()), s.get_estimate())"

clickhouse local --query "SELECT length(toString(uniqHLL12State(number))) FROM numbers(100000)"
```

Or against real Druid data: `.venv/bin/python spikes/inspect_sketches.py`.

Per-column `lgK` and target type vary by column — read them from the sketch preamble rather than
assuming Druid's defaults (the fixture has two different settings precisely to catch that).

## HLL path (working — `migrate_hll.py`)

Implements options 1 + 2 together: pre-merged estimates *and* verbatim sketch bytes, plus an
executable UDF so ClickHouse can union the carried bytes at query time. All checks pass.

**Target shape.** One table, `wikipedia_rollup_hll`, tagged by a `grain` column, materialising
`hour_dims` (2486 rows) → `hour_channel` (909) → `day_channel` (51) → `day_total` (1). Each row
carries `<col>_est`, `<col>_lb2`, `<col>_ub2` and `<col>_sketch` (raw bytes in a `String`).
Dimensions are `Nullable`: NULL means "this grain aggregated it away", and `grain` is what
disambiguates that from a genuine Druid NULL at the `hour_dims` grain.

At the `hour_dims` grain the original Druid bytes are carried through untouched — no union, no
re-serialisation — so byte fidelity is provable. Coarser grains store the unioned sketch.

**Verified properties:**

- Pre-merged estimates agree with Druid's own merge at matching lgK, within the sketch's 2σ
  bounds at every grain checked (deltas 0.02%–0.10%).
- Carried bytes round-trip: re-read from the `String` column, `hll_sketch.deserialize()` gives
  back the stored estimate to 1e-9.
- **Query-time UDF merge is bit-identical to the pre-merged value**: unioning all 2486
  `hour_dims` sketches through `hllMergeEstimate` returns exactly `10564.3385` / `35292.473`,
  the same as the `day_total` row. Mergeability is genuinely restored.
- Summing estimates instead of unioning sketches inflates the daily total 1.6x
  (17193 vs 10564) — the trap this design exists to avoid, asserted in `verify()`.

**`hllMergeEstimate` UDF.** `clickhouse/hll_merge_udf.py` + `hll_merge_function.xml`, wired via
`user_scripts_path` / `user_defined_executable_functions_config` in `clickhouse/config.xml`.
Adding or changing a function config needs a **server restart**; the tables survive it.

```sql
SELECT hllMergeEstimate(arrayStringConcat(groupArray(base64Encode(users_sketch)), ','))
FROM wikipedia_rollup_hll WHERE grain = 'hour_dims'
```

Uses `execute_direct=0` so the venv interpreter is named explicitly (no shebang/chmod reliance).
Protocol is TabSeparated, one output line per input line — base64 contains no comma or tab, so
comma-joining is safe. The UDF must never crash: an exception aborts the whole query, so it
catches and emits `-1`. A whole-table merge is one ~800 KB line and works fine, but this is
memory-hungry on wide groups by construction.

**A real trap worth knowing: Druid's `APPROX_COUNT_DISTINCT_DS_HLL` merges at lgK=12 by default,
ignoring the column's actual lgK.** On the `pages` column (lgK=14) the default gives 35610 while
an explicit `APPROX_COUNT_DISTINCT_DS_HLL(col, 14)` gives 35285. So Druid's default query is
*less* accurate than the stored data allows, and validating a faithful migration against it makes
the migration look broken. Always pass the column's real lgK when comparing; `verify()` prints
both to make the gap visible.

Note also that Python-merged and Druid-merged results are **not bit-identical** even at matching
lgK (~0.02% residual, from union internals and Druid rounding to an integer). Assert against the
sketch's own 2σ bounds, which self-calibrate per row, rather than a hand-picked percentage.

## Working notes

- Druid rollup semantics matter: sketch columns exist precisely because raw rows were discarded
  at ingestion. Any design that quietly falls back to "re-read the raw data" is not a migration
  of these tables.
- When reading sketches out of Druid, prefer a path that yields the raw serialized sketch
  (e.g. base64 via native/SQL query) over one that returns only the estimate — the estimate
  discards the mergeability that makes a rollup table useful.
- Verify sketch claims empirically against the installed versions rather than from memory; the
  binary formats are version-sensitive and both products move fast.
