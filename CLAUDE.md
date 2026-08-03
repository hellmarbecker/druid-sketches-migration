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

No test runner or linter is chosen yet, so there is no test/lint command to run. Ask before
picking one.

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

.venv/bin/python migrate_theta.py                        # migrate + self-verify
```

`migrate_theta.py` recreates the target table each run, so it is safe to re-run.

`druid/rollup-sketches-index.json` builds `wikipedia_rollup_sketches` from the bundled wikiticker
sample: HOUR rollup, 3 dimensions, 39,244 events → 2,486 rows (15.8x). It carries **four** sketch
columns with deliberately different parameters (`lgK` 12/14, `HLL_4`/`HLL_8`, theta `size`
16384/4096) so code cannot get away with assuming defaults.

## Current state

Early. `druid/` holds the fixture ingestion spec, `spikes/` holds throwaway investigation
scripts — no adapter code yet, and no tests. Still undecided (do not assume): whether this ships
as a library, a CLI, or scripts.

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
3. **Register transplant into `uniqHLL12` — blocked, do not start here.** HLL's estimator is
   symmetric in its buckets, so copying a register array across implementations would preserve
   the *cardinality estimate* even though the hash functions differ (Murmur3-128 vs ClickHouse's
   own). Three problems: the 2648-byte ClickHouse packing is undocumented and was not decoded by
   the probes above (reverse-engineering it means reading `HyperLogLogCounter.h` /
   `AggregateFunctionUniq.h`); aggregate-state bytes are not a stable cross-version contract; and
   because the same key hashes to different buckets, a transplanted sketch **cannot be unioned
   with a natively-built ClickHouse sketch** without double-counting. Normalize to HLL_8 first
   via `hll_union.get_result(tgt_hll_type.HLL_8)` (registers then start at byte 40) if pursued.
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
