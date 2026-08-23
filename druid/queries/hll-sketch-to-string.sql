-- Inspecting the rollup table's HLL sketches with HLL_SKETCH_TO_STRING.
--
-- HLL_SKETCH_TO_STRING returns DataSketches' own human-readable summary of a sketch: its
-- lgK, target type, current representation, estimate and error bounds. It is the quickest
-- way to see what is actually inside a COMPLEX<HLLSketch> column without decoding bytes,
-- and it independently corroborates the layout described in docs/hll-sketch-formats.md.
--
-- Verified against Druid 37.0.0 on the fixture datasource; the outputs below are real.
--
-- Submit like any Druid SQL query:
--
--   curl -s -X POST -H 'Content-Type: application/json' \
--     -d '{"query":"<one query from below>"}' \
--     http://localhost:8888/druid/v2/sql
--
--
-- HOW IT SLOTS IN: it takes the place of the estimator, so it runs in conjunction with the
-- aggregator. Anywhere HLL_SKETCH_ESTIMATE(DS_HLL(col)) would go, HLL_SKETCH_TO_STRING(...)
-- goes instead -- it is a post-aggregation over an aggregator's output, not a function of a
-- stored column. So this does not work:
--
--   SELECT HLL_SKETCH_TO_STRING(users_hll_k12_hll4) FROM wikipedia_rollup_sketches LIMIT 1
--   -- Unhandled Query Planning Failure
--
-- Wrap the column in DS_HLL() instead. To inspect one stored row, wrap it and filter down to
-- that row: aggregating a single row returns that row's sketch unchanged.


-- ============================================================ 1. A single row, LIST mode
-- Rollup rows with only a handful of distinct values stay in LIST mode: a short coupon
-- array rather than a register array. Coupon Count is the number of distinct values seen.

SELECT HLL_SKETCH_TO_STRING(DS_HLL(users_hll_k12_hll4, 12)) AS sketch
FROM wikipedia_rollup_sketches
WHERE channel = '#vi.wikipedia'
  AND isRobot = 'true'
  AND countryName IS NULL
  AND __time = MILLIS_TO_TIMESTAMP(1442016000000);

--   ### HLL SKETCH SUMMARY:
--     Log Config K   : 12
--     Hll Target     : HLL_4
--     Current Mode   : LIST
--     Memory         : false
--     LB             : 3.0
--     Estimate       : 3.000000014901161
--     UB             : 3.0001498026537594
--     OutOfOrder Flag: false
--     Coupon Count   : 3


-- ============================================================= 2. A single row, SET mode
-- Busier rows promote from LIST to SET -- still coupons, just a larger hash table. Note the
-- estimate is effectively exact here: below the dense threshold nothing has been
-- approximated away yet.

SELECT HLL_SKETCH_TO_STRING(DS_HLL(users_hll_k12_hll4, 12)) AS sketch
FROM wikipedia_rollup_sketches
WHERE channel = '#en.wikipedia'
  AND isRobot = 'false'
  AND countryName IS NULL
  AND __time = MILLIS_TO_TIMESTAMP(1442084400000);

--   ### HLL SKETCH SUMMARY:
--     Log Config K   : 12
--     Hll Target     : HLL_4
--     Current Mode   : SET
--     Memory         : false
--     LB             : 302.0
--     Estimate       : 302.000225757753
--     UB             : 302.0153044027116
--     OutOfOrder Flag: false
--     Coupon Count   : 302
--
-- Every stored per-row sketch in this fixture is LIST or SET, never dense. That is typical
-- of a rollup table and is why the SQL-only transplant in
-- clickhouse/migrate-sketches-sqlonly.sql only parses coupons.


-- ================================================== 3. Merged across rows, dense HLL mode
-- Merging enough rows tips the sketch into HLL mode, and the summary changes shape: the
-- coupon count is replaced by register bookkeeping (CurMin, NumAtCurMin) and the HIP
-- estimator's accumulators (HipAccum, KxQ0, KxQ1).

SELECT HLL_SKETCH_TO_STRING(DS_HLL(pages_hll_k14_hll8, 14)) AS sketch
FROM wikipedia_rollup_sketches;

--   ### HLL SKETCH SUMMARY:
--     Log Config K   : 14
--     Hll Target     : HLL_4
--     Current Mode   : HLL
--     Memory         : false
--     LB             : 35056.56506037928
--     Estimate       : 35284.584629850906
--     UB             : 35515.58984935926
--     OutOfOrder Flag: false
--     CurMin         : 0
--     NumAtCurMin    : 1865
--     HipAccum       : 35284.584629850906
--     KxQ0           : 5230.803955078125
--     KxQ1           : 0.0
--     Rebuild KxQ Flg: false


-- ============================================ 4. Why the DS_HLL arguments are not optional
-- DS_HLL(expr) defaults to lgK=12 and HLL_4 regardless of how the column was built, so the
-- default silently downsamples the lgK=14 `pages` column. HLL_SKETCH_TO_STRING makes that
-- visible in a way a bare estimate does not: compare the Log Config K lines.

SELECT HLL_SKETCH_TO_STRING(DS_HLL(pages_hll_k14_hll8))                  AS defaulted,
       HLL_SKETCH_TO_STRING(DS_HLL(pages_hll_k14_hll8, 14, 'HLL_8'))     AS explicit
FROM wikipedia_rollup_sketches;

--   defaulted -> Log Config K : 12   Hll Target : HLL_4   Estimate : 35610.486919460425
--   explicit  -> Log Config K : 14   Hll Target : HLL_8   Estimate : 35284.584629850906
--
-- Same stored data, ~326 apart, because the default merged at half the configured precision.
-- This is the trap documented in CLAUDE.md: validating a faithful migration against Druid's
-- default merge makes the migration look wrong when it is the baseline that is coarser.
-- Always pass the column's real lgK, which is readable from preamble byte 3.
--
-- The `Hll Target` line also shows the third argument at work. It controls the *result*
-- sketch's encoding, not the source's: query 3 reports HLL_4 while reading an HLL_8 column,
-- because DS_HLL defaulted the target type.
