-- SQL-based (MSQ) equivalent of druid/rollup-sketches-index.json, kept for reference.
--
-- Verified: this was actually run against Druid 37.0.0 and produces a datasource equivalent
-- to the JSON spec -- same 2486 rows from 39244 events, same column types
-- (COMPLEX<HLLSketch> / COMPLEX<thetaSketch>), same dimension values, and byte-identical
-- Theta sketches. See "Verified equivalence" at the bottom for the one difference and why
-- it is expected.
--
-- It writes wikipedia_rollup_sketches_sql so it cannot clobber the fixture. Change the
-- REPLACE INTO target to wikipedia_rollup_sketches to rebuild the fixture itself.
--
--
-- HOW THE JSON SPEC MAPS ONTO SQL
--
--   dataSchema.dataSource               -> REPLACE INTO <name>
--   granularitySpec.intervals           -> OVERWRITE WHERE __time >= ... AND __time < ...
--   granularitySpec.segmentGranularity  -> PARTITIONED BY DAY
--   granularitySpec.queryGranularity    -> TIME_FLOOR(..., 'PT1H') in SELECT and GROUP BY
--   granularitySpec.rollup: true        -> the GROUP BY itself
--   dimensionsSpec.dimensions           -> the grouped-by columns
--   metricsSpec                         -> the aggregate expressions
--   ioConfig.inputSource / inputFormat  -> EXTERN(...) with an EXTEND(...) schema
--   tuningConfig.partitionsSpec         -> no equivalent; MSQ decides, tune via CLUSTERED BY
--
--
-- HOW TO SUBMIT
--
-- SQL ingestion goes to /druid/v2/sql/task, not /druid/v2/sql (that endpoint runs queries
-- and will reject REPLACE). The request body is JSON, so wrap this file rather than pasting
-- it -- and write the payload to a file instead of piping through `echo`, which in zsh
-- re-interprets the \n escapes in the JSON string and corrupts it:
--
--   .venv/bin/python -c "
--   import json
--   open('/tmp/msq.json','w').write(json.dumps({
--       'query': open('druid/rollup-sketches-index.sql').read(),
--       'context': {'finalizeAggregations': False, 'maxNumTasks': 2}}))
--   "
--   curl -X POST -H 'Content-Type: application/json' --data-binary @/tmp/msq.json \
--     http://localhost:8888/druid/v2/sql/task
--   # then poll /druid/indexer/v1/task/<taskId>/status until SUCCESS
--
-- MSQ is bundled in $DRUID_HOME/lib (druid-multi-stage-query-37.0.0.jar) and loads from the
-- classpath, so it works without being added to druid.extensions.loadList.
--
--
-- TWO CONTEXT SETTINGS THAT MATTER
--
-- "finalizeAggregations": false
--   Mandatory here. Without it MSQ finalises the aggregators and every sketch column lands
--   as a plain number (the estimate) instead of a sketch -- silently destroying exactly what
--   this migration exists to move. The column type check is how you catch it: the columns
--   must report COMPLEX<HLLSketch> / COMPLEX<thetaSketch> in INFORMATION_SCHEMA.COLUMNS.
--
-- "maxNumTasks": 2
--   Must be <= the middleManager's druid.worker.capacity, which is 2 in this fixture. The
--   count includes the controller, so maxNumTasks=3 on a capacity-2 cluster deadlocks: the
--   controller and worker0 take both slots and worker1 stays pending forever, with the task
--   reporting RUNNING indefinitely and no error. Diagnose via
--   /druid/indexer/v1/pendingTasks and /druid/indexer/v1/workers.

REPLACE INTO "wikipedia_rollup_sketches_sql"
OVERWRITE WHERE __time >= TIMESTAMP '2015-09-12' AND __time < TIMESTAMP '2015-09-13'
SELECT
    -- queryGranularity: HOUR
    TIME_FLOOR(TIME_PARSE("time"), 'PT1H')  AS __time,

    -- dimensionsSpec.dimensions
    "channel",
    "countryName",
    "isRobot",

    -- metricsSpec. "count" and "user" are reserved words, hence the quoting.
    COUNT(*)                                AS "count",
    SUM("added")                            AS "sum_added",
    DS_HLL("user", 12, 'HLL_4')             AS "users_hll_k12_hll4",
    DS_HLL("page", 14, 'HLL_8')             AS "pages_hll_k14_hll8",
    DS_THETA("user", 16384)                 AS "users_theta_16384",
    DS_THETA("page", 4096)                  AS "pages_theta_4096"

FROM TABLE(
    EXTERN(
        '{"type":"local","baseDir":"/Users/hellmarbecker/apache-druid-37.0.0/quickstart/tutorial","filter":"wikiticker-2015-09-12-sampled.json.gz"}',
        '{"type":"json"}'
    )
) EXTEND (
    -- Only the columns this ingestion actually reads need declaring.
    -- isRobot is a JSON boolean in the source but is declared VARCHAR so the stored column
    -- holds the strings 'true'/'false', matching what the JSON spec's dimensionsSpec yields.
    "time"        VARCHAR,
    "channel"     VARCHAR,
    "countryName" VARCHAR,
    "isRobot"     VARCHAR,
    "user"        VARCHAR,
    "page"        VARCHAR,
    "added"       BIGINT
)
GROUP BY 1, 2, 3, 4
PARTITIONED BY DAY

-- VERIFIED EQUIVALENCE (JSON spec vs this SQL, measured on Druid 37.0.0)
--
--   rows / events            2486 / 39244            identical
--   column types             all four COMPLEX<...>   identical
--   isRobot values           'true' / 'false'        identical
--   NULL countryName rows    1254                    identical
--   distinct channels        51                      identical
--   Theta estimates          10531 / 35173           identical, and byte-identical sketches
--   HLL estimates            10562 vs 10561          differ by 1
--
-- The HLL difference is expected and is not a defect in the translation. Comparing the
-- merged HLL sketches byte by byte: preamble bytes 0-7 match, the register payload matches
-- EXACTLY, and only bytes 8-39 differ -- the HIP accumulator and KxQ sums. DataSketches'
-- HIP estimator is accumulated incrementally and is therefore order-dependent, and the two
-- ingestion paths aggregate rows in a different order. The sketches hold identical
-- information; only the order-dependent estimator bookkeeping differs, worth ~1.4 on an
-- estimate of ~10562. Theta has no such accumulator, which is why it is byte-identical.
-- See docs/hll-sketch-formats.md, section 1 ("Estimator").
