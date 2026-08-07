-- Migrating Druid sketches with ClickHouse SQL only.
--
-- All three migration paths expressed as ClickHouse SQL, with no Python touching the sketch
-- bytes. Verified against the Python migrations on the fixture: every state produced here is
-- BYTE-IDENTICAL to the one migrate_theta.py / migrate_hll.py / migrate_hll_transplant.py
-- produce for the same row (2486 rows, both HLL columns, zero differences).
--
-- Reference material. The Python migrations remain the supported path; this exists to show
-- what the engine can do unaided, and because a SQL-only route avoids deploying anything.
--
--
-- WHAT DOES NOT WORK: reading Druid from ClickHouse
--
-- `url()` cannot drive Druid. Druid's /druid/v2/sql is POST-with-a-JSON-body only (GET
-- returns 405), and url() issues GET for SELECT -- its signature is
-- (uri, format, structure, compression) plus headers, with no method or body. Adding
-- Content-Type headers does not change the verb. Confirmed both ways: url() against
-- /status/health returns `true`, url() against /druid/v2/sql returns 405.
--
-- So one non-SQL transport step is unavoidable. Anything works; the cheapest is to land
-- Druid's own output where ClickHouse can see it:
--
--   curl -s -X POST -H 'Content-Type: application/json' \
--     -d '{"query":"SELECT __time, channel, countryName, isRobot, \"count\", sum_added,
--                    users_theta_16384, pages_theta_4096
--                   FROM wikipedia_rollup_sketches","resultFormat":"objectLines"}' \
--     http://localhost:8888/druid/v2/sql > <user_files_path>/druid_theta.jsonl
--
-- Everything after that point is SQL. Alternatives that keep it SQL-only end to end: a
-- GET->POST proxy in front of Druid, or Druid MSQ exporting to a location ClickHouse reads.
--
--
-- THE ONE FUNCTION THAT MAKES THIS POSSIBLE
--
-- CAST(<String> AS AggregateFunction(...)) deserializes serialized aggregate-state bytes.
-- It is not documented as a conversion, but it works for both uniqTheta and uniqHLL12 and
-- round-trips toString(state) exactly. Everything below is about producing the right bytes
-- to hand it.
--
-- Druid emits complex columns as base64 inside a *quoted* JSON string, so the inner quotes
-- come off first: base64Decode(trim(BOTH '"' FROM col)).


-- ============================================================================ helpers
-- LEB128 varint, for the length prefix uniqTheta states carry. Covers up to 2^21, which is
-- more than any DataSketches Theta sketch. Verified against ClickHouse's own prefixes
-- (8016 -> D03E, 16384 -> 808001).
--
--   (L -> multiIf(L < 128,   char(L),
--                 L < 16384, char(bitOr(bitAnd(L,127),128), bitShiftRight(L,7)),
--                            char(bitOr(bitAnd(L,127),128),
--                                 bitOr(bitAnd(bitShiftRight(L,7),127),128),
--                                 bitShiftRight(L,14)))) AS leb128
--
-- Fixed-width little-endian ints must be built from hex(). reinterpretAsString() looks like
-- the right tool and is not: it strips trailing zero bytes, so toUInt32(1) yields one byte.
--
--   (v -> concat(hex(toUInt8(bitAnd(v,255))), hex(toUInt8(bitAnd(bitShiftRight(v,8),255))),
--                hex(toUInt8(bitAnd(bitShiftRight(v,16),255))),
--                hex(toUInt8(bitAnd(bitShiftRight(v,24),255))))) AS le32
--   (v -> concat(hex(toUInt8(bitAnd(v,255))),
--                hex(toUInt8(bitAnd(bitShiftRight(v,8),255))))) AS le16


-- ================================================================= 1. Theta -> uniqTheta
-- Lossless and cheap: a uniqTheta state is LEB128(len) + the compact DataSketches sketch
-- Druid already stores. One INSERT, no chunking, byte-identical to migrate_theta.py.

CREATE TABLE IF NOT EXISTS rollup_theta_sqlonly (
    ts          DateTime,
    channel     String,
    countryName Nullable(String),
    isRobot     String,
    cnt         SimpleAggregateFunction(sum, UInt64),
    sum_added   SimpleAggregateFunction(sum, Int64),
    users_theta AggregateFunction(uniqTheta, String),
    pages_theta AggregateFunction(uniqTheta, String)
) ENGINE = AggregatingMergeTree
ORDER BY (channel, countryName, isRobot, ts)
SETTINGS allow_nullable_key = 1;

INSERT INTO rollup_theta_sqlonly
WITH
  (L -> multiIf(L < 128,   char(L),
                L < 16384, char(bitOr(bitAnd(L,127),128), bitShiftRight(L,7)),
                           char(bitOr(bitAnd(L,127),128),
                                bitOr(bitAnd(bitShiftRight(L,7),127),128),
                                bitShiftRight(L,14)))) AS leb128,
  (b -> base64Decode(trim(BOTH '"' FROM b)))            AS unwrap,
  (s -> CAST(concat(leb128(length(s)), s) AS AggregateFunction(uniqTheta, String))) AS to_state
SELECT
    parseDateTimeBestEffort(__time) AS ts,
    channel, countryName, isRobot,
    `count`                         AS cnt,
    sum_added,
    to_state(unwrap(users_theta_16384)),
    to_state(unwrap(pages_theta_4096))
FROM file('druid_theta.jsonl', 'JSONEachRow',
          '__time String, channel String, countryName Nullable(String), isRobot String,
           `count` UInt64, sum_added Int64,
           users_theta_16384 String, pages_theta_4096 String');


-- ========================================================= 2. HLL -> carried sketch bytes
-- The option-2 path of migrate_hll.py. Trivial in SQL because it involves no transformation
-- at all: ClickHouse just stores the DataSketches bytes. Merge them later with the
-- hllMergeEstimate UDF (clickhouse/hll_merge_udf.py); ClickHouse cannot union them natively.

CREATE TABLE IF NOT EXISTS rollup_hll_bytes_sqlonly (
    ts DateTime, channel String, countryName Nullable(String), isRobot String,
    users_sketch String, pages_sketch String
) ENGINE = MergeTree ORDER BY (channel, ts) SETTINGS allow_nullable_key = 1;

INSERT INTO rollup_hll_bytes_sqlonly
SELECT parseDateTimeBestEffort(__time), channel, countryName, isRobot,
       base64Decode(trim(BOTH '"' FROM users_hll_k12_hll4)),
       base64Decode(trim(BOTH '"' FROM pages_hll_k14_hll8))
FROM file('druid_hll.jsonl', 'JSONEachRow',
          '__time String, channel String, countryName Nullable(String), isRobot String,
           users_hll_k12_hll4 String, pages_hll_k14_hll8 String');


-- ============================================== 3. HLL register transplant -> uniqHLL12
-- Builds a native 2651-byte uniqHLL12 state in SQL. See docs/hll-sketch-formats.md for the
-- format and uniqhll12.py for the Python equivalent.
--
-- READ THE TWO CONSTRAINTS BELOW BEFORE USING THIS.
--
-- (a) SPARSE INPUT ONLY. This reads the DataSketches coupon list. Fed a dense sketch it
--     computes pre=40 from preInts=10, treats the 4096-byte register array as 1024
--     "coupons", and produces a structurally valid state holding garbage -- no error, no
--     warning. Every stored sketch in the fixture is sparse (LIST/SET), which is typical of
--     a rollup table, but that is a property of the data, not a guarantee. RUN THE GUARD.
--     Supporting dense would need three more branches, and HLL_4's curMin-plus-exceptions
--     encoding is where this stops being reasonable to express in SQL.
--
-- (b) MUST BE CHUNKED. `arrayMap(b -> ... regs[...], range(2560))` makes ClickHouse
--     broadcast the 4096-element register array once per lambda iteration: 2486 rows at once
--     tries to allocate 24.28 GiB and is killed. max_block_size does not help, because the
--     nested subqueries do not re-block. Splitting into 26 chunks of ~96 rows runs in ~8 s
--     per column. Adjust the modulus to taste; smaller is safer.
--
-- Why the lgK=14 column needs no separate fold step: for a sparse sketch the register index
-- is `coupon & (2^lgK - 1)`, so masking the coupon with 4095 folds 14 -> 12 for free. That
-- is sound because DataSketches takes the rank from a different 64-bit hash lane than the
-- slot, leaving it independent of the index bits.

-- ---- GUARD: run this first. Every count must be 0, or the transplant below is invalid. ----
SELECT
    countIf(bitAnd(reinterpretAsUInt8(substring(users_sketch, 8, 1)), 3) = 2) AS users_dense,
    countIf(bitAnd(reinterpretAsUInt8(substring(pages_sketch, 8, 1)), 3) = 2) AS pages_dense,
    countIf((length(users_sketch)
             - bitAnd(reinterpretAsUInt8(substring(users_sketch, 1, 1)), 63) * 4) % 4 != 0)
                                                                             AS users_bad_len,
    countIf((length(pages_sketch)
             - bitAnd(reinterpretAsUInt8(substring(pages_sketch, 1, 1)), 63) * 4) % 4 != 0)
                                                                             AS pages_bad_len
FROM rollup_hll_bytes_sqlonly;

CREATE TABLE IF NOT EXISTS _sqlonly_users (
    ts DateTime, channel String, countryName Nullable(String), isRobot String,
    s AggregateFunction(uniqHLL12, String)
) ENGINE = MergeTree ORDER BY (channel, ts) SETTINGS allow_nullable_key = 1;

-- Run once per chunk, k = 0 .. 25. Substitute the source column and target table to build
-- the pages side into _sqlonly_pages the same way.
INSERT INTO _sqlonly_users
WITH
  (v -> concat(hex(toUInt8(bitAnd(v,255))), hex(toUInt8(bitAnd(bitShiftRight(v,8),255))),
               hex(toUInt8(bitAnd(bitShiftRight(v,16),255))),
               hex(toUInt8(bitAnd(bitShiftRight(v,24),255))))) AS le32,
  (v -> concat(hex(toUInt8(bitAnd(v,255))),
               hex(toUInt8(bitAnd(bitShiftRight(v,8),255)))))  AS le16
SELECT
    ts, channel, countryName, isRobot,
    CAST(
      unhex(concat(
        -- is_large flag
        '01',
        -- 4096 registers at 5 bits each, LSB-first. 5 and 8 interleave, so an output byte
        -- draws on up to three registers; out-of-range indices return 0, which is correct.
        arrayStringConcat(arrayMap(b -> hex(toUInt8(bitAnd(
            bitOr(bitOr(
              bitShiftRight(regs[intDiv(b*8,5)+1],      b*8 - 5*intDiv(b*8,5)),
              bitShiftLeft( regs[intDiv(b*8,5)+2],  5 - (b*8 - 5*intDiv(b*8,5)))),
              bitShiftLeft( regs[intDiv(b*8,5)+3], 10 - (b*8 - 5*intDiv(b*8,5)))), 255))),
          range(2560))),
        -- 22 x UInt32 histogram of register values. Counted over the sparse value list
        -- rather than the dense array: hist[0] is just 4096 minus the number of set
        -- registers. (ClickHouse recomputes its denominator from the registers and ignores
        -- this field on read, but a correct one is needed for byte-exact round-trip.)
        arrayStringConcat(arrayMap(r -> le32(if(r = 0, 4096 - length(mmk),
                                                arrayCount(x -> x = r, mmv))), range(22))),
        -- UInt16 zero-register count
        le16(4096 - length(mmk))
      )) AS AggregateFunction(uniqHLL12, String))
FROM (
  SELECT ts, channel, countryName, isRobot,
         arrayMap(j -> m[toUInt16(j)], range(4096)) AS regs, mmk, mmv
  FROM (
    SELECT ts, channel, countryName, isRobot,
           CAST((mmk, mmv) AS Map(UInt16, UInt8)) AS m, mmk, mmv
    FROM (
      SELECT ts, channel, countryName, isRobot,
             tupleElement(mm, 1) AS mmk, tupleElement(mm, 2) AS mmv
      FROM (
        -- max rank per register index, within the row
        SELECT ts, channel, countryName, isRobot,
               arrayReduce('maxMap',
                 -- low 12 bits of the coupon's 26-bit slot: also folds lgK 14 -> 12
                 [arrayMap(c -> toUInt16(bitAnd(c, 4095)), cps)],
                 -- top 6 bits are the rank; clamp to ClickHouse's ceiling of 21
                 [arrayMap(c -> toUInt8(least(bitShiftRight(c, 26), 21)), cps)]) AS mm
        FROM (
          -- the coupon array: UInt32 each, after a preInts*4 preamble
          SELECT ts, channel, countryName, isRobot,
                 arrayMap(i -> reinterpretAsUInt32(substring(users_sketch, pre + 1 + i*4, 4)),
                          range(intDiv(length(users_sketch) - pre, 4))) AS cps
          FROM (
            SELECT *, bitAnd(reinterpretAsUInt8(substring(users_sketch, 1, 1)), 63) * 4 AS pre
            FROM rollup_hll_bytes_sqlonly
            WHERE cityHash64(channel, ts, isRobot, ifNull(countryName, '')) % 26 = {k:UInt8}
          )
        )
      )
    )
  )
);

-- Query the result with plain SQL, no UDF:
--   SELECT channel, uniqHLL12Merge(s) FROM _sqlonly_users GROUP BY channel;
--
-- The transplant rule from docs/hll-sketch-formats.md still applies in full: these states
-- must never be merged with natively-built ClickHouse ones, or the union double-counts.
