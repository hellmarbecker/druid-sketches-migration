"""Tests that need a live Druid and/or ClickHouse. They skip themselves when it is not up.

Start them per CLAUDE.md:
    cd $DRUID_HOME && bin/start-druid -m 6g
    clickhouse server --config-file=clickhouse/config.xml

These cover the claims the migrations rest on and that no offline test can reach:
the uniqHLL12 format still matching this ClickHouse build, the DataSketches seed still
matching across systems, and the two HLL merge behaviours (transplanted states union
correctly with each other; mixing with native states double-counts).
"""

from __future__ import annotations

import base64
import struct

import pytest
import requests
from datasketches import hll_sketch, hll_union, tgt_hll_type, update_theta_sketch

from sketch_io import decode_druid_sketch, enc_agg_state, enc_string
from uniqhll12 import LG_K, decode_registers, encode_state, state_from_datasketches

FIXTURE = "wikipedia_rollup_sketches"
EXPECTED_ROWS = 2486
EXPECTED_EVENTS = 39244
DS_DEFAULT_SEED_HASH = 37836


def hll(items, lg_k=LG_K):
    s = hll_sketch(lg_k, tgt_hll_type.HLL_8)
    for i in items:
        s.update(i)
    return s


# --------------------------------------------------------------------------- ClickHouse
@pytest.mark.clickhouse
@pytest.mark.parametrize("n", [17, 100, 1000, 100000])
def test_uniqhll12_format_still_matches_this_build(ch_query, n):
    """The regression gate for the reverse-engineered format. If ClickHouse ever changes its
    aggregate-state layout, this fails and migrate_hll_transplant.py must not be trusted."""
    produced = bytes.fromhex(
        ch_query(f"SELECT hex(toString(uniqHLL12State(number))) FROM numbers({n})"))
    assert encode_state(decode_registers(produced)) == produced


@pytest.mark.clickhouse
def test_uniqhll12_small_set_form_is_still_sparse_below_17(ch_query):
    """Transplants must always emit the dense form; this pins where the boundary sits."""
    for n, expected_len in ((1, 10), (16, 130)):
        state = bytes.fromhex(
            ch_query(f"SELECT hex(toString(uniqHLL12State(number))) FROM numbers({n})"))
        assert state[0] == 0 and len(state) == expected_len
    dense = bytes.fromhex(
        ch_query("SELECT hex(toString(uniqHLL12State(number))) FROM numbers(17)"))
    assert dense[0] == 1 and len(dense) == 2651


@pytest.mark.clickhouse
def test_clickhouse_ignores_the_stored_histogram(ch_query):
    """Documents observed behaviour, and pins it so a change surfaces here.

    The estimate is computed from the registers; the serialised histogram is recomputed on
    read rather than trusted. Four states with identical registers but deliberately wrong
    histograms must therefore agree. We still write a correct histogram -- byte-exact
    round-trip against ClickHouse's own states depends on it -- but no *estimate* does.
    """
    import random
    from uniqhll12 import BUCKETS, HIST_SLOTS, REG_BYTES

    rng = random.Random(7)
    regs = [rng.choice([0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]) for _ in range(BUCKETS)]
    good = encode_state(regs)

    def rewrite_histogram(state, hist, zero_count):
        b = bytearray(state)
        b[1 + REG_BYTES: 1 + REG_BYTES + HIST_SLOTS * 4] = struct.pack(f"<{HIST_SLOTS}I", *hist)
        b[1 + REG_BYTES + HIST_SLOTS * 4:] = struct.pack("<H", zero_count)
        return bytes(b)

    variants = {
        "good": good,
        "zeroed": rewrite_histogram(good, [0] * HIST_SLOTS, 0),
        "claims_empty": rewrite_histogram(good, [BUCKETS] + [0] * (HIST_SLOTS - 1), BUCKETS),
        "claims_full": rewrite_histogram(good, [0] * (HIST_SLOTS - 1) + [BUCKETS], 0),
    }
    for state in variants.values():
        assert decode_registers(state) == regs, "variants must differ only in the histogram"

    ch_query("DROP TABLE IF EXISTS _t_hist")
    ch_query("CREATE TABLE _t_hist (label String, s AggregateFunction(uniqHLL12, String)) "
             "ENGINE = MergeTree ORDER BY label")
    try:
        for label, state in variants.items():
            ch_query("INSERT INTO _t_hist FORMAT RowBinary", data=enc_string(label) + state)
        got = {label: int(ch_query(
            f"SELECT uniqHLL12Merge(s) FROM _t_hist WHERE label = '{label}'"))
            for label in variants}
        assert len(set(got.values())) == 1, (
            f"the stored histogram now affects the estimate: {got} -- "
            "docs/hll-sketch-formats.md says it does not, and needs updating")
    finally:
        ch_query("DROP TABLE IF EXISTS _t_hist")


@pytest.mark.clickhouse
def test_transplanted_state_is_readable_by_clickhouse(ch_query):
    ch_query("DROP TABLE IF EXISTS _t_transplant")
    ch_query("CREATE TABLE _t_transplant (label String, s AggregateFunction(uniqHLL12, String)) "
             "ENGINE = MergeTree ORDER BY label")
    try:
        s = hll(f"v{i}" for i in range(10000))
        ch_query("INSERT INTO _t_transplant FORMAT RowBinary",
                 data=enc_string("a") + state_from_datasketches(s.serialize_compact()))
        got = float(ch_query("SELECT uniqHLL12Merge(s) FROM _t_transplant"))
        # Two different bias corrections over the same registers; a few percent is expected,
        # a wildly different number means the transplant is broken.
        assert abs(got - s.get_estimate()) / s.get_estimate() < 0.06
    finally:
        ch_query("DROP TABLE IF EXISTS _t_transplant")


@pytest.mark.clickhouse
def test_transplanted_states_union_with_each_other(ch_query):
    """The property that makes native uniqHLL12Merge usable on migrated data."""
    ch_query("DROP TABLE IF EXISTS _t_union")
    ch_query("CREATE TABLE _t_union (label String, s AggregateFunction(uniqHLL12, String)) "
             "ENGINE = MergeTree ORDER BY label")
    try:
        a = hll(f"k{i}" for i in range(1000))
        b = hll(f"k{i}" for i in range(500, 1500))
        for label, sk in (("a", a), ("b", b), ("a_dup", a)):
            ch_query("INSERT INTO _t_union FORMAT RowBinary",
                     data=enc_string(label) + state_from_datasketches(sk.serialize_compact()))

        ab = int(ch_query("SELECT uniqHLL12Merge(s) FROM _t_union WHERE label IN ('a','b')"))
        with_dup = int(ch_query("SELECT uniqHLL12Merge(s) FROM _t_union"))
        assert abs(ab - 1500) / 1500 < 0.05, "union of overlapping sets should approach 1500"
        assert with_dup == ab, "re-adding an identical sketch must not inflate the union"
    finally:
        ch_query("DROP TABLE IF EXISTS _t_union")


@pytest.mark.clickhouse
def test_transplanted_and_native_states_must_not_be_mixed(ch_query):
    """Documents the hard constraint by demonstrating the breakage. If this ever stops
    failing, the two systems have converged on a hash and the docs need revisiting."""
    ch_query("DROP TABLE IF EXISTS _t_mix")
    ch_query("CREATE TABLE _t_mix (label String, s AggregateFunction(uniqHLL12, String)) "
             "ENGINE = MergeTree ORDER BY label")
    try:
        a = hll(f"k{i}" for i in range(1000))
        ch_query("INSERT INTO _t_mix FORMAT RowBinary",
                 data=enc_string("transplanted") + state_from_datasketches(a.serialize_compact()))
        ch_query("INSERT INTO _t_mix SELECT 'native', "
                 "uniqHLL12State(concat('k', toString(number))) FROM numbers(1000)")

        transplanted = int(ch_query("SELECT uniqHLL12Merge(s) FROM _t_mix WHERE label='transplanted'"))
        native = int(ch_query("SELECT uniqHLL12Merge(s) FROM _t_mix WHERE label='native'"))
        merged = int(ch_query("SELECT uniqHLL12Merge(s) FROM _t_mix"))

        assert abs(transplanted - 1000) / 1000 < 0.05
        assert abs(native - 1000) / 1000 < 0.05
        assert merged > 1.8 * 1000, (
            "identical inputs merged to ~1x, so the hashes now agree -- "
            "re-check docs/hll-sketch-formats.md before relying on this")
    finally:
        ch_query("DROP TABLE IF EXISTS _t_mix")


@pytest.mark.clickhouse
def test_uniqtheta_state_is_a_bare_length_prefixed_sketch(ch_query):
    """The whole Theta write path is varint(len) + compact sketch, with no wrapper."""
    ch_query("DROP TABLE IF EXISTS _t_theta")
    ch_query("CREATE TABLE _t_theta (s AggregateFunction(uniqTheta, String)) "
             "ENGINE = MergeTree ORDER BY tuple()")
    try:
        sk = update_theta_sketch(14)
        for i in range(1000):
            sk.update(f"probe{i}")
        ch_query("INSERT INTO _t_theta FORMAT RowBinary",
                 data=enc_agg_state(sk.compact().serialize()))
        assert int(ch_query("SELECT uniqThetaMerge(s) FROM _t_theta")) == 1000
    finally:
        ch_query("DROP TABLE IF EXISTS _t_theta")


@pytest.mark.clickhouse
def test_theta_dedupes_across_systems(ch_query):
    """Unlike HLL, a transcoded Theta sketch unions correctly with a native one."""
    ch_query("DROP TABLE IF EXISTS _t_theta_mix")
    ch_query("CREATE TABLE _t_theta_mix (label String, s AggregateFunction(uniqTheta, String)) "
             "ENGINE = MergeTree ORDER BY label")
    try:
        sk = update_theta_sketch(14)
        for i in range(1000):
            sk.update(f"probe{i}")
        ch_query("INSERT INTO _t_theta_mix FORMAT RowBinary",
                 data=enc_string("transcoded") + enc_agg_state(sk.compact().serialize()))
        ch_query("INSERT INTO _t_theta_mix SELECT 'native_same', "
                 "uniqThetaState(concat('probe', toString(number))) FROM numbers(1000)")
        ch_query("INSERT INTO _t_theta_mix SELECT 'native_shift', "
                 "uniqThetaState(concat('probe', toString(number + 500))) FROM numbers(1000)")

        # Under k=4096 everything is exact, so these are equalities, not tolerances.
        same = int(ch_query("SELECT uniqThetaMerge(s) FROM _t_theta_mix "
                            "WHERE label IN ('transcoded','native_same')"))
        shifted = int(ch_query("SELECT uniqThetaMerge(s) FROM _t_theta_mix "
                               "WHERE label IN ('transcoded','native_shift')"))
        assert same == 1000, "identical sets must dedupe completely"
        assert shifted == 1500, "50% overlap must be detected"
    finally:
        ch_query("DROP TABLE IF EXISTS _t_theta_mix")


@pytest.mark.druid
@pytest.mark.clickhouse
def test_druid_is_queryable_from_clickhouse_sql(ch_query):
    """clickhouse/druid_query.py makes Druid addressable from ClickHouse SQL via
    executable(), which is what `url()` cannot do (Druid's SQL endpoint is POST-only).

    Skips rather than fails when the script is not deployed -- it needs user_scripts_path
    set in config.xml and the executable bit, neither of which is a property of this repo's
    Python code.
    """
    probe = (
        "SELECT * FROM executable('druid_query.py', 'JSONEachRow', 'c UInt64', "
        "(SELECT $$SELECT COUNT(*) AS c FROM " + FIXTURE + "$$))"
    )
    try:
        got = ch_query(probe)
    except RuntimeError as exc:
        pytest.skip(f"druid_query.py not usable from ClickHouse: {str(exc)[:120]}")

    assert got.isdigit(), f"expected a row count, got {got[:200]}"
    assert int(got) == EXPECTED_ROWS

    # The point of the script: sketch bytes survive the round trip intact, and can be
    # transcoded to a uniqTheta state in SQL. Compared against Druid's own estimate, so this
    # fails if any byte were mangled in transit.
    got = ch_query(
        "WITH (L -> multiIf(L < 128, char(L), L < 16384, "
        "        char(bitOr(bitAnd(L,127),128), bitShiftRight(L,7)), "
        "        char(bitOr(bitAnd(L,127),128), bitOr(bitAnd(bitShiftRight(L,7),127),128), "
        "             bitShiftRight(L,14)))) AS leb128 "
        "SELECT reinterpretAsUInt8(substring(sk, 2, 1)) AS ser_ver, "
        "       reinterpretAsUInt8(substring(sk, 3, 1)) AS family, "
        "       finalizeAggregation(CAST(concat(leb128(length(sk)), sk) "
        "                                AS AggregateFunction(uniqTheta, String))) AS est, "
        "       any(druid_est) AS druid "
        "FROM (SELECT base64Decode(trim(BOTH '\"' FROM s)) AS sk, e AS druid_est "
        "      FROM executable('druid_query.py', 'JSONEachRow', 's String, e Float64', "
        "        (SELECT $$SELECT users_theta_16384 AS s, "
        "                        THETA_SKETCH_ESTIMATE(users_theta_16384) AS e "
        "                 FROM " + FIXTURE + " WHERE channel = '#en.wikipedia' LIMIT 1$$))) "
        "GROUP BY sk"
    ).split("\t")
    ser_ver, family, est, druid = int(got[0]), int(got[1]), int(got[2]), float(got[3])
    assert (ser_ver, family) == (3, 3), "not a compact DataSketches Theta preamble"
    assert est == round(druid), f"transcoded estimate {est} != druid {druid}"


@pytest.mark.druid
@pytest.mark.clickhouse
def test_druid_reachable_through_the_nginx_shim(ch_query):
    """nginx/druid-get-to-post.conf lets plain url() query Druid by rewriting GET to POST.

    Skips when the shim is not running -- it is an optional third daemon, not something the
    repo's code can guarantee. Asserts the sketch bytes survive, not merely that rows come
    back, since a proxy that mangled the body would still return well-formed JSON.
    """
    # Probe the shim directly rather than discovering it through ClickHouse. url() against a
    # dead upstream blocks for the whole client timeout instead of failing fast, which turns
    # a missing optional daemon into a two-minute hang and then an error rather than a skip.
    try:
        requests.get("http://127.0.0.1:8890/health", timeout=2).raise_for_status()
    except requests.RequestException as exc:
        pytest.skip(f"nginx GET->POST shim not reachable on :8890 ({type(exc).__name__})")

    got = ch_query(
        "SELECT reinterpretAsUInt8(substring(sk, 2, 1)) AS ser_ver, "
        "       reinterpretAsUInt8(substring(sk, 3, 1)) AS family, count() AS rows "
        "FROM (SELECT base64Decode(trim(BOTH '\"' FROM users_theta_16384)) AS sk "
        "      FROM url('http://127.0.0.1:8890/named/rollup-theta', 'JSONEachRow', "
        "               '__time String, channel String, countryName Nullable(String), "
        "                isRobot String, cnt UInt64, sum_added Int64, "
        "                users_theta_16384 String, pages_theta_4096 String')) "
        "GROUP BY ser_ver, family"
    )
    ser_ver, family, rows = (int(x) for x in got.split("\t"))
    assert (ser_ver, family) == (3, 3), "not a compact DataSketches Theta preamble"
    assert rows == EXPECTED_ROWS


# -------------------------------------------------------------------------------- Druid
@pytest.mark.druid
def test_fixture_rollup_shape(druid_query):
    row = druid_query(f'SELECT COUNT(*) AS nrows, SUM("count") AS events FROM {FIXTURE}')[0]
    assert row["nrows"] == EXPECTED_ROWS
    assert row["events"] == EXPECTED_EVENTS


@pytest.mark.druid
def test_sketch_columns_are_complex_not_finalised(druid_query):
    """If an ingestion ever finalises the aggregators, these become plain numbers and the
    whole migration silently has nothing to move."""
    types = {r["COLUMN_NAME"]: r["DATA_TYPE"] for r in druid_query(
        f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_NAME = '{FIXTURE}'")}
    assert types["users_hll_k12_hll4"] == "COMPLEX<HLLSketch>"
    assert types["pages_hll_k14_hll8"] == "COMPLEX<HLLSketch>"
    assert types["users_theta_16384"] == "COMPLEX<thetaSketch>"
    assert types["pages_theta_4096"] == "COMPLEX<thetaSketch>"


@pytest.mark.druid
def test_per_column_lg_k_differs_and_is_readable(druid_query):
    """The fixture mixes lgK 12 and 14 precisely so code cannot assume a default."""
    from sketch_io import hll_lg_k
    row = druid_query(f"SELECT users_hll_k12_hll4 AS u, pages_hll_k14_hll8 AS p "
                      f"FROM {FIXTURE} LIMIT 1")[0]
    assert hll_lg_k(decode_druid_sketch(row["u"])) == 12
    assert hll_lg_k(decode_druid_sketch(row["p"])) == 14


@pytest.mark.druid
def test_python_reads_druid_sketches_exactly(druid_query):
    """datasketches-java 4.2.0 wrote these; the Python binding must agree to float precision."""
    row = druid_query(
        f"SELECT users_hll_k12_hll4 AS h, HLL_SKETCH_ESTIMATE(users_hll_k12_hll4) AS he, "
        f"users_theta_16384 AS t, THETA_SKETCH_ESTIMATE(users_theta_16384) AS te "
        f"FROM {FIXTURE} WHERE channel = '#en.wikipedia' LIMIT 1")[0]

    from datasketches import compact_theta_sketch
    assert abs(hll_sketch.deserialize(decode_druid_sketch(row["h"])).get_estimate()
               - row["he"]) < 1e-6
    assert abs(compact_theta_sketch.deserialize(decode_druid_sketch(row["t"])).get_estimate()
               - row["te"]) < 1e-6


@pytest.mark.druid
def test_stored_rollup_sketches_are_sparse(druid_query):
    """Sparse is the common case in a rollup table, so extraction must handle it."""
    row = druid_query(f"SELECT users_hll_k12_hll4 AS h FROM {FIXTURE} LIMIT 1")[0]
    assert decode_druid_sketch(row["h"])[7] & 0x03 != 2


# ------------------------------------------------------------------- both systems needed
@pytest.mark.druid
@pytest.mark.clickhouse
def test_theta_seed_hash_matches_across_systems(druid_query, ch_query):
    """The assumption the entire Theta path rests on. Same seed hash => same key hashes to
    the same value => cross-system unions actually dedupe."""
    def seed_hash(sketch_bytes):
        return struct.unpack_from("<H", sketch_bytes, 6)[0]

    druid_sketch = decode_druid_sketch(
        druid_query(f"SELECT users_theta_16384 AS t FROM {FIXTURE} LIMIT 1")[0]["t"])

    raw = bytes.fromhex(
        ch_query("SELECT hex(toString(uniqThetaState(toString(number)))) FROM numbers(1000)"))
    length, pos = 0, 0
    shift = 0
    while True:                       # strip the LEB128 length prefix
        byte = raw[pos]
        pos += 1
        length |= (byte & 0x7F) << shift
        if not byte & 0x80:
            break
        shift += 7
    ch_sketch = raw[pos:pos + length]

    assert seed_hash(druid_sketch) == DS_DEFAULT_SEED_HASH
    assert seed_hash(ch_sketch) == DS_DEFAULT_SEED_HASH
