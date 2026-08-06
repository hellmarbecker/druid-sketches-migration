"""Shared fixtures. Integration tests skip themselves when their server is not up.

The point of the skip logic is that `pytest` with nothing running still exercises everything
that does not need a server -- which is most of the value, since the fragile part of this
repo is the reverse-engineered uniqHLL12 codec and the RowBinary encoders, both pure logic.

Reachability is probed once per session and cached, so a down server costs one timeout rather
than one per test.
"""

from __future__ import annotations

import functools

import pytest
import requests

from sketch_io import CH_URL, DRUID_URL

FIXTURE_DATASOURCE = "wikipedia_rollup_sketches"
PROBE_TIMEOUT = 3


@functools.cache
def clickhouse_up() -> bool:
    try:
        r = requests.post(CH_URL, params={"query": "SELECT 1"}, timeout=PROBE_TIMEOUT)
        return r.status_code == 200 and r.text.strip() == "1"
    except requests.RequestException:
        return False


@functools.cache
def druid_up() -> bool:
    """Druid must be reachable *and* hold the fixture datasource -- a bare Druid with no
    data would fail these tests for a reason that has nothing to do with the code."""
    try:
        r = requests.post(
            f"{DRUID_URL}/druid/v2/sql",
            json={"query": f"SELECT COUNT(*) AS c FROM {FIXTURE_DATASOURCE}"},
            timeout=PROBE_TIMEOUT,
        )
        return r.status_code == 200 and r.json()[0]["c"] > 0
    except (requests.RequestException, ValueError, KeyError, IndexError):
        return False


def pytest_collection_modifyitems(items):
    """Attach the umbrella `integration` marker and the skip conditions."""
    skip_ch = pytest.mark.skip(reason=f"ClickHouse not reachable at {CH_URL}")
    skip_druid = pytest.mark.skip(
        reason=f"Druid not reachable at {DRUID_URL}, or fixture datasource missing")

    for item in items:
        needs_ch = "clickhouse" in item.keywords
        needs_druid = "druid" in item.keywords
        if needs_ch or needs_druid:
            item.add_marker(pytest.mark.integration)
        if needs_ch and not clickhouse_up():
            item.add_marker(skip_ch)
        if needs_druid and not druid_up():
            item.add_marker(skip_druid)


@pytest.fixture(scope="session")
def ch_query():
    """Run a ClickHouse query, returning stripped text. Raises on non-200."""
    def run(sql: str, data: bytes | None = None) -> str:
        r = requests.post(CH_URL, params={"query": sql}, data=data, timeout=120)
        if r.status_code != 200:
            raise RuntimeError(f"ClickHouse error: {r.text[:400]}")
        return r.text.strip()
    return run


@pytest.fixture(scope="session")
def druid_query():
    """Run a Druid SQL query, returning the parsed JSON rows."""
    def run(sql: str) -> list[dict]:
        r = requests.post(f"{DRUID_URL}/druid/v2/sql", json={"query": sql}, timeout=120)
        if r.status_code != 200:
            raise RuntimeError(f"Druid error: {r.text[:400]}")
        return r.json()
    return run
