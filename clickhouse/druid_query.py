#!/Users/hellmarbecker/druid-sketches-migration/.venv/bin/python
"""ClickHouse `executable()` table function: run a Druid SQL query from inside ClickHouse.

Exists because `url()` cannot reach Druid: /druid/v2/sql is POST-with-a-JSON-body only and
returns 405 for GET, while url() issues GET for SELECT and has no method or body parameter.
This script is the smallest thing that closes that gap -- ClickHouse pipes it a query, it
POSTs to Druid, and Druid's objectLines output is already valid JSONEachRow on the way back.

    SELECT *
    FROM executable('druid_query.py', 'JSONEachRow', 'channel String, users Float64',
                    (SELECT $$SELECT channel,
                                     APPROX_COUNT_DISTINCT_DS_THETA(users_theta_16384) AS users
                              FROM wikipedia_rollup_sketches GROUP BY 1 ORDER BY users DESC$$));

Reads one query per input line and emits that query's rows, so a multi-row input runs several
Druid queries and concatenates the results. ClickHouse may send input in any of several
formats depending on how the table function was called, so each line is unwrapped
defensively: a bare string, a TSV field, or a JSON object all work.

Must never crash: an exception here aborts the whole ClickHouse query, so failures are
reported as a single JSON object with an `error` field instead, and the real reason goes to
stderr where ClickHouse logs it.

Requires user_scripts_path in clickhouse/config.xml (already set for hll_merge_udf.py), plus
the executable bit. The shebang names the venv interpreter directly because `executable()`
runs the script itself rather than through a shell.
"""

import json
import os
import sys

import requests

DRUID_URL = os.environ.get("DRUID_ROUTER_URL", "http://localhost:8888")
TIMEOUT = 600


def extract_query(line: str) -> str:
    """Pull the SQL out of whatever wrapper ClickHouse used for this line."""
    line = line.strip()
    if not line:
        return ""
    if line.startswith("{"):                      # JSONEachRow: take the only value
        obj = json.loads(line)
        return str(next(iter(obj.values())))
    if line.startswith('"'):                      # a quoted JSON string
        return json.loads(line)
    return line.split("\t")[0].replace("\\t", "\t").replace("\\n", "\n")


def run(query: str) -> None:
    r = requests.post(
        f"{DRUID_URL}/druid/v2/sql",
        json={"query": query, "resultFormat": "objectLines"},
        timeout=TIMEOUT,
    )
    if r.status_code != 200:
        raise RuntimeError(f"druid HTTP {r.status_code}: {r.text[:300]}")
    for row in r.text.splitlines():
        if row.strip():
            print(row, flush=True)


def main() -> None:
    for line in sys.stdin:
        try:
            query = extract_query(line)
            if query:
                run(query)
        except Exception as exc:                  # noqa: BLE001 - must not abort the query
            print(json.dumps({"error": str(exc)[:500]}), flush=True)
            print(f"druid_query: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
