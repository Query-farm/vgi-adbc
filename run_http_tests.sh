#!/usr/bin/env bash
# HTTP-transport E2E: serve the worker over HTTP (single waitress process, so the sticky
# ADBC session lives in-process) and drive it with a real DuckDB client (`haybarn-cli`)
# over an http:// ATTACH LOCATION, asserting the results.
#
# Why not haybarn-unittest here: the sqllogictest runner auto-skips an http:// ATTACH
# ("skip on error_message matching 'HTTP'") before it ever connects, so it can't assert
# the HTTP path. haybarn-cli (the real DuckDB shell) does attach over HTTP, so we assert
# against it directly. The stdio suite (run_tests.sh) covers the sqllogictest assertions.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"

FIXTURE_DIR="$REPO_ROOT/duckdb_unittest_tempdir"
mkdir -p "$FIXTURE_DIR"
FIXTURE="$FIXTURE_DIR/adbc_http.sqlite"
uv run --quiet python "$REPO_ROOT/test/make_fixture.py" "$FIXTURE"

PORT="${VGI_ADBC_HTTP_PORT:-8123}"
LOG="$FIXTURE_DIR/http-server.log"

uv run --quiet vgi-serve vgi_adbc.worker:AdbcWorker --http --host 127.0.0.1 --port "$PORT" --quiet >"$LOG" 2>&1 &
SERVER_PID=$!
cleanup() { kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; }
trap cleanup EXIT

echo "==> Waiting for the HTTP worker on :$PORT"
for _ in $(seq 1 50); do
    if curl -fsS "http://127.0.0.1:$PORT/health" >/dev/null 2>&1; then break; fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then echo "server died; log:" >&2; cat "$LOG" >&2; exit 1; fi
    sleep 0.2
done

# Ensure the vgi extension is available to haybarn-cli.
echo "INSTALL vgi FROM community;" | uvx haybarn-cli >/dev/null 2>&1 || true

echo "==> Driving the worker over HTTP with haybarn-cli"
SQL=$(cat <<SQL
LOAD vgi;
ATTACH 'adbc' AS db (TYPE vgi, LOCATION 'http://127.0.0.1:${PORT}', driver 'sqlite', uri '${FIXTURE}');
SELECT 'count=' || count(*) FROM db.main.employees;
SELECT 'filtered=' || coalesce(string_agg(name, ',' ORDER BY id), '') FROM db.main.employees WHERE salary > 90000;
SELECT 'projected=' || string_agg(column_name, ',' ORDER BY column_name) FROM (DESCRIBE SELECT id, name FROM db.main.employees);
SELECT 'view=' || count(*) FROM db.main.active_high;
SELECT 'in=' || count(*) FROM db.main.employees WHERE dept IN ('eng', 'ops');
SELECT 'joined=' || string_agg(e.name, ',' ORDER BY e.name) FROM db.main.employees AS e JOIN db.main.adbc_query('SELECT dept, AVG(salary) AS avg_salary FROM employees GROUP BY dept') AS d ON d.dept = e.dept WHERE e.salary > d.avg_salary;
SELECT 'correlated=' || string_agg(dept || ':' || n, ',' ORDER BY dept) FROM (SELECT dept, (SELECT count(*) FROM db.main.active_high AS a WHERE a.dept = e.dept) AS n FROM db.main.employees AS e GROUP BY dept);
SQL
)
OUT=$(echo "$SQL" | uvx haybarn-cli :memory: -unsigned -list -noheader 2>&1)
echo "$OUT"

fail() { echo "HTTP E2E assertion failed: expected '$1'" >&2; echo "--- server log ---" >&2; cat "$LOG" >&2; exit 1; }
grep -Fxq 'count=5'              <<<"$OUT" || fail 'count=5'
grep -Fxq 'filtered=Alice,Bob'  <<<"$OUT" || fail 'filtered=Alice,Bob'
grep -Fxq 'projected=id,name'   <<<"$OUT" || fail 'projected=id,name'
grep -Fxq 'view=3'              <<<"$OUT" || fail 'view=3'
grep -Fxq 'in=3'               <<<"$OUT" || fail 'in=3'
# Two concurrent remote scans in one query (table JOIN adbc_query): works over HTTP because
# one vgi-serve process holds the session (README "Sticky sessions").
grep -Fxq 'joined=Alice,Carol'  <<<"$OUT" || fail 'joined=Alice,Carol'
# A correlated subquery under GROUP BY opens two scans of the attach at once; on the
# subprocess transport this shape fails deterministically with SessionNotFoundError.
grep -Fxq 'correlated=eng:2,ops:0,sales:1' <<<"$OUT" || fail 'correlated=eng:2,ops:0,sales:1'

echo "==> HTTP transport E2E passed (attach + introspect + scan + projection/filter pushdown over HTTP)"
