#!/usr/bin/env bash
# End-to-end benchmarks for the vgi-adbc worker over BOTH transports (stdio subprocess and
# HTTP) against SQLite (always, no server) and PostgreSQL (a throwaway Docker container).
#
# A real DuckDB client (haybarn + the vgi extension) attaches the worker and times a query
# suite; see bench/run_bench.py. PostgreSQL is auto-skipped (with a message) when Docker or
# the ADBC driver is unavailable, so the SQLite benchmark runs anywhere.
#
# Env knobs:
#   BENCH_ROWS   (default 100000)   rows in the benchmark table
#   BENCH_ITERS  (default 20)       timed iterations per query
#   BENCH_HTTP_PORT (default 8134)  port for the HTTP worker
#   BENCH_PG_DSN                    use an existing PostgreSQL instead of Docker
#   BENCH_NO_PG=1                   skip PostgreSQL entirely
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"

ROWS="${BENCH_ROWS:-100000}"
ITERS="${BENCH_ITERS:-20}"
HTTP_PORT="${BENCH_HTTP_PORT:-8134}"
OUT_DIR="$REPO_ROOT/bench/results"
WORK_DIR="$REPO_ROOT/duckdb_unittest_tempdir"
mkdir -p "$OUT_DIR" "$WORK_DIR"

echo "==> Ensuring the vgi extension is available"
echo "LOAD vgi;" | uvx haybarn-cli >/dev/null 2>&1 || {
    echo "    installing vgi from community"
    echo "INSTALL vgi FROM community;" | uvx haybarn-cli
}

echo "==> Ensuring ADBC drivers (sqlite, postgresql) are installed"
DBC="$(command -v dbc || echo "$HOME/.local/bin/dbc")"
if [[ ! -x "$DBC" ]]; then
    echo "    installing dbc"
    curl -LsSf https://dbc.columnar.tech/install.sh | sh
    DBC="$HOME/.local/bin/dbc"
fi
"$DBC" install sqlite >/dev/null 2>&1 || true
"$DBC" install postgresql >/dev/null 2>&1 || true

echo "==> Building the SQLite benchmark fixture ($ROWS rows)"
SQLITE_FIXTURE="$WORK_DIR/bench.sqlite"
uv run --quiet python "$REPO_ROOT/bench/make_bench_fixture.py" "$SQLITE_FIXTURE" "$ROWS"

# --- PostgreSQL setup (optional) -------------------------------------------------------
PG_DSN=""
PG_CONTAINER=""
cleanup() {
    if [[ -n "$PG_CONTAINER" ]]; then
        docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

if [[ "${BENCH_NO_PG:-0}" == "1" ]]; then
    echo "==> Skipping PostgreSQL (BENCH_NO_PG=1)"
elif [[ -n "${BENCH_PG_DSN:-}" ]]; then
    echo "==> Using existing PostgreSQL from BENCH_PG_DSN"
    PG_DSN="$BENCH_PG_DSN"
elif command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    echo "==> Starting a throwaway PostgreSQL container"
    PG_CONTAINER="vgi-adbc-bench-pg"
    docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 || true
    docker run -d --rm --name "$PG_CONTAINER" \
        -e POSTGRES_PASSWORD=bench -e POSTGRES_DB=bench \
        -p 5433:5432 postgres:16 >/dev/null
    PG_DSN="postgresql://postgres:bench@127.0.0.1:5433/bench"
    echo "    waiting for PostgreSQL to accept connections"
    for _ in $(seq 1 60); do
        if docker exec "$PG_CONTAINER" pg_isready -U postgres >/dev/null 2>&1; then break; fi
        sleep 0.5
    done
else
    echo "==> Docker unavailable — skipping PostgreSQL (SQLite benchmark still runs)"
fi

if [[ -n "$PG_DSN" ]]; then
    echo "==> Loading the PostgreSQL benchmark table ($ROWS rows)"
    if ! uv run --quiet python "$REPO_ROOT/bench/load_pg.py" "$PG_DSN" "$ROWS"; then
        echo "    PostgreSQL load failed — continuing with SQLite only" >&2
        PG_DSN=""
    fi
fi

# --- Run the benchmark -----------------------------------------------------------------
echo "==> Running benchmarks (stdio + HTTP)"
PG_ARGS=()
[[ -n "$PG_DSN" ]] && PG_ARGS=(--pg "$PG_DSN")

uv run --quiet python "$REPO_ROOT/bench/run_bench.py" \
    --sqlite "$SQLITE_FIXTURE" \
    "${PG_ARGS[@]}" \
    --transport both \
    --rows "$ROWS" \
    --iters "$ITERS" \
    --http-port "$HTTP_PORT" \
    --out "$OUT_DIR"

echo "==> Done. Results in $OUT_DIR/{results.md,results.json}"
