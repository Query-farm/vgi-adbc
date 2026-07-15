#!/usr/bin/env bash
# Run the SQLLogic (haybarn) E2E suite in test/sql/ against the Python worker, using the
# haybarn DuckDB distribution's unittest runner (which loads the `vgi` extension from the
# community repository). The worker itself installs `adbc_scanner` from the community
# repository into its own embedded haybarn DuckDB on first ATTACH.
#
# The suite attaches a freshly-built SQLite fixture through the worker over ADBC and
# asserts schema, counts, projection pushdown, and filter pushdown. SQLite needs no
# server (the ADBC SQLite driver embeds SQLite), so this runs anywhere.
#
# Prerequisites (one-time):
#   uv tool install haybarn-unittest                      # the DuckDB unittest binary
#   echo "INSTALL vgi FROM community;" | uvx haybarn-cli  # install the vgi extension
#   uv sync --extra dev                                   # the worker's Python environment
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"

UNITTEST="${VGI_UNITTEST:-$(command -v haybarn-unittest || true)}"
if [[ -z "$UNITTEST" || ! -x "$UNITTEST" ]]; then
    echo "ERROR: haybarn-unittest not found. Install it with:" >&2
    echo "       uv tool install haybarn-unittest" >&2
    exit 1
fi

# Ensure the vgi community extension is installed for this haybarn version.
if ! echo "LOAD vgi;" | uvx haybarn-cli >/dev/null 2>&1; then
    echo "==> Installing vgi extension from community repository"
    echo "INSTALL vgi FROM community;" | uvx haybarn-cli
fi

# Build the SQLite fixture the tests attach over ADBC (deterministic, no server needed).
FIXTURE_DIR="$REPO_ROOT/duckdb_unittest_tempdir"
mkdir -p "$FIXTURE_DIR"
FIXTURE="$FIXTURE_DIR/adbc_test.sqlite"
uv run --quiet python "$REPO_ROOT/test/make_fixture.py" "$FIXTURE"

WORKER="$REPO_ROOT/bin/vgi-adbc-worker"
TEST_GLOB="${1:-test/sql/*}"

echo "==> Running SQLLogic tests"
echo "    worker:   $WORKER"
echo "    unittest: $UNITTEST"
echo "    fixture:  $FIXTURE"
echo "    tests:    $TEST_GLOB"

VGI_TEST_WORKER="$WORKER" \
VGI_WORKER_CATALOG_NAME="adbc" \
VGI_ADBC_TEST_SQLITE="$FIXTURE" \
    "$UNITTEST" --test-dir "$REPO_ROOT" "$TEST_GLOB"
