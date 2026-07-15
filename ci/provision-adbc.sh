#!/usr/bin/env bash
# Provision the ADBC runtime the tests/E2E need, the same way the Docker image does:
#   1. the SQLite ADBC driver (via `dbc`), into the standard manifest search path, and
#   2. the `adbc_scanner` extension pre-installed into haybarn (so the first ATTACH needs no
#      network) — with retries, because the haybarn community extension repo can be flaky.
#
# CI has no ADBC drivers by default, so haybarn's driver manager can't dlopen `sqlite`; this
# closes that gap. Runs on Linux and macOS runners. Requires the project already `uv sync`ed.
set -euo pipefail

retry() {
    local n=0
    until "$@"; do
        n=$((n + 1))
        if [ "$n" -ge 5 ]; then
            echo "command failed after $n attempts: $*" >&2
            return 1
        fi
        echo "==> attempt $n failed; retrying in 5s: $*" >&2
        sleep 5
    done
}

echo "==> Installing the SQLite ADBC driver via dbc"
if ! command -v dbc >/dev/null 2>&1 && [ ! -x "$HOME/.local/bin/dbc" ]; then
    retry sh -c 'curl -LsSf https://dbc.columnar.tech/install.sh | sh'
fi
DBC="$(command -v dbc || echo "$HOME/.local/bin/dbc")"
retry "$DBC" install sqlite

echo "==> Pre-installing the adbc_scanner extension into haybarn"
# Use `python -c` (not a heredoc) so a retry re-runs cleanly — heredoc stdin can't be re-read.
retry uv run --quiet python -c 'import haybarn; c = haybarn.connect(config={"allow_unsigned_extensions": "true"}); c.execute("INSTALL adbc_scanner FROM community"); c.execute("LOAD adbc_scanner"); print("adbc_scanner ready")'

echo "==> ADBC provisioning complete"
