#!/bin/sh
# Dispatch the vgi-adbc image into one of its transports:
#   http   (default) HTTP server on $PORT (/health + VGI RPC)
#   stdio            a worker DuckDB spawns over stdio (on-host execution)
#   *                exec'd verbatim (debug escape hatch)
#
# HTTP concurrency: waitress defaults to 4 threads. Sticky, stateful ADBC sessions make
# that a real ceiling, so set VGI_ADBC_HTTP_THREADS to size the pool to expected
# concurrency (the worker's --http path exposes --http-threads; vgi-serve does not).
set -e
case "${1:-http}" in
  http)
    set -- vgi-adbc-worker --http --host 0.0.0.0 --port "${PORT:-8000}"
    [ -n "${VGI_ADBC_HTTP_THREADS:-}" ] && set -- "$@" --http-threads "${VGI_ADBC_HTTP_THREADS}"
    exec "$@"
    ;;
  stdio) shift 2>/dev/null || true; exec vgi-adbc-worker "$@" ;;
  *)     exec "$@" ;;
esac
