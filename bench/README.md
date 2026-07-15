# Benchmarks

End-to-end benchmarks for the vgi-adbc worker. A real DuckDB client (`haybarn` + the `vgi`
extension) `ATTACH`es the worker and times a query suite over **both** transports (stdio
subprocess and HTTP) against **SQLite** and **PostgreSQL**.

## Run

```sh
./run_benchmarks.sh          # SQLite (always) + PostgreSQL (throwaway Docker container)
BENCH_NO_PG=1 ./run_benchmarks.sh          # SQLite only
BENCH_ROWS=1000000 BENCH_ITERS=30 ./run_benchmarks.sh
BENCH_PG_DSN=postgresql://user:pw@host/db ./run_benchmarks.sh   # use an existing PostgreSQL
```

Results are written to `bench/results/{results.md,results.json}` and printed to stdout.
PostgreSQL is auto-skipped (with a message) when Docker or the ADBC driver is unavailable.

Prerequisites are provisioned by the wrapper: the `vgi` extension (via `haybarn-cli`) and
the `sqlite`/`postgresql` ADBC drivers (via `dbc`).

## Files

| File | What |
|------|------|
| `run_bench.py` | The driver — attaches over each transport, times the suite, writes JSON+MD |
| `make_bench_fixture.py` | Builds the SQLite fixture (parametrized row count) |
| `load_pg.py` | Loads the same table into PostgreSQL via `psql \copy` |
| `_data.py` | Shared, seeded row generator + DDL (identical data across engines) |

## Query suite

`full_scan`, `projection`, `filter_selective` (1-row pushdown), `filter_broad` (~70%),
`count_star`, `group_by`, and `adbc_query` (raw passthrough). `attach_ms` is the one-time
cold ATTACH + catalog introspection, reported separately from steady-state per-query time.

## Interpreting the numbers

- **stdio vs HTTP:** stdio is markedly faster per query; HTTP carries a per-statement
  round-trip floor (each streamed batch is an HTTP request under the sticky session). Use
  stdio for latency-sensitive local clients; HTTP for WASM/locked-down clients that can't
  spawn a subprocess.
- **Pushdown:** `filter_selective` crossing 1 row confirms predicate + projection pushdown
  to the source; compare its time to `full_scan`.
- **Cardinality:** `attach_ms` and the first `count_star` reflect the catalog/cardinality
  probing cost (see the `VGI_ADBC_CARDINALITY` knob).
