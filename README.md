# vgi-adbc

Query **PostgreSQL**, **SQLite**, and any other [ADBC](https://arrow.apache.org/adbc/)
database from [DuckDB](https://duckdb.org) — over [VGI](https://query.farm).

ADBC (Arrow Database Connectivity) is a native, Arrow-first database API. Its drivers are
native shared libraries and its connections are live, stateful, process-local objects —
none of which a web/WASM DuckDB (or a locked-down DuckDB build) can load or hold.
`vgi-adbc` moves ADBC to the **server side**: the worker holds the native connection, runs
the query through an embedded [`haybarn`](https://query.farm) DuckDB with the
[`adbc_scanner`](https://github.com/Query-farm/adbc_scanner) community extension, and
streams Apache Arrow back to the DuckDB client. The client only needs the VGI extension —
so even DuckDB-WASM in a browser can reach a remote database, with **projection and filter
pushdown** all the way down to the source.

```
┌────────────┐   VGI / Arrow IPC    ┌──────────────── vgi-adbc worker ────────────────┐
│  DuckDB    │  (stdio or HTTP)     │  embedded haybarn DuckDB + adbc_scanner          │   ADBC
│  (+ vgi    │◀────────────────────▶│  ATTACH remote (TYPE adbc, driver 'postgresql')  │◀────────▶  PostgreSQL
│   ext)     │                      │  SELECT … FROM remote.schema.table WHERE …       │   native   / SQLite / …
└────────────┘                      └──────────────────────────────────────────────────┘
```

**What it does**

- **`ATTACH` a remote database** and query it with ordinary SQL, tables and views
  discovered live.
- **Projection + filter pushdown** — the columns you select and the `WHERE` you write are
  translated and sent to the source; only the rows/columns you asked for cross the wire.
- **Cardinality estimates** so DuckDB plans joins against large remote tables sensibly.
- **`adbc_query('sql')`** — run native SQL on the remote in its own dialect, for anything
  the mirror can't express.
- **Read-only** (v1) — writes are rejected on the scan path (the remote is attached
  `READ_ONLY`) *and* on the `adbc_query` passthrough (its connection is sealed read-only).
- **Strictly sticky sessions** — an attached connection lives in one worker process; every
  follow-up must reach it. A mis-routed call fails loudly rather than silently rebuilding a
  connection that would lose transactions / temp tables. See
  [Sticky sessions](#sticky-sessions).

## Use from DuckDB

```sql
-- Attach a remote database over the worker (spawned via stdio here).
ATTACH 'adbc' AS pg (
    TYPE vgi,
    LOCATION '/path/to/vgi-adbc/bin/vgi-adbc-worker',
    driver 'postgresql',
    uri 'postgresql://user:pw@localhost:5432/mydb'
);

-- Query it as if it were local — the WHERE and the column list are pushed to Postgres.
SELECT id, name FROM pg.public.events WHERE id > 100 ORDER BY id;
SELECT count(*) FROM pg.public.events;

-- SQLite works the same way (the ADBC SQLite driver embeds SQLite; no server needed).
ATTACH 'adbc' AS sl (TYPE vgi, LOCATION '…/bin/vgi-adbc-worker',
                     driver 'sqlite', uri '/data/app.sqlite');
SELECT * FROM sl.main.orders;

DETACH pg;
```

The connection string can also be the ATTACH name, mirroring the native `adbc_scanner`
syntax:

```sql
ATTACH '/data/app.sqlite' AS sl (TYPE vgi, LOCATION '…/bin/vgi-adbc-worker', driver 'sqlite');
```

### ATTACH options

| Option | Meaning |
|--------|---------|
| `driver` **(required)** | ADBC driver name (`postgresql`, `sqlite`, …) or a path to a driver shared library / manifest |
| `uri` | The connection string (may instead be given as the ATTACH name) |
| `username`, `password`, `database`, … | Forwarded verbatim to the ADBC driver |
| `entrypoint` | Custom ADBC driver entry-point symbol |
| `read_only` | Attach the remote read-only (v1 is read-only regardless) |
| `catalog_refresh_seconds` | Re-read the remote catalog at most this often (see *Refreshing*); `0` = stable snapshot |

Credentials are never logged.

### Drivers

`adbc_scanner` loads ADBC drivers by name through the standard driver-manifest mechanism.
Install them once with [`dbc`](https://columnar.tech/dbc/):

```sh
curl -LsSf https://dbc.columnar.tech/install.sh | sh
dbc install postgresql
dbc install sqlite
dbc install mysql
```

`dbc` also offers `mssql` (SQL Server), `trino`, `flightsql`, `datafusion`, and more.
**PostgreSQL, SQLite, and MySQL are exercised end-to-end here — including over the HTTP
transport** (see `tests/test_http_transport.py`); other ADBC drivers (Trino, Snowflake,
Flight SQL, …) work the same way — the connector is driver-agnostic, and the VGI transport is
identical regardless of which driver the worker loads. (MySQL surfaces its database as the
`main` schema and takes a Go-style DSN, e.g. `driver 'mysql', uri 'user:pw@tcp(host:3306)/db'`.)

> **PostgreSQL schemas.** Some drivers scope table discovery to the connection's default
> search path — the PostgreSQL driver lists only the `search_path` schemas (default:
> `public`). Tables in other schemas are not surfaced in the catalog (querying one returns
> "table does not exist"). Set the server-side `search_path` for the connecting role or the
> database, **or** reach them directly with `adbc_query('SELECT * FROM other.table')`.

## Raw SQL passthrough (`adbc_query`)

Every attached database also exposes an `adbc_query` table function that runs SQL **on the
remote database itself, in its own dialect** — for engine-specific features and objects the
mirrored catalog doesn't expose (other schemas, system catalogs, functions):

```sql
-- Native SQL, executed on the remote:
SELECT * FROM pg.public.adbc_query('SELECT now() AT TIME ZONE ''UTC'' AS utc_now');

-- Reach a table in a schema the catalog didn't surface:
SELECT count(*) FROM pg.public.adbc_query('SELECT * FROM analytics.summary');

-- A SQLite PRAGMA — impossible to express against the mirror:
SELECT name, pk FROM sl.main.adbc_query('PRAGMA table_info(orders)');

-- Combine it with the mirror: each column's declared SQLite type next to the DuckDB type
-- the attached table exposes (e.g. INTEGER -> BIGINT, REAL -> DOUBLE):
SELECT p.name AS column_name, p.type AS source_type, c.data_type AS duckdb_type
FROM sl.main.adbc_query('PRAGMA table_info(orders)') AS p
JOIN duckdb_columns() AS c
  ON c.database_name = 'sl' AND c.schema_name = 'main'
 AND c.table_name = 'orders' AND c.column_name = p.name
ORDER BY p.cid;
```

The result schema is determined at query time. `adbc_query` is scoped to the attachment it
is called on (`<alias>.<schema>.adbc_query(...)`), so it uses that database's connection.

## Refreshing the catalog

`adbc_scanner` snapshots the remote catalog at `ATTACH`, so remote DDL (a new table, a
changed column) is invisible to an existing attachment by default. Two ways to pick it up:

- **Re-attach** — `DETACH`, then `ATTACH` again. Always gets the current catalog.
- **`catalog_refresh_seconds`** — pass this ATTACH option to have the worker re-read the
  remote catalog at most that often. At the next statement, DuckDB re-introspects and sees
  the change. The version only advances when the catalog actually changed, so an unchanged
  remote stays cached, and in-flight scans are never disrupted (the refresh swaps in a
  fresh connection rather than detaching the live one).

```sql
ATTACH 'adbc' AS pg (TYPE vgi, LOCATION '<worker>',
                     driver 'postgresql', uri 'postgresql://…', catalog_refresh_seconds '30');
```

The default (`0`) keeps the attach-time snapshot stable — no background re-reads.

## Sticky sessions

An ADBC connection is a live, native, **process-local** object that can't be serialized into
a VGI state token. Each `ATTACH` is held in **one worker process**, keyed by a session id in
the (AEAD-sealed, on HTTP) attach handle. Every follow-up call **must** reach that same
process. A call that lands elsewhere — a mis-routed HTTP request, or the VGI worker pool
spawning a sibling subprocess (notably after a prior error) — fails loudly with a clear
`SessionNotFoundError`; re-`ATTACH` to recover.

> **Queries that read two remote relations at once (subprocess transport).** A stdio
> worker process serves one open stream at a time, so when a single query scans two
> relations of the same attachment concurrently — a join of two remote tables, a remote
> table joined to `adbc_query(...)`, a correlated subquery — the VGI extension can hand the
> second scan to a sibling subprocess, which doesn't hold the session: the query fails with
> `SessionNotFoundError` — every time for some shapes (a correlated subquery under
> `GROUP BY`), intermittently for others, depending on how DuckDB schedules the scans.
> Over **HTTP** (one `vgi-serve` process holds every session) such queries work. On the
> subprocess transport, push the join down into one native `adbc_query('… JOIN …')`, or
> combine a single remote scan with local data (e.g. `duckdb_columns()`, as above).

The worker **deliberately does not reconnect** a lost session. A rebuilt connection is a
*fresh* connection: it would silently lose an open transaction, temp tables, or session
settings, and a streaming scan's cursor position can't move between processes at all. Failing
loudly is the only way to stay correct — so, when serving over HTTP behind a load balancer,
**enable session affinity (sticky sessions)**; the worker sets a `vgi_adbc_sticky` cookie at
attach and rejects follow-ups that arrive without it. (Stateless read scale-out — pooled
reconnect for connections with *no* connection-local state — is deferred future work.)

## Tuning & resource limits

All of these are optional environment variables read by the worker.

| Variable | Default | Meaning |
|----------|---------|---------|
| `VGI_ADBC_SESSION_IDLE_SECONDS` | `3600` (1h) | Evict an attachment idle longer than this, so a crashed/never-`DETACH`ing client can't leak a session (a whole DuckDB instance + ADBC connection) forever. Any query resets the timer, so only a *dormant* attach is evicted; the client re-`ATTACH`es on next use. `0` disables. |
| `VGI_ADBC_MAX_SESSIONS` | `0` (∞) | Refuse a new `ATTACH` past this many live attachments (idle ones are reaped first). |
| `VGI_ADBC_SCAN_IDLE_SECONDS` | `300` (5m) | Reap an in-flight scan reader not read within this window (frees a cursor an abandoned/disconnected scan left pinned). Well above any real inter-batch gap, so a live scan is never reaped mid-stream (that would raise `ScanInterruptedError`). `0` disables. |
| `VGI_ADBC_MAX_SCANS` | `0` (∞) | Cap concurrent scan readers; the least-recently-used are evicted past it. |
| `VGI_ADBC_CARDINALITY` | `estimate` | `estimate` (cheap PostgreSQL `pg_class.reltuples`, else a bounded count), `count` (always exact `count(*)`), or `off`. |
| `VGI_ADBC_CARDINALITY_MAX_COUNT` | `0` (∞) | When counting for cardinality, count at most this many rows and return no estimate past the cap — so planning never full-scans a huge remote table. |
| `VGI_ADBC_QUERY_TIMEOUT_SECONDS` | `0` (off) | Per-statement timeout on the **`adbc_query` raw handle** (PostgreSQL `statement_timeout`, applied via `adbc_execute`). See the note below on why the **scan path** can't be bounded the same way. |
| `VGI_ADBC_HTTP_THREADS` | waitress default (4) | HTTP worker thread pool size. Sticky, stateful sessions make the default a real ceiling — size it to expected concurrency. |
| `VGI_ADBC_METRICS_PORT` | — | Serve Prometheus metrics on this port (see *Observability*). Unset = disabled. |
| `VGI_ADBC_METRICS_HOST` | `0.0.0.0` | Bind address for the metrics endpoint. |
| `VGI_ADBC_EXTENSION_PATH` | — | Load a local `adbc_scanner.duckdb_extension` instead of installing from the community repository. |

> **PostgreSQL filter pushdown.** With the current `adbc_scanner` + PostgreSQL driver, a
> `WHERE` evaluated *directly on the mirrored catalog* can return an empty result in
> isolation; end-to-end through the DuckDB client, results are correct (DuckDB re-applies
> the predicate). Reach PostgreSQL with native SQL through `adbc_query(...)` if you need a
> pushed-down filter you can rely on at the source.

> **Query timeouts.** The worker enforces `VGI_ADBC_QUERY_TIMEOUT_SECONDS` on the
> **`adbc_query` raw handle** only. The **scan path** cannot be bounded at the worker with
> the current PostgreSQL driver: it rejects connection-option injection (`options` /
> `?options=`), `adbc_scanner` owns the scan connection so the worker can't `SET` on it, and
> a server-side `statement_timeout` was **not** honored on the scan connection in testing.
> Bound long scans with infrastructure controls (request/proxy timeouts, connection caps,
> monitoring) rather than a per-query setting.

## Observability

- **Prometheus metrics.** Set `VGI_ADBC_METRICS_PORT` to expose `/metrics` (and `/healthz`)
  on a background thread. Counters (`vgi_adbc_attaches_total`, `detaches_total`,
  `attach_errors_total`, `sessions_evicted_total`, `scans_opened_total`, `scans_reaped_total`,
  `scans_evicted_total`, `cardinality_errors_total`, `refresh_failures_total`) and gauges
  (`vgi_adbc_active_sessions`, `vgi_adbc_active_scans`) come from the engine's live stats.
- **Structured logs.** The worker logs a rate-limited `adbc.stats key=value …` snapshot line
  (the same counters/gauges) and per-RPC access logs — scrape either.
- **Tracing / OTel.** The underlying VGI worker supports OpenTelemetry; configure it via the
  standard `OTEL_*` environment variables to export request traces to a collector.

## Run as a container

The image serves both transports:

```sh
docker run --rm -p 8000:8000 ghcr.io/query-farm/vgi-adbc      # HTTP (default): /health + VGI RPC
docker run --rm -i ghcr.io/query-farm/vgi-adbc stdio          # stdio worker
```

The image bakes in the `adbc_scanner` community extension and the PostgreSQL + SQLite +
MySQL ADBC drivers (via `dbc`). `dbc` is kept in the image, so more drivers (`mssql`,
`trino`, `flightsql`, …) can be added with `dbc install <name>` in a derived image.

## Development

```sh
uv sync --extra dev --extra serve
uv run ruff format . && uv run ruff check . && uv run mypy vgi_adbc/
uv run pytest -q          # unit + engine + HTTP-transport tests (see below for what skips)
./run_tests.sh            # stdio SQLLogic E2E against a real DuckDB + the vgi extension
./run_http_tests.sh       # HTTP-transport E2E via haybarn-cli over an http:// ATTACH
./run_benchmarks.sh       # end-to-end benchmarks, stdio + HTTP, SQLite + PostgreSQL (see bench/)
```

Which tests run depends on what's available, so `pytest` is safe to run anywhere:

- **Pure-logic tests** (SQL quoting, option/target resolution, scan registry, batch
  alignment) always run.
- **Engine tests** exercise a real SQLite database over ADBC and skip if `adbc_scanner`
  can't load.
- **HTTP-transport tests** (`tests/test_http_transport.py`) serve the worker over HTTP and
  attach it from a Python DuckDB client; they skip unless the `serve` extra (waitress) and
  the ADBC drivers are present. CI's `http-e2e` job runs them plus the `haybarn-cli` E2E.
- **PostgreSQL tests** (`tests/test_engine_postgres.py`) run only when `VGI_ADBC_TEST_PG`
  holds a DSN — e.g. `docker run -d --rm -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=db -p
  5433:5432 postgres:16`, then `VGI_ADBC_TEST_PG=postgresql://postgres:pw@127.0.0.1:5433/db
  uv run pytest -q tests/test_engine_postgres.py`.

The worker's embedded engine is `haybarn` (a duckdb-python drop-in, pinned recent so its
community extension repository serves a recent `adbc_scanner`). Point
`VGI_ADBC_EXTENSION_PATH` at a local `adbc_scanner.duckdb_extension` to load a specific
build instead of installing from the community repository.

## Layout

| Path | What |
|------|------|
| `vgi_adbc/engine.py` | Embedded haybarn-DuckDB engine + the strictly-sticky session registry (idle eviction, caps); introspection, scans, cardinality, refresh, raw queries |
| `vgi_adbc/catalog.py` | `AdbcCatalog(ReadOnlyCatalogInterface)` — ATTACH, introspect, route scans, advertise `adbc_query` |
| `vgi_adbc/functions.py` | `adbc_scan` (generic pushdown scan) and `adbc_query` (raw passthrough) table functions + the in-flight scan-reader registry |
| `vgi_adbc/sql.py` | Safe DuckDB SQL construction (quoting/escaping/option-key validation) |
| `vgi_adbc/metadata.py` | Authored `vgi.*` catalog/schema documentation tags |
| `vgi_adbc/worker.py` | The `Worker` subclass, `AttachOptions`, `main()`, and the SIGTERM drain |
| `tests/` | pytest: pure-logic, engine (SQLite over ADBC), HTTP-transport, and opt-in PostgreSQL |
| `bench/` + `run_benchmarks.sh` | End-to-end benchmarks over both transports and both databases |

## License

MIT — Copyright 2026 Query Farm LLC.
