# vgi-adbc — agent notes

A VGI **catalog worker** that lets any DuckDB client `ATTACH` a remote **ADBC** database
(PostgreSQL, SQLite, …) over VGI and query it with pushdown. The worker holds the native
ADBC connection server-side and streams Arrow back, so clients that can't load native ADBC
drivers (DuckDB-WASM, locked-down builds) can still use it.

## The core idea

ADBC drivers are native shared libraries and ADBC connections are live, process-local
objects — a web/WASM DuckDB can load neither. So we run ADBC **server-side**: the worker
embeds a DuckDB (the `haybarn` distribution) with the `adbc_scanner` community extension,
`ATTACH`es the remote there, and re-exposes it to the VGI client as a catalog. The client
only needs the `vgi` extension.

```
DuckDB (+vgi ext) ──VGI/Arrow──► vgi-adbc worker ──ADBC──► PostgreSQL / SQLite / …
                                 (haybarn DuckDB + adbc_scanner)
```

## Architecture (keep this separation)

| Module | Responsibility |
|--------|----------------|
| `engine.py` | The embedded DuckDB engine + the **sticky-session registry**. One `haybarn` DuckDB instance per attach, `adbc_scanner` loaded, remote `ATTACH`ed as `remote`. Introspection (`schemas`/`tables`/`arrow_schema`), scans (`scan`), cardinality (`row_count`), refresh, and raw queries (`raw_query`). |
| `catalog.py` | `AdbcCatalog(ReadOnlyCatalogInterface)` — resolves the ADBC target from ATTACH options, opens the session, mirrors the remote catalog (schemas + tables/views), routes scans to `adbc_scan`, advertises `adbc_query`. |
| `functions.py` | `adbc_scan` (generic dynamic-schema pushdown scan) and `adbc_query` (raw passthrough). Both stream one Arrow batch per `process` call via the shared `_active_scans` reader registry. |
| `sql.py` | Safe DuckDB SQL construction (identifier/string quoting, option-key validation). |
| `metadata.py` | Authored `vgi.*` catalog/schema tags (the lint gate scores these). |
| `worker.py` | `AdbcWorker(Worker)` + `main()` + the `AttachOptions` declaration. |

## How a scan flows (the non-obvious wiring)

1. `ATTACH 'adbc' AS db (TYPE vgi, LOCATION <worker>, driver 'sqlite', uri '<path>')` →
   `catalog_attach` resolves an `AdbcTarget`, `engine.attach()` opens the connection and
   returns a 16-byte **session id**, which becomes the catalog's `attach_opaque_data`.
2. `SELECT * FROM db.main.employees WHERE …` → the extension calls
   `catalog_table_scan_branches_get` (answered from `table_scan_function_get`), which returns
   `ScanFunctionResult(function_name="adbc_scan", positional_arguments=[schema, table])`.
3. `adbc_scan` runs **in the worker**: `on_bind` introspects the remote table's Arrow
   schema; `cardinality` returns the cached `count(*)`; `process` reads
   `params.attach_opaque_data` (the session id) → the live session, builds
   `SELECT <projected cols> FROM remote.schema.table WHERE <pushed filters>`, and streams
   batches. DuckDB pushes the predicate through `adbc_scanner` to the source.

## adbc_query (raw SQL passthrough)

`adbc_query('sql')` is a **catalog-scoped, user-callable** table function
(`<alias>.<schema>.adbc_query(...)`) that runs native SQL on the remote via an
`adbc_connect` handle + `adbc_scan(handle, sql)` — a separate connection from the ATTACH
scan pool. Wiring: advertised from `AdbcCatalog.schema_contents(type=TABLE_FUNCTION)` via
`self._function_to_info(AdbcQuery, name)` (empty `output_schema` placeholder — the real
schema comes from `on_bind`, which does `raw_query_schema` = `… LIMIT 0`), and registered
in `Worker.functions`. The C++ extension stamps the attach handle onto the function entry,
so `params.attach_opaque_data` is populated just like the scan function (see the vgi-python
`accumulate` fixture — the canonical template for a catalog-scoped callable function with
attach context). The engine opens the raw handle lazily (`_ensure_query_handle`), keyed on
the session; the handle lives in `adbc_scanner`'s **process-global** `ConnectionRegistry`,
so it survives a refresh connection-swap and is `adbc_disconnect`-ed in `_close_session`.

**Read-only seal (load-bearing).** The ATTACH scan connection honors `READ_ONLY`, but the
`adbc_query` handle is a *separate* `adbc_connect` connection that does **not** inherit it —
so under a read-only attach `_ensure_query_handle` runs a driver-specific seal once via
`adbc_execute` (`_READONLY_SEAL`: SQLite `PRAGMA query_only = ON`, PostgreSQL `SET SESSION
CHARACTERISTICS AS TRANSACTION READ ONLY`). No-result statements (INSERT/SET/PRAGMA-set)
**must** go through `adbc_execute`, not `adbc_scan` (which raises "must return at least one
column" yet still executes the side effect — that was the original read-only bypass). A
driver with no seal entry is *refused* `adbc_query` under a read-only attach (raises) rather
than silently permitting writes; `read_only 'false'` opts out.

## Catalog refresh

`adbc_scanner` **snapshots** the remote catalog at ATTACH — external DDL is invisible until
a re-ATTACH (verified). So `engine.refresh()` opens a **fresh** connection with the remote
attached, re-fingerprints the remote's columns (`information_schema.columns` hash), and —
only when the fingerprint changed — atomically swaps `session.con`, clears the caches, and
bumps `session.catalog_version`. `catalog_version()` returns that version so DuckDB
re-introspects on change. The connection-swap (rather than DETACH-in-place) is what keeps
in-flight concurrent scans from breaking; the previous connection's DB stays alive for
existing cursors until they finish. `catalog_refresh_seconds` drives `maybe_refresh()` from
`catalog_version()` (called at transaction boundaries, never mid-scan); `0` = stable
snapshot. Column-only changes still bump the version (the fingerprint covers
`data_type`/position).

## Resource lifecycle & tuning knobs

Bounds are opportunistic and off by default (defaults preserve v1's unbounded behavior);
`AdbcEngine.__init__` takes them and `_engine_config_from_env` maps env vars to them:

- **Reaping is opportunistic, not threaded.** `AdbcEngine.reap()` runs from `attach` and
  `catalog_version` (transaction boundaries), rate-limited by `VGI_ADBC_REAP_INTERVAL_SECONDS`
  — no background thread (avoids thread-lifecycle/GC headaches; an idle worker accrues no new
  leaks anyway). It evicts sessions idle past `VGI_ADBC_SESSION_IDLE_SECONDS` and reaps scan
  readers idle past `VGI_ADBC_SCAN_IDLE_SECONDS`.
- **Scan registry (`functions._active_scans`).** Entries are `_ScanEntry(reader, session_id,
  last_used)`. `release_scans_for_session` is called by `detach`/`_close_session` **before**
  closing the connection (readers pin cursors that pin pooled ADBC connections). Don't reap a
  scan reader while its scan is still live — a resumed `process` re-opens from row 0. That's
  why `VGI_ADBC_SCAN_IDLE_SECONDS` defaults off and must exceed the slowest inter-batch gap.
- **Cursor creation is guarded** by `session.lifecycle_lock` in `_reader`/`_fetchall` so a
  refresh/detach can't swap/close `con` mid-`cursor()`.
- **Cardinality is pluggable** (`VGI_ADBC_CARDINALITY` = estimate|count|off): estimate prefers
  PostgreSQL `pg_class.reltuples` via the read-only raw handle, else a bounded count
  (`VGI_ADBC_CARDINALITY_MAX_COUNT`) that never full-scans a huge remote at plan time.
- **Refresh resilience:** `maybe_refresh` swallows a failed refresh (logs WARNING, keeps the
  cached catalog) and advances `last_refresh` so a transient outage doesn't retry every call.

## Load-bearing gotchas

- **Declare ATTACH options twice.** The `vgi` extension rejects any undeclared ATTACH
  option ("Accepted options: (none)"). You must (1) declare `class AttachOptions` on the
  Worker with `Annotated[T, AttachOption(...)]` fields, **and** (2) because we override
  `catalogs()`, include `attach_option_specs=[spec.serialize() for spec in self.attach_option_specs]`
  in the `CatalogInfo` (the base `catalogs()` does this; an override drops it).
- **Where metadata lives.** Catalog-level tags/comment ride on
  `CatalogAttachResult.comment` / `.tags`; schema tags on `SchemaInfo.tags`; function
  examples on `Meta.examples`. `CatalogInfo` has **no** `tags` field.
- **Streaming state.** A live `RecordBatchReader` can't serialize into an HTTP state token.
  Keep it in the process-local `_active_scans` dict keyed by a token stored in the
  (serializable) `_ScanState`. Strict sticky sessions keep every call on this process.
- **Strictly sticky — no reconnect (deliberate).** The ADBC connection + `_sessions`/
  `_active_scans` live in one worker **process**'s memory, and the VGI DuckDB extension runs a
  **pool of worker subprocesses even over stdio** (verified: distinct PIDs). Normally a
  client's calls reuse one process, but when a worker RPC returns an **error**, the extension
  routes the *next* call to a **freshly-spawned** process with an empty registry (verified:
  12+ sequential *normal* queries stay on one PID; a single failing `adbc_query` shifts
  following calls to new PIDs). `AdbcEngine.resolve` (used by catalog + functions) does a
  strict registry lookup and raises `SessionNotFoundError` on a miss — it does **not** rebuild
  the connection. This is a correctness decision: a rebuilt connection is a *fresh* one that
  would silently drop an open transaction, temp tables, or session settings — so a lost
  session fails loudly and the client re-`ATTACH`es. (`attach_opaque_data` is just the 16-byte
  session id; no target/credentials ride in it. Stateless reconnect for connections with *no*
  connection-local state is deferred.)
  **Concurrent scans hit this too, with no prior error:** a stdio process serves one open
  stream at a time, so one query that scans two relations of the same attach concurrently
  (table JOIN table, table JOIN `adbc_query`, correlated subquery) can get its second
  `init` routed to a fresh sibling → `SessionNotFoundError`, depending on how DuckDB
  schedules the scans (verified 2026-11 on haybarn 2.0: a correlated subquery under
  GROUP BY — `SELECT dept, (SELECT count(*) FROM active_high a WHERE a.dept = e.dept)
  FROM employees e GROUP BY dept` — fails every time; a plain hash join of `employees`
  and `adbc_query(...)` usually works, since the build side is drained before the probe
  scan starts, but is not guaranteed). HTTP (single `vgi-serve` process) is unaffected —
  `run_http_tests.sh` asserts both shapes. Published examples therefore use ONE remote
  scan + local catalog metadata.
- **Streaming likewise never silently restarts.** A live `RecordBatchReader` is a
  non-serializable cursor position. If a `process` call for an *already-opened* scan finds no
  reader in this process — the reader was reaped as idle, or a hop landed the batch elsewhere —
  `_ScanState.opened` is True → raise `ScanInterruptedError` (re-run the query) rather than
  re-open from row 0 and duplicate rows. HTTP sets the `vgi_adbc_sticky` cookie
  (`catalog_version` asserts it) so a load balancer can pin affinity.
- **haybarn, not duckdb.** `engine.py` imports `haybarn` (a duckdb-python drop-in) so the
  embedded DuckDB is recent enough to load a recent `adbc_scanner`. `VGI_ADBC_EXTENSION_PATH`
  loads a local `.duckdb_extension` instead of the community build. haybarn is capped
  below 1.5.5: that `adbc_scanner` build makes raw handles per-connection and needs explicit
  `columns` for SQLite queries, breaking the `adbc_query` passthrough.
- **Filter oracle = stock duckdb 1.5.5.** vgi-python binds every pushed filter predicate
  against an in-process DuckDB 1.5.5 / Haybarn 1.5.5-rc1 only. `_filter_engine.py` (imported
  first by `vgi_adbc`) points that evaluator at the `duckdb==1.5.5` dependency, so haybarn
  stays on the 1.5.4 ADBC engine.
- **Protocol 3.3 function listing.** `schema_contents(TABLE_FUNCTION)` lists `adbc_query`
  and the `table_only` `adbc_scan` under every remote schema (the extension refuses a
  table whose scan function is unlisted). DuckDB then calls them schema-qualified
  (e.g. `public`), so `AdbcWorker._candidates_for` resolves those two names unscoped.
- **Clean shutdown.** `AdbcEngine.close_all()` (registered via `atexit` in `get_engine`)
  closes every connection before interpreter shutdown — otherwise `adbc_scanner`'s global
  `ConnectionRegistry` can abort at `__cxa_finalize` with live connections.

## Upstream adbc_scanner notes

Two `adbc_scanner` bugs were found here and fixed on its `v1.5` branch (they reach the
worker once the **community** build rebuilds; `FORCE INSTALL adbc_scanner FROM community`):

- **Teardown abort** (commit `c9689de`): the global `ConnectionRegistry` destructor released
  pooled connections against an already-destroyed pool at process exit → `std::terminate`.
  Fixed by leaking the singleton + a `noexcept` pool deleter. The worker's `atexit` cleanup
  is belt-and-suspenders on top.
- **Unicode identifiers** (commit `9c7fdb1`): `AdbcTableEntry::GetScanFunction` built the
  table/schema name with `Value::CreateValue(std::string)`, which returns a **BLOB** — so a
  non-ASCII name (`naïve_café`) threw on the ATTACH scan path. Fixed to `Value(name)`.

Still upstream-blocked (no worker fix): the ADBC drivers don't propagate NOT NULL / PK /
constraints (every column reports `nullable=True`, `duckdb_constraints` is empty — both
SQLite and PostgreSQL), and the PostgreSQL driver rejects a `search_path`/`options`
passthrough. `adbc_query` (native `PRAGMA`/`information_schema`) is the workaround for both.

- **PostgreSQL filtered-scan-returns-0 (isolation only).** A `SELECT ... WHERE ...` run
  **directly** on the mirrored `remote.<schema>.<table>` PostgreSQL catalog returns 0 rows
  (any predicate, even wrapped/`OFFSET 0` to block pushdown; a filter-free scan returns all
  rows, and the native `adbc_query` handle returns correct rows). **End-to-end through the
  DuckDB client this is not observed** — the E2E and `bench/` PostgreSQL runs return correct
  filtered results because DuckDB re-applies the predicate — so it's not a worker correctness
  bug, but direct-engine tests must not assert filtered PostgreSQL scans (see
  `tests/test_engine_postgres.py::test_pg_projected_scan`).

## The connector doesn't own its tables

Schemas/tables/columns are discovered **live** from the attached database, so `vgi-lint.toml`
exempts the per-table / per-column / per-constraint rules (VGI111/112/113/123/126/201/202/
411/412/413/501/511/520/806/807, the example-count VGI151, and VGI307 for `adbc_query`'s
runtime-determined schema): there is nothing static to document. The catalog, its schema,
and the functions are documented richly in `metadata.py` / `functions.py`, and those rules
stay enforced.

## Transports & testing

- `run_tests.sh` — stdio sqllogictest E2E (`haybarn-unittest`) against a freshly-built
  SQLite fixture attached over ADBC.
- `run_http_tests.sh` — HTTP E2E. **`haybarn-unittest` auto-skips an `http://` ATTACH**
  ("skip on error_message matching 'HTTP'") before it connects, so the HTTP E2E drives the
  worker with **`haybarn-cli`** (the real DuckDB shell, which does attach over HTTP) and
  asserts on its output. `vgi-serve` uses a single waitress process, so the sticky session
  lives in-process.
- `tests/` — pytest: pure-logic (SQL quoting, option resolution, alignment, scan registry)
  always run; engine tests exercise real SQLite over ADBC and skip if `adbc_scanner` can't
  load; `tests/test_engine_postgres.py` runs only when `VGI_ADBC_TEST_PG` holds a DSN.
  `tests/test_http_transport.py` drives the worker over `http://`: SQLite always, plus
  **PostgreSQL and MySQL** when `VGI_ADBC_TEST_PG` / `VGI_ADBC_TEST_MYSQL` are set (the CI
  `http-e2e` job runs postgres + mysql service containers and sets both — proving the
  driver-agnostic "arbitrary server over HTTP" path). MySQL's DSN is the ADBC Go form
  (`user:pw@tcp(host:port)/db`) and it surfaces the database as the `main` schema.
- `run_benchmarks.sh` + `bench/` — end-to-end benchmarks (a real haybarn+vgi client attaches
  the worker) over **both** transports against SQLite (always) and PostgreSQL (throwaway
  Docker `postgres:16`, auto-skipped if Docker is absent). Writes `bench/results/{md,json}`.

```bash
uv sync --extra dev
uv run ruff check --fix . && uv run ruff format . && uv run mypy vgi_adbc/
uv run pytest -q
./run_tests.sh
./run_http_tests.sh

# vgi-lint needs a live DB to introspect — build the fixture and point it at it:
uv run python test/make_fixture.py /tmp/f.sqlite
uvx --prerelease allow --from vgi-lint-check vgi-lint lint bin/vgi-adbc-worker \
    --attach-option driver=sqlite --attach-option uri=/tmp/f.sqlite --fail-on info --no-execute
```

## Scope (v1)

Read-only: attach + introspect + scan with projection & filter pushdown, cardinality
estimates, catalog refresh, and the `adbc_query` raw-SQL passthrough. Sessions are **strictly
sticky** — a call that misses the owning process fails loudly (`SessionNotFoundError`), never
reconnecting, because a fresh connection would drop connection-local state (transactions,
temp tables); a live scan that hops likewise fails with `ScanInterruptedError` rather than
restarting. **Deferred:** writes, transactions, bulk insert, and stateless read scale-out
(pooled reconnect for state-free connections). PostgreSQL needs a reachable server for its
E2E; SQLite runs anywhere (its ADBC driver embeds SQLite).
