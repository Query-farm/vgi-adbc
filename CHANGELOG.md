# Changelog

All notable changes to **vgi-adbc** are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/); this project adheres to
[Semantic Versioning](https://semver.org/).

## Compatibility

vgi-adbc requires **`vgi-python >= 0.50.0`** — the first release speaking **VGI protocol
3.2**, which the `vgi` client extension currently published to the haybarn community
repository requires (it matches the protocol major+minor exactly, so an older worker is
rejected at attach). The `vgi` extension is a moving target; CI's `http-e2e` job installs the **live**
extension (and runs nightly), so a future drift ahead of the pinned `vgi-python` is caught as
a red build, signalling a needed bump.

## [Unreleased]

### Changed
- **Ported to VGI protocol 3.2** (`vgi-python` 0.16.0 -> 0.50.0, `vgi-rpc` 0.24.0 -> 0.53.1),
  matching the `vgi` extension now served by the haybarn community repository. Catalog methods
  take raw schema paths, and remote NOT NULL columns are declared as canonical `ConstraintInfo`
  records. The worker's embedded engine stays on haybarn 1.5.4 (its `adbc_scanner` API).
- Tests: the SQLLogic suite runs `haybarn-unittest --keep-home` (2.0 isolates `$HOME`). The
  in-process HTTP pytest suite needs a haybarn 2.0 client (`uv run --with 'haybarn>=2.0.0rc11'
  pytest tests/test_http_transport.py`) and skips on the locked 1.5.4, whose channel serves a
  pre-3.2 `vgi`; `run_http_tests.sh` still covers the HTTP path with the latest haybarn-cli.
- Catalog metadata: added `vgi.subtitle`, dropped grader fields from `vgi.agent_test_tasks`
  and a boilerplate pointer from `vgi.doc_md` (current vgi-lint rules).

## [0.3.0] — 2026-07-16

### Added
- **Observability.** Engine lifecycle stats (counters + live `active_sessions` /
  `active_scans` gauges), a rate-limited `adbc.stats` log line, and an opt-in Prometheus
  `/metrics` endpoint (`VGI_ADBC_METRICS_PORT`). OpenTelemetry documented.
- **PostgreSQL + MySQL over HTTP tests**, run against service containers in CI; MySQL driver
  bundled in the image.
- **Container build-time attach smoke** (`ci/attach_smoke.py`) — a broken driver/extension
  bake now fails the image build instead of the first user's `ATTACH`.
- **HTTP concurrency test** and a **benchmark correctness smoke** (row-shape + throughput).
- Nightly scheduled CI as an extension-compat drift guard.

### Changed
- **Resource-safe defaults**: idle session eviction (`VGI_ADBC_SESSION_IDLE_SECONDS`, 1h) and
  idle scan reaping (`VGI_ADBC_SCAN_IDLE_SECONDS`, 5m) are now **on by default** so a
  crashed/disconnected client can't leak a session or scan cursor forever. Set either to `0`
  to disable.

### Documented / investigated
- **Scan-path query timeout is not enforceable at the worker** with the current ADBC
  PostgreSQL driver (it rejects connection-option injection, `adbc_scanner` owns the scan
  connection, and a server-side `statement_timeout` was not honored on it in testing). The
  `adbc_query` raw handle *is* bounded by `VGI_ADBC_QUERY_TIMEOUT_SECONDS`.
- The `read_only=false` write-then-teardown DuckDB abort no longer reproduces; locked in with
  a regression test.

## [0.2.0]

### Changed
- **Strictly sticky sessions (correctness).** Removed stateless reconnect: a rebuilt
  connection would silently drop an open transaction, temp tables, or session settings, and a
  fresh worker process can't know at runtime whether a session was stateful. A missed session
  now raises `SessionNotFoundError` — re-`ATTACH` to recover.
- Pinned **`vgi-python >= 0.16.0`** to match the current community `vgi` extension schema.

### Added
- CI ADBC-driver provisioning (`ci/provision-adbc.sh`) so attach-backed jobs run on the
  runners; the engine now requires **haybarn** (not stock duckdb).

## [0.1.0]

- Initial release: attach + introspect + scan with projection & filter pushdown, cardinality
  estimates, catalog refresh, `adbc_query` raw-SQL passthrough, read-only enforcement, over
  stdio and HTTP, with a container image.

[Unreleased]: https://github.com/Query-farm/vgi-adbc/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/Query-farm/vgi-adbc/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/Query-farm/vgi-adbc/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Query-farm/vgi-adbc/releases/tag/v0.1.0
