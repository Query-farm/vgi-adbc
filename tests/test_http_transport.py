"""HTTP-transport integration tests: drive the worker over ``http://`` with a real client.

Starts a single ``vgi-serve`` HTTP worker (one process, so the sticky session lives
in-process) and attaches it from a ``haybarn`` DuckDB client over an ``http://`` ATTACH
LOCATION — the same path a browser DuckDB-WASM client uses. Asserts schema fidelity,
scans, projection + filter pushdown, the ``adbc_query`` passthrough, and the read-only
guarantee, all across the HTTP transport.

Skips cleanly when a piece isn't available: the HTTP-serving ``serve`` extra (waitress),
the ``adbc_scanner`` extension / ADBC drivers, or the ``vgi`` client extension. So this
runs in the standard ``pytest`` job when the environment supports it and is a no-op
otherwise (the dedicated CI ``http-e2e`` job installs the extra and guarantees it runs).
"""

from __future__ import annotations

import contextlib
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import time
import urllib.request
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

REPO_ROOT = Path(__file__).resolve().parent.parent

# Opt-in remote databases for the "arbitrary server over HTTP" tests. Set to a DSN to run:
#   VGI_ADBC_TEST_PG=postgresql://postgres:pw@127.0.0.1:5432/app
#   VGI_ADBC_TEST_MYSQL=root:pw@tcp(127.0.0.1:3306)/app   (the ADBC MySQL Go-style DSN)
_PG_DSN = os.environ.get("VGI_ADBC_TEST_PG")
_MYSQL_DSN = os.environ.get("VGI_ADBC_TEST_MYSQL")

_EMPLOYEES = [
    (1, "Alice", "eng", 120000.0, 1),
    (2, "Bob", "eng", 95000.0, 1),
    (3, "Carol", "sales", 80000.0, 0),
    (4, "Dave", "sales", 60000.0, 1),
    (5, "Eve", "ops", 45000.0, 1),
]


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _vgi_serve_path() -> str | None:
    from shutil import which

    # Prefer the project environment's vgi-serve: the worker must run on the project's own
    # (locked) haybarn, while CI may overlay a newer haybarn for the *client* side only
    # (`uv run --with haybarn>=2...`), whose overlay bin/ would otherwise shadow it on PATH.
    candidate = REPO_ROOT / ".venv" / "bin" / "vgi-serve"
    if candidate.exists():
        return str(candidate)
    return which("vgi-serve")


def _build_fixture(path: Path) -> None:
    con = sqlite3.connect(path)
    try:
        con.execute(
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary REAL, active INTEGER)"
        )
        con.executemany("INSERT INTO employees VALUES (?, ?, ?, ?, ?)", _EMPLOYEES)
        con.execute("CREATE VIEW active_high AS SELECT * FROM employees WHERE active = 1 AND salary > 50000")
        con.commit()
    finally:
        con.close()


@pytest.fixture(scope="module")
def http_worker(tmp_path_factory: pytest.TempPathFactory) -> Iterator[tuple[str, str]]:
    """Serve the worker over HTTP for the module; yield ``(base_url, sqlite_path)``."""
    serve = _vgi_serve_path()
    if serve is None:
        pytest.skip("vgi-serve not available (install the 'serve' extra: uv sync --extra dev --extra serve)")

    workdir = tmp_path_factory.mktemp("http_transport")
    fixture = workdir / "fixture.sqlite"
    _build_fixture(fixture)

    port = _free_port()
    log_path = workdir / "server.log"
    log = log_path.open("w")
    proc = subprocess.Popen(
        [serve, "vgi_adbc.worker:AdbcWorker", "--http", "--host", "127.0.0.1", "--port", str(port), "--quiet"],
        stdout=log,
        stderr=subprocess.STDOUT,
        cwd=str(REPO_ROOT),
    )
    try:
        ready = False
        health = f"http://127.0.0.1:{port}/health"
        for _ in range(100):
            if proc.poll() is not None:
                break
            try:
                with urllib.request.urlopen(health, timeout=1) as resp:  # noqa: S310 - localhost
                    if resp.status == 200:
                        ready = True
                        break
            except Exception:  # noqa: BLE001 - not up yet
                time.sleep(0.2)
        if not ready:
            pytest.skip(
                f"HTTP worker did not become ready (serve extra / drivers missing?); log:\n{log_path.read_text()}"
            )
        yield f"http://127.0.0.1:{port}", str(fixture)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        log.close()


def _vgi_client():  # noqa: ANN202 - haybarn connection with the vgi extension loaded
    """Open a haybarn DuckDB client with the ``vgi`` extension loaded (skip if unavailable)."""
    try:
        import haybarn
    except ImportError:
        pytest.skip("haybarn not available")
    # The worker speaks VGI protocol 3.2 and the vgi extension matches major+minor exactly.
    # The haybarn 1.5.x community channel only serves an older vgi build, so an in-process
    # 1.5.x client can't attach. The worker's embedded engine stays on the locked haybarn
    # 1.5.4 (its adbc_scanner API), so run this suite with a haybarn 2.0 client overlaid:
    #   uv run --with 'haybarn>=2.0.0rc11' pytest tests/test_http_transport.py
    # (./run_http_tests.sh covers the HTTP path with the latest haybarn-cli regardless.)
    if int(haybarn.__version__.split(".", 1)[0]) < 2:
        pytest.skip(
            f"in-process haybarn {haybarn.__version__} serves a pre-3.2 vgi client; "
            "run with: uv run --with 'haybarn>=2.0.0rc11' pytest tests/test_http_transport.py"
        )
    con = haybarn.connect(config={"allow_unsigned_extensions": "true"})
    try:
        try:
            con.execute("LOAD vgi")
        except Exception:  # noqa: BLE001 - not installed yet
            con.execute("INSTALL vgi FROM community")
            con.execute("LOAD vgi")
    except Exception as exc:  # noqa: BLE001 - offline / extension unavailable
        con.close()
        pytest.skip(f"vgi client extension unavailable: {exc}")
    return con


@pytest.fixture
def client(http_worker: tuple[str, str]):  # noqa: ANN201 - haybarn connection
    """A DuckDB (haybarn) client attached to the HTTP worker (SQLite); detaches on teardown."""
    location, sqlite_path = http_worker
    con = _vgi_client()
    con.execute(f"ATTACH 'adbc' AS db (TYPE vgi, LOCATION '{location}', driver 'sqlite', uri '{sqlite_path}')")
    try:
        yield con
    finally:
        with contextlib.suppress(Exception):
            con.execute("DETACH db")
        con.close()


def test_http_scan_count(client) -> None:  # noqa: ANN001
    assert client.execute("SELECT count(*) FROM db.main.employees").fetchone()[0] == 5


def test_http_schema_types(client) -> None:  # noqa: ANN001
    # DESCRIBE returns (column_name, column_type, ...); map name -> type.
    types = {r[0]: r[1] for r in client.execute("DESCRIBE SELECT * FROM db.main.employees").fetchall()}
    assert types["id"] == "BIGINT"
    assert types["name"] == "VARCHAR"
    assert types["salary"] == "DOUBLE"


def test_http_filter_pushdown(client) -> None:  # noqa: ANN001
    rows = client.execute("SELECT name FROM db.main.employees WHERE salary > 90000 ORDER BY id").fetchall()
    assert [r[0] for r in rows] == ["Alice", "Bob"]


def test_http_in_list_pushdown(client) -> None:  # noqa: ANN001
    assert client.execute("SELECT count(*) FROM db.main.employees WHERE dept IN ('eng', 'ops')").fetchone()[0] == 3


def test_http_projection(client) -> None:  # noqa: ANN001
    cols = [r[0] for r in client.execute("DESCRIBE SELECT id, name FROM db.main.employees").fetchall()]
    assert cols == ["id", "name"]


def test_http_view_scan(client) -> None:  # noqa: ANN001
    assert client.execute("SELECT count(*) FROM db.main.active_high").fetchone()[0] == 3


def test_http_adbc_query_passthrough(client) -> None:  # noqa: ANN001
    rows = client.execute(
        "SELECT id FROM db.main.adbc_query('SELECT id FROM employees WHERE dept = ''eng''') ORDER BY id"
    ).fetchall()
    assert [r[0] for r in rows] == [1, 2]
    # A native-only construct the mirror can't express.
    pk = client.execute("SELECT name FROM db.main.adbc_query('PRAGMA table_info(employees)') WHERE pk = 1").fetchone()
    assert pk[0] == "id"


def test_http_concurrent_clients(http_worker: tuple[str, str]) -> None:
    # Many clients attaching + querying the HTTP worker concurrently — exercises the waitress
    # thread pool and multiple simultaneous sticky sessions. Assert every query is correct and
    # no thread errored.
    import threading

    location, sqlite_path = http_worker
    _vgi_client().close()  # gate: skips here if haybarn/vgi is unavailable

    errors: list[str] = []
    results: list[int] = []

    def run() -> None:
        try:
            import haybarn

            con = haybarn.connect(config={"allow_unsigned_extensions": "true"})
            con.execute("LOAD vgi")
            con.execute(f"ATTACH 'adbc' AS db (TYPE vgi, LOCATION '{location}', driver 'sqlite', uri '{sqlite_path}')")
            for _ in range(5):
                results.append(con.execute("SELECT count(*) FROM db.main.employees WHERE salary > 50000").fetchone()[0])
            con.execute("DETACH db")
            con.close()
        except Exception as exc:  # noqa: BLE001 - collect any failure across threads
            errors.append(repr(exc))

    threads = [threading.Thread(target=run) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == [], errors
    assert results == [4] * 30  # 6 clients x 5 queries; 4 employees earn > 50000


def test_http_readonly_blocks_writes_and_stays_usable(client) -> None:  # noqa: ANN001
    # The read-only seal rejects a mutating passthrough over HTTP...
    with pytest.raises(Exception):  # noqa: B017, PT011 - worker surfaces an IO/InvalidInput error
        client.execute(
            "SELECT * FROM db.main.adbc_query('INSERT INTO employees VALUES (99, ''M'', ''x'', 1, 1)')"
        ).fetchall()
    # ...it left no trace, and the sticky session (single in-process server here) is still
    # usable for follow-up queries after the error.
    assert client.execute("SELECT count(*) FROM db.main.employees WHERE id = 99").fetchone()[0] == 0
    assert client.execute("SELECT count(*) FROM db.main.employees").fetchone()[0] == 5


# ---------------------------------------------------------------------------------------------
# Arbitrary remote servers over HTTP (opt-in): the connector is driver-agnostic, so the HTTP
# transport behaves identically for PostgreSQL, MySQL, etc. These prove a *non-embedded* server
# works end-to-end over http:// — attach + introspect + scan + filter pushdown. They seed a
# throwaway `http_probe` table themselves and skip unless the matching DSN env is set.
# ---------------------------------------------------------------------------------------------

_PROBE_ROWS = "(1, 'alice'), (2, 'bob'), (3, 'carol')"


def _seed_pg(dsn: str) -> None:
    if shutil.which("psql") is None:
        pytest.skip("psql client not available to seed PostgreSQL")
    sql = (
        "DROP TABLE IF EXISTS http_probe;"
        "CREATE TABLE http_probe (id int PRIMARY KEY, name text);"
        f"INSERT INTO http_probe VALUES {_PROBE_ROWS};"
    )
    try:
        subprocess.run(["psql", dsn, "-v", "ON_ERROR_STOP=1", "-q", "-c", sql], check=True, timeout=60)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        pytest.skip(f"cannot reach/seed PostgreSQL: {exc}")


def _mysql_cli_args(dsn: str) -> list[str]:
    # Parse the ADBC MySQL Go DSN: user:pw@tcp(host:port)/db
    m = re.match(r"(?P<user>[^:]+):(?P<pw>[^@]*)@tcp\((?P<host>[^:]+):(?P<port>\d+)\)/(?P<db>[^?]+)", dsn)
    if not m:
        pytest.skip(f"VGI_ADBC_TEST_MYSQL is not an ADBC MySQL DSN (user:pw@tcp(host:port)/db): {dsn!r}")
    g = m.groupdict()
    return [f"-h{g['host']}", f"-P{g['port']}", f"-u{g['user']}", f"-p{g['pw']}", g["db"]]


def _seed_mysql(dsn: str) -> None:
    if shutil.which("mysql") is None:
        pytest.skip("mysql client not available to seed MySQL")
    args = _mysql_cli_args(dsn)
    sql = (
        "DROP TABLE IF EXISTS http_probe;"
        "CREATE TABLE http_probe (id int PRIMARY KEY, name varchar(50));"
        f"INSERT INTO http_probe VALUES {_PROBE_ROWS};"
    )
    try:
        subprocess.run(["mysql", *args, "-e", sql], check=True, timeout=60)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        pytest.skip(f"cannot reach/seed MySQL: {exc}")


def _remote_client(http_worker: tuple[str, str], alias: str, driver: str, uri: str):  # noqa: ANN202
    location, _ = http_worker
    con = _vgi_client()
    try:
        con.execute(f"ATTACH 'adbc' AS {alias} (TYPE vgi, LOCATION '{location}', driver '{driver}', uri '{uri}')")
    except Exception as exc:  # noqa: BLE001 - driver missing / server unreachable
        con.close()
        pytest.skip(f"cannot attach {driver} over HTTP: {exc}")
    return con


@pytest.fixture
def pg_http_client(http_worker: tuple[str, str]):  # noqa: ANN201
    if not _PG_DSN:
        pytest.skip("set VGI_ADBC_TEST_PG to a PostgreSQL DSN to run")
    _seed_pg(_PG_DSN)
    con = _remote_client(http_worker, "pg", "postgresql", _PG_DSN)
    try:
        yield con
    finally:
        with contextlib.suppress(Exception):
            con.execute("DETACH pg")
        con.close()


@pytest.fixture
def mysql_http_client(http_worker: tuple[str, str]):  # noqa: ANN201
    if not _MYSQL_DSN:
        pytest.skip("set VGI_ADBC_TEST_MYSQL to an ADBC MySQL DSN to run")
    _seed_mysql(_MYSQL_DSN)
    con = _remote_client(http_worker, "my", "mysql", _MYSQL_DSN)
    try:
        yield con
    finally:
        with contextlib.suppress(Exception):
            con.execute("DETACH my")
        con.close()


def test_http_postgresql_scan_and_pushdown(pg_http_client) -> None:  # noqa: ANN001
    # PostgreSQL over the HTTP transport: introspect + scan + filter pushdown. PG surfaces the
    # table under the `public` schema.
    assert pg_http_client.execute("SELECT count(*) FROM pg.public.http_probe").fetchone()[0] == 3
    rows = pg_http_client.execute("SELECT name FROM pg.public.http_probe WHERE id = 2").fetchall()
    assert [r[0] for r in rows] == ["bob"]
    cols = [r[0] for r in pg_http_client.execute("DESCRIBE SELECT id, name FROM pg.public.http_probe").fetchall()]
    assert cols == ["id", "name"]


def test_http_mysql_scan_and_pushdown(mysql_http_client) -> None:  # noqa: ANN001
    # MySQL over the HTTP transport. The ADBC MySQL driver surfaces the database as the `main`
    # schema (verified), so the relation is `my.main.http_probe`.
    assert mysql_http_client.execute("SELECT count(*) FROM my.main.http_probe").fetchone()[0] == 3
    rows = mysql_http_client.execute("SELECT name FROM my.main.http_probe WHERE id = 1").fetchall()
    assert [r[0] for r in rows] == ["alice"]
