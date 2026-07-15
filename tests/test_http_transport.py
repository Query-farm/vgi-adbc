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

    found = which("vgi-serve")
    if found:
        return found
    candidate = REPO_ROOT / ".venv" / "bin" / "vgi-serve"
    return str(candidate) if candidate.exists() else None


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


@pytest.fixture
def client(http_worker: tuple[str, str]):  # noqa: ANN201 - haybarn connection
    """A DuckDB (haybarn) client attached to the HTTP worker; detaches on teardown."""
    location, sqlite_path = http_worker
    try:
        import haybarn
    except ImportError:
        pytest.skip("haybarn not available")
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


def test_http_readonly_blocks_writes_and_stays_usable(client) -> None:  # noqa: ANN001
    # The read-only seal rejects a mutating passthrough over HTTP...
    with pytest.raises(Exception):  # noqa: B017, PT011 - worker surfaces an IO/InvalidInput error
        client.execute(
            "SELECT * FROM db.main.adbc_query('INSERT INTO employees VALUES (99, ''M'', ''x'', 1, 1)')"
        ).fetchall()
    # ...it left no trace, and the attachment is still usable afterwards (resilient to the error).
    assert client.execute("SELECT count(*) FROM db.main.employees WHERE id = 99").fetchone()[0] == 0
    assert client.execute("SELECT count(*) FROM db.main.employees").fetchone()[0] == 5
