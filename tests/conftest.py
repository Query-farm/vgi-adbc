"""Shared pytest fixtures.

Pure-logic tests (SQL quoting, option parsing, batch alignment) run everywhere.
Tests that exercise the real embedded engine need the ``adbc_scanner`` extension
(installed once from the community repository); they skip gracefully when it can't
be loaded (e.g. an offline machine that has never installed it).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

    from vgi_adbc.engine import AdbcEngine, AdbcSession

_EMPLOYEES = [
    (1, "Alice", "eng", 120000.0, 1),
    (2, "Bob", "eng", 95000.0, 1),
    (3, "Carol", "sales", 80000.0, 0),
    (4, "Dave", "sales", 60000.0, 1),
    (5, "Eve", "ops", 45000.0, 1),
]


@pytest.fixture
def sqlite_db(tmp_path: Path) -> Path:
    """Create a small SQLite fixture database and return its path."""
    path = tmp_path / "fixture.sqlite"
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
    return path


@pytest.fixture
def engine() -> AdbcEngine:
    """A fresh engine, or skip if the adbc_scanner extension can't be loaded."""
    from vgi_adbc.engine import AdbcEngine

    eng = AdbcEngine()
    try:
        eng._new_connection().close()
    except Exception as exc:  # noqa: BLE001 - offline / extension unavailable
        pytest.skip(f"adbc_scanner unavailable: {exc}")
    return eng


@pytest.fixture
def sqlite_session(engine: AdbcEngine, sqlite_db: Path) -> Iterator[tuple[AdbcEngine, AdbcSession]]:
    """A live engine session attached to the SQLite fixture over ADBC."""
    from vgi_adbc.engine import AdbcTarget

    session_id = engine.attach(AdbcTarget(driver="sqlite", location=str(sqlite_db)))
    try:
        yield engine, engine.get(session_id)
    finally:
        engine.detach(session_id)
