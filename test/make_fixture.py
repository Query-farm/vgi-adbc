#!/usr/bin/env python3
# Copyright 2026 Query Farm LLC - https://query.farm
"""Build the deterministic SQLite fixture the haybarn E2E suite attaches over ADBC.

Usage: ``python test/make_fixture.py <path-to-sqlite-file>``. Rebuilt from scratch
each run so the assertions are stable. Uses only the standard library.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

EMPLOYEES = [
    (1, "Alice", "eng", 120000.0, 1),
    (2, "Bob", "eng", 95000.0, 1),
    (3, "Carol", "sales", 80000.0, 0),
    (4, "Dave", "sales", 60000.0, 1),
    (5, "Eve", "ops", 45000.0, 1),
]


def build(path: Path) -> None:
    """Create the fixture database at ``path`` (overwriting any existing file)."""
    if path.exists():
        path.unlink()
    con = sqlite3.connect(path)
    try:
        con.execute(
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary REAL, active INTEGER)"
        )
        con.executemany("INSERT INTO employees VALUES (?, ?, ?, ?, ?)", EMPLOYEES)
        con.execute("CREATE VIEW active_high AS SELECT * FROM employees WHERE active = 1 AND salary > 50000")
        con.commit()
    finally:
        con.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: make_fixture.py <path-to-sqlite-file>", file=sys.stderr)
        raise SystemExit(2)
    build(Path(sys.argv[1]))
