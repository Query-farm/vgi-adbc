#!/usr/bin/env python3
# Copyright 2026 Query Farm LLC - https://query.farm
"""Build the SQLite benchmark fixture (parametrized row count).

Usage: ``python bench/make_bench_fixture.py <path> [rows]`` (default 100000 rows).
Rebuilt from scratch each run so numbers are comparable. Uses only the standard library.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _data import DDL_SQLITE, TABLE, gen_rows  # noqa: E402


def build(path: Path, rows: int) -> None:
    """Create the benchmark SQLite database at ``path`` with ``rows`` rows."""
    if path.exists():
        path.unlink()
    con = sqlite3.connect(path)
    try:
        con.execute(DDL_SQLITE)
        con.executemany(f"INSERT INTO {TABLE} VALUES (?, ?, ?, ?, ?)", gen_rows(rows))
        con.execute(f"CREATE INDEX idx_{TABLE}_dept ON {TABLE}(dept)")
        con.commit()
    finally:
        con.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: make_bench_fixture.py <path-to-sqlite-file> [rows]", file=sys.stderr)
        raise SystemExit(2)
    out = Path(sys.argv[1])
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 100_000
    build(out, n)
    print(f"built {out} with {n} rows in table {TABLE!r}")
