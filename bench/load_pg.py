#!/usr/bin/env python3
# Copyright 2026 Query Farm LLC - https://query.farm
"""Load the benchmark table into a PostgreSQL database via ``psql`` (no Python PG dep).

Usage: ``python bench/load_pg.py <dsn> [rows]``. Writes a CSV and streams it in with
``\\copy``. Exits non-zero (with a clear message) if ``psql`` is missing or the server
is unreachable — the benchmark wrapper treats that as "skip PostgreSQL".
"""

from __future__ import annotations

import csv
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _data import DDL_PG, TABLE, gen_rows  # noqa: E402


def _psql(dsn: str, sql: str) -> None:
    subprocess.run(
        ["psql", dsn, "-v", "ON_ERROR_STOP=1", "-q", "-c", sql],
        check=True,
    )


def load(dsn: str, rows: int) -> None:
    """Create + populate the benchmark table on the PostgreSQL server at ``dsn``."""
    if shutil.which("psql") is None:
        print("psql not found on PATH; cannot load PostgreSQL benchmark", file=sys.stderr)
        raise SystemExit(3)

    # Fail fast (and clearly) if the server is unreachable.
    try:
        subprocess.run(["psql", dsn, "-v", "ON_ERROR_STOP=1", "-q", "-c", "SELECT 1"], check=True)
    except subprocess.CalledProcessError as exc:
        print(f"cannot reach PostgreSQL at the given DSN: {exc}", file=sys.stderr)
        raise SystemExit(3) from exc

    _psql(dsn, f"DROP TABLE IF EXISTS {TABLE}")
    _psql(dsn, DDL_PG)

    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, newline="") as fh:
        csv_path = Path(fh.name)
        writer = csv.writer(fh)
        writer.writerows(gen_rows(rows))
    try:
        subprocess.run(
            ["psql", dsn, "-v", "ON_ERROR_STOP=1", "-q", "-c", f"\\copy {TABLE} FROM '{csv_path}' WITH (FORMAT csv)"],
            check=True,
        )
        _psql(dsn, f"CREATE INDEX idx_{TABLE}_dept ON {TABLE}(dept)")
        _psql(dsn, f"ANALYZE {TABLE}")
    finally:
        csv_path.unlink(missing_ok=True)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: load_pg.py <dsn> [rows]", file=sys.stderr)
        raise SystemExit(2)
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 100_000
    load(sys.argv[1], n)
    print(f"loaded {n} rows into PostgreSQL table {TABLE!r}")
