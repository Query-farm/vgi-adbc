#!/usr/bin/env python3
# Copyright 2026 Query Farm LLC - https://query.farm
"""Smoke test: attach a throwaway SQLite database over ADBC and scan it.

Run in the Docker build (and usable in CI) to fail fast if the baked ADBC driver +
``adbc_scanner`` extension can't actually ATTACH and scan — rather than deferring the
failure to the first user's ATTACH. Exits non-zero on any problem.
"""

from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path

from vgi_adbc.engine import AdbcEngine, AdbcTarget


def main() -> int:
    db = Path(tempfile.mktemp(suffix=".sqlite"))
    con = sqlite3.connect(db)
    try:
        con.execute("CREATE TABLE smoke (id INTEGER PRIMARY KEY, name TEXT)")
        con.executemany("INSERT INTO smoke VALUES (?, ?)", [(1, "a"), (2, "b"), (3, "c")])
        con.commit()
    finally:
        con.close()

    engine = AdbcEngine()
    try:
        session_id = engine.attach(AdbcTarget(driver="sqlite", location=str(db)))
        session = engine.get(session_id)
        rows = engine.scan(session, "main", "smoke", ["id", "name"], '"id" >= ?', [2]).read_all().num_rows
        if rows != 2:
            print(f"attach smoke FAILED: expected 2 rows, got {rows}", file=sys.stderr)
            return 1
        engine.detach(session_id)
    finally:
        engine.close_all()
        db.unlink(missing_ok=True)
    print("attach smoke OK (attach + projection + filter pushdown over ADBC)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
