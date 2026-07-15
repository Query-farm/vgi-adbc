# Copyright 2026 Query Farm LLC - https://query.farm

"""Shared, deterministic data generation for the vgi-adbc benchmarks.

The benchmark table mirrors the ``employees`` shape used by the test fixture, scaled
up to an arbitrary row count so scans, projection/filter pushdown, cardinality, and the
``adbc_query`` passthrough can be measured. The generator is seeded so SQLite and
PostgreSQL are loaded with identical data and results are reproducible run to run.
"""

from __future__ import annotations

import random
from collections.abc import Iterator

TABLE = "bench"
DEPARTMENTS = ("eng", "sales", "ops", "finance", "support", "legal")

# Column order for the CSV/COPY path and the CREATE TABLE statements below.
COLUMNS = ("id", "name", "dept", "salary", "active")

DDL_SQLITE = f"CREATE TABLE {TABLE} (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary REAL, active INTEGER)"
DDL_PG = f"CREATE TABLE {TABLE} (id BIGINT PRIMARY KEY, name TEXT, dept TEXT, salary DOUBLE PRECISION, active INTEGER)"


def gen_rows(n: int, *, seed: int = 1234, name_width: int = 16) -> Iterator[tuple[int, str, str, float, int]]:
    """Yield ``n`` deterministic ``(id, name, dept, salary, active)`` rows.

    Args:
        n: Number of rows to generate (ids are ``1..n``).
        seed: PRNG seed so runs are reproducible.
        name_width: Minimum width of the ``name`` string (padded), to control row size.

    Yields:
        One tuple per row.

    """
    rng = random.Random(seed)
    for i in range(1, n + 1):
        dept = DEPARTMENTS[i % len(DEPARTMENTS)]
        salary = round(rng.uniform(40_000, 200_000), 2)
        active = 1 if rng.random() < 0.7 else 0
        name = f"emp{i:0{max(1, name_width - 3)}d}"
        yield (i, name, dept, salary, active)
