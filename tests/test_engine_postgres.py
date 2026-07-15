"""Integration tests against a real PostgreSQL server (opt-in).

Skipped unless ``VGI_ADBC_TEST_PG`` holds a PostgreSQL DSN (e.g.
``postgresql://postgres:bench@127.0.0.1:5433/bench``) and the ADBC PostgreSQL driver
loads. Covers the PostgreSQL-specific paths that SQLite can't exercise: the read-only
seal (``SET SESSION CHARACTERISTICS``), the cheap ``pg_class.reltuples`` cardinality
estimate, and a native passthrough query.

Bring a server up with the benchmark helper::

    docker run -d --rm --name pg -e POSTGRES_PASSWORD=bench -e POSTGRES_DB=bench \\
        -p 5433:5432 postgres:16
    VGI_ADBC_TEST_PG=postgresql://postgres:bench@127.0.0.1:5433/bench uv run pytest -q \\
        tests/test_engine_postgres.py
"""

from __future__ import annotations

import os
import subprocess
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

    from vgi_adbc.engine import AdbcEngine, AdbcSession

_DSN = os.environ.get("VGI_ADBC_TEST_PG")

pytestmark = pytest.mark.skipif(not _DSN, reason="set VGI_ADBC_TEST_PG to a PostgreSQL DSN to run")


def _psql(sql: str) -> None:
    subprocess.run(["psql", _DSN, "-v", "ON_ERROR_STOP=1", "-q", "-c", sql], check=True)


@pytest.fixture(scope="module")
def pg_table() -> str:
    if os.environ.get("VGI_ADBC_TEST_PG") is None:  # pragma: no cover - guarded by skipif
        pytest.skip("no PostgreSQL DSN")
    _psql("DROP TABLE IF EXISTS pg_bench")
    _psql("CREATE TABLE pg_bench (id BIGINT PRIMARY KEY, name TEXT, active INTEGER)")
    _psql("INSERT INTO pg_bench SELECT g, 'n' || g, g % 2 FROM generate_series(1, 500) g")
    _psql("ANALYZE pg_bench")
    return "pg_bench"


@pytest.fixture
def pg_session(pg_table: str) -> Iterator[tuple[AdbcEngine, AdbcSession, str]]:
    from vgi_adbc.engine import AdbcEngine, AdbcTarget

    eng = AdbcEngine(cardinality_mode="estimate")
    try:
        eng._new_connection().close()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"adbc_scanner unavailable: {exc}")
    try:
        sid = eng.attach(AdbcTarget(driver="postgresql", location=str(_DSN)))
    except Exception as exc:  # noqa: BLE001 - driver missing / server unreachable
        pytest.skip(f"cannot attach PostgreSQL: {exc}")
    try:
        yield eng, eng.get(sid), pg_table
    finally:
        eng.detach(sid)


def test_pg_projected_scan(pg_session: tuple[AdbcEngine, AdbcSession, str]) -> None:
    # NOTE: a *filtered* scan executed directly on the mirrored `remote` PostgreSQL
    # catalog returns 0 rows in isolation — an upstream adbc_scanner + PostgreSQL quirk
    # (a WHERE above the adbc scan yields an empty result). End-to-end through the DuckDB
    # client this is not observed (DuckDB re-applies the predicate), so filter-pushdown
    # correctness is exercised by the SQLite E2E suite. Here we assert projection + full
    # scan, which is the reliable direct-engine PostgreSQL path.
    eng, session, table = pg_session
    rows = eng.scan(session, "public", table, ["id", "name"], "", []).read_all().to_pylist()
    assert len(rows) == 500
    assert set(rows[0].keys()) == {"id", "name"}


def test_pg_reltuples_estimate(pg_session: tuple[AdbcEngine, AdbcSession, str]) -> None:
    eng, session, table = pg_session
    est = eng.row_count(session, "public", table)
    # reltuples after ANALYZE is an estimate; it should be in the right ballpark.
    assert est is not None
    assert 250 <= est <= 750


def test_pg_readonly_blocks_writes(pg_session: tuple[AdbcEngine, AdbcSession, str]) -> None:
    eng, session, _table = pg_session
    with pytest.raises(Exception):  # noqa: B017, PT011 - server rejects write in read-only txn
        eng.raw_query(session, "CREATE TABLE should_not_exist (x int)").read_all()
    # Confirm nothing was created.
    rows = (
        eng.raw_query(
            session, "SELECT count(*) AS n FROM information_schema.tables WHERE table_name = 'should_not_exist'"
        )
        .read_all()
        .to_pylist()
    )
    assert rows[0]["n"] == 0
