"""Integration tests: the real embedded engine + catalog against a SQLite fixture.

These skip when ``adbc_scanner`` can't be loaded (see the ``engine`` fixture).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pytest
from vgi.catalog import SchemaObjectType

from vgi_adbc.catalog import CATALOG_NAME, AdbcCatalog
from vgi_adbc.engine import SessionNotFoundError

if TYPE_CHECKING:
    from pathlib import Path

    from vgi_adbc.engine import AdbcEngine, AdbcSession


def test_engine_schemas_and_tables(sqlite_session: tuple[AdbcEngine, AdbcSession]) -> None:
    engine, session = sqlite_session
    assert engine.schemas(session) == ["main"]
    names = {name for name, _type in engine.tables(session, "main")}
    assert {"employees", "active_high"} <= names


def test_engine_arrow_schema_types(sqlite_session: tuple[AdbcEngine, AdbcSession]) -> None:
    engine, session = sqlite_session
    schema = engine.arrow_schema(session, "main", "employees")
    by_name = {f.name: f.type for f in schema}
    assert by_name["id"] == pa.int64()
    assert by_name["name"] == pa.string()
    assert by_name["salary"] == pa.float64()


def test_engine_schema_cache_hits(sqlite_session: tuple[AdbcEngine, AdbcSession]) -> None:
    engine, session = sqlite_session
    first = engine.arrow_schema(session, "main", "employees")
    assert ("main", "employees") in session.schema_cache
    assert engine.arrow_schema(session, "main", "employees") is first


def test_engine_projection_and_filter_pushdown(sqlite_session: tuple[AdbcEngine, AdbcSession]) -> None:
    engine, session = sqlite_session
    reader = engine.scan(session, "main", "employees", ["id", "name"], '"salary" > ?', [90000])
    rows = reader.read_all().to_pylist()
    assert sorted(r["name"] for r in rows) == ["Alice", "Bob"]
    assert set(rows[0].keys()) == {"id", "name"}


def test_engine_scan_view(sqlite_session: tuple[AdbcEngine, AdbcSession]) -> None:
    engine, session = sqlite_session
    reader = engine.scan(session, "main", "active_high", None, "", [])
    assert reader.read_all().num_rows == 3


def test_refresh_picks_up_new_table(engine: AdbcEngine, sqlite_db: Path) -> None:
    import sqlite3

    from vgi_adbc.engine import AdbcTarget

    session_id = engine.attach(AdbcTarget(driver="sqlite", location=str(sqlite_db)))
    session = engine.get(session_id)
    try:
        before = {name for name, _ in engine.tables(session, "main")}
        assert "newly_added" not in before
        v0 = session.catalog_version

        # Add a table to the SQLite file out-of-band; adbc_scanner snapshots at attach,
        # so it stays invisible until a refresh re-attaches.
        con = sqlite3.connect(sqlite_db)
        con.execute("CREATE TABLE newly_added (x INTEGER)")
        con.commit()
        con.close()
        assert "newly_added" not in {name for name, _ in engine.tables(session, "main")}

        new_version = engine.refresh(session)
        assert new_version > v0
        assert "newly_added" in {name for name, _ in engine.tables(session, "main")}

        # An idempotent refresh (nothing changed) does not bump the version.
        assert engine.refresh(session) == new_version
    finally:
        engine.detach(session_id)


def test_refresh_is_safe_against_concurrent_scans(engine: AdbcEngine, sqlite_db: Path) -> None:
    # Regression guard for the connection-swap refresh: DETACH-in-place used to break
    # in-flight scans with `Catalog "remote" does not exist`. The swap must let concurrent
    # scans keep streaming while a refresh re-attaches.
    import threading

    from vgi_adbc.engine import AdbcTarget

    session_id = engine.attach(AdbcTarget(driver="sqlite", location=str(sqlite_db)))
    session = engine.get(session_id)
    stop = False
    errors: list[str] = []

    def scanner() -> None:
        while not stop:
            try:
                engine.scan(session, "main", "employees", None, "", []).read_all()
            except Exception as exc:  # noqa: BLE001 - collect any failure
                errors.append(repr(exc))

    def refresher() -> None:
        for _ in range(15):
            engine.refresh(session)

    try:
        scanners = [threading.Thread(target=scanner) for _ in range(4)]
        for t in scanners:
            t.start()
        r = threading.Thread(target=refresher)
        r.start()
        r.join()
        stop = True
        for t in scanners:
            t.join()
        assert errors == []
    finally:
        engine.detach(session_id)


def test_maybe_refresh_respects_interval(engine: AdbcEngine, sqlite_db: Path) -> None:
    from vgi_adbc.engine import AdbcTarget

    # Interval 0 -> never auto-refreshes.
    session_id = engine.attach(AdbcTarget(driver="sqlite", location=str(sqlite_db), refresh_seconds=0.0))
    session = engine.get(session_id)
    try:
        v0 = session.catalog_version
        engine.maybe_refresh(session)
        assert session.catalog_version == v0  # disabled -> no refresh
    finally:
        engine.detach(session_id)


def test_row_count_for_cardinality(sqlite_session: tuple[AdbcEngine, AdbcSession]) -> None:
    engine, session = sqlite_session
    assert engine.row_count(session, "main", "employees") == 5
    # Cached.
    assert ("main", "employees") in session.count_cache
    assert engine.row_count(session, "main", "employees") == 5


def test_refresh_clears_count_cache(engine: AdbcEngine, sqlite_db: Path) -> None:
    import sqlite3

    from vgi_adbc.engine import AdbcTarget

    session_id = engine.attach(AdbcTarget(driver="sqlite", location=str(sqlite_db)))
    session = engine.get(session_id)
    try:
        assert engine.row_count(session, "main", "employees") == 5
        con = sqlite3.connect(sqlite_db)
        con.execute("INSERT INTO employees VALUES (6, 'Fred', 'ops', 50000, 1)")
        con.execute("CREATE TABLE newt (x INTEGER)")  # changes the fingerprint
        con.commit()
        con.close()
        engine.refresh(session)
        # Cache was cleared, so the fresh count reflects the insert.
        assert engine.row_count(session, "main", "employees") == 6
    finally:
        engine.detach(session_id)


def _fresh_engine(**kwargs: object) -> AdbcEngine:
    from vgi_adbc.engine import AdbcEngine

    eng = AdbcEngine(**kwargs)  # type: ignore[arg-type]
    try:
        eng._new_connection().close()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"adbc_scanner unavailable: {exc}")
    return eng


def test_cardinality_off_returns_none(sqlite_db: Path) -> None:
    from vgi_adbc.engine import AdbcTarget

    eng = _fresh_engine(cardinality_mode="off")
    sid = eng.attach(AdbcTarget(driver="sqlite", location=str(sqlite_db)))
    try:
        assert eng.row_count(eng.get(sid), "main", "employees") is None
    finally:
        eng.detach(sid)


def test_cardinality_count_mode(sqlite_db: Path) -> None:
    from vgi_adbc.engine import AdbcTarget

    eng = _fresh_engine(cardinality_mode="count")
    sid = eng.attach(AdbcTarget(driver="sqlite", location=str(sqlite_db)))
    try:
        assert eng.row_count(eng.get(sid), "main", "employees") == 5
    finally:
        eng.detach(sid)


def test_cardinality_bounded_count(sqlite_db: Path) -> None:
    from vgi_adbc.engine import AdbcTarget

    # Cap below the row count -> unknown-but-large -> None (no unbounded full count).
    eng = _fresh_engine(cardinality_mode="count", cardinality_max_count=3)
    sid = eng.attach(AdbcTarget(driver="sqlite", location=str(sqlite_db)))
    try:
        assert eng.row_count(eng.get(sid), "main", "employees") is None
    finally:
        eng.detach(sid)
    # Cap above the row count -> exact count returned.
    eng2 = _fresh_engine(cardinality_mode="count", cardinality_max_count=100)
    sid2 = eng2.attach(AdbcTarget(driver="sqlite", location=str(sqlite_db)))
    try:
        assert eng2.row_count(eng2.get(sid2), "main", "employees") == 5
    finally:
        eng2.detach(sid2)


def test_maybe_refresh_survives_remote_failure(engine: AdbcEngine, sqlite_db: Path, monkeypatch) -> None:  # noqa: ANN001
    import time as _time

    from vgi_adbc.engine import AdbcTarget

    sid = engine.attach(AdbcTarget(driver="sqlite", location=str(sqlite_db), refresh_seconds=60.0))
    session = engine.get(sid)
    try:
        v0 = session.catalog_version
        session.last_refresh = _time.monotonic() - 1000.0  # due for a refresh

        def _boom(*_a: object, **_k: object) -> object:
            msg = "remote down"
            raise RuntimeError(msg)

        monkeypatch.setattr(engine, "_new_connection", _boom)
        engine.maybe_refresh(session)  # must NOT raise
        assert session.catalog_version == v0  # still serving cached catalog

        # The clock advanced, so an immediate re-check does not retry (no refresh storm).
        calls = {"n": 0}
        real_refresh = engine.refresh

        def _counting(s: AdbcSession) -> int:
            calls["n"] += 1
            return real_refresh(s)

        monkeypatch.setattr(engine, "refresh", _counting)
        engine.maybe_refresh(session)
        assert calls["n"] == 0
    finally:
        engine.detach(sid)


def test_raw_query_passthrough(sqlite_session: tuple[AdbcEngine, AdbcSession]) -> None:
    engine, session = sqlite_session
    schema = engine.raw_query_schema(session, "SELECT id, name FROM employees WHERE dept = 'eng'")
    assert [f.name for f in schema] == ["id", "name"]
    rows = engine.raw_query(session, "SELECT id, name FROM employees WHERE dept = 'eng' ORDER BY id").read_all()
    assert rows.to_pylist() == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    # A native-only construct (SQLite PRAGMA) the mirror cannot express.
    pragma = engine.raw_query(session, "PRAGMA table_info(employees)").read_all().to_pylist()
    assert any(r["name"] == "id" and r["pk"] == 1 for r in pragma)
    assert session.query_handle is not None


def test_adbc_query_readonly_blocks_writes(engine: AdbcEngine, sqlite_db: Path) -> None:
    # Under a read-only attach (the v1 default), the raw passthrough handle is sealed
    # read-only, so a mutating statement must be rejected at the source and leave no trace.
    import sqlite3

    from vgi_adbc.engine import AdbcTarget

    session_id = engine.attach(AdbcTarget(driver="sqlite", location=str(sqlite_db), read_only=True))
    session = engine.get(session_id)
    try:
        with pytest.raises(Exception):  # noqa: B017, PT011 - driver raises an IO/InternalException
            engine.raw_query(session, "INSERT INTO employees VALUES (99, 'Mallory', 'x', 1, 1)").read_all()
    finally:
        engine.detach(session_id)
    con = sqlite3.connect(sqlite_db)
    try:
        assert con.execute("SELECT count(*) FROM employees WHERE id = 99").fetchone()[0] == 0
    finally:
        con.close()


def test_adbc_query_not_sealed_when_not_readonly(engine: AdbcEngine, sqlite_db: Path) -> None:
    # With read_only=false the handle is opened WITHOUT a read-only seal (writes are the
    # caller's explicit choice); reads still work and no gating error is raised.
    from vgi_adbc.engine import AdbcTarget

    session_id = engine.attach(AdbcTarget(driver="sqlite", location=str(sqlite_db), read_only=False))
    session = engine.get(session_id)
    try:
        rows = engine.raw_query(session, "SELECT count(*) AS n FROM employees").read_all().to_pylist()
        assert rows == [{"n": 5}]
    finally:
        engine.detach(session_id)


def test_adbc_query_gated_for_unsealable_driver_under_readonly(engine: AdbcEngine, sqlite_db: Path) -> None:
    # A driver with no known read-only seal must refuse adbc_query under a read-only attach
    # rather than silently permit writes. We simulate that by attaching sqlite but stubbing
    # the driver name to something with no seal entry.
    import dataclasses

    from vgi_adbc.engine import AdbcTarget

    session_id = engine.attach(AdbcTarget(driver="sqlite", location=str(sqlite_db), read_only=True))
    session = engine.get(session_id)
    session.target = dataclasses.replace(session.target, driver="mysterydb")
    try:
        with pytest.raises(ValueError, match="read-only"):
            engine.raw_query(session, "SELECT 1")
    finally:
        session.target = dataclasses.replace(session.target, driver="sqlite")
        engine.detach(session_id)


def test_raw_query_handle_released_on_detach(engine: AdbcEngine, sqlite_db: Path) -> None:
    from vgi_adbc.engine import AdbcTarget

    session_id = engine.attach(AdbcTarget(driver="sqlite", location=str(sqlite_db)))
    session = engine.get(session_id)
    engine.raw_query(session, "SELECT 1").read_all()
    assert session.query_handle is not None
    engine.detach(session_id)
    assert session.query_handle is None


def test_close_all_drains_sessions(engine: AdbcEngine, sqlite_db: Path) -> None:
    from vgi_adbc.engine import AdbcTarget

    sids = [engine.attach(AdbcTarget(driver="sqlite", location=str(sqlite_db))) for _ in range(3)]
    assert len(engine._sessions) == 3
    engine.close_all()
    assert len(engine._sessions) == 0
    # Idempotent.
    engine.close_all()
    # The closed session ids are gone.
    from vgi_adbc.engine import SessionNotFoundError

    for sid in sids:
        try:
            engine.get(sid)
        except SessionNotFoundError:
            pass
        else:  # pragma: no cover
            raise AssertionError("session should be gone after close_all")


def test_session_idle_eviction(sqlite_db: Path) -> None:
    from vgi_adbc.engine import AdbcEngine, AdbcTarget, SessionNotFoundError

    eng = AdbcEngine(session_idle_seconds=100.0, reap_interval_seconds=0.0)
    try:
        eng._new_connection().close()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"adbc_scanner unavailable: {exc}")
    sid = eng.attach(AdbcTarget(driver="sqlite", location=str(sqlite_db)))
    session = eng.get(sid)
    # Backdate activity beyond the idle window, then force a reap.
    import time as _time

    session.last_used = _time.monotonic() - 1000.0
    eng.reap(force=True)
    with pytest.raises(SessionNotFoundError):
        eng.get(sid)


def test_max_sessions_rejected(sqlite_db: Path) -> None:
    from vgi_adbc.engine import AdbcEngine, AdbcTarget, SessionLimitError

    eng = AdbcEngine(max_sessions=2)
    try:
        eng._new_connection().close()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"adbc_scanner unavailable: {exc}")
    sids = [eng.attach(AdbcTarget(driver="sqlite", location=str(sqlite_db))) for _ in range(2)]
    try:
        with pytest.raises(SessionLimitError, match="session limit"):
            eng.attach(AdbcTarget(driver="sqlite", location=str(sqlite_db)))
    finally:
        for sid in sids:
            eng.detach(sid)


def test_detach_releases_session_scans(engine: AdbcEngine, sqlite_db: Path) -> None:
    # detach must release the session's in-flight scan readers (they pin the connection)
    # before closing it. We register a fake reader for the session and assert it's dropped.
    import vgi_adbc.functions as fn
    from vgi_adbc.engine import AdbcTarget

    class _FakeReader:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    session_id = engine.attach(AdbcTarget(driver="sqlite", location=str(sqlite_db)))
    reader = _FakeReader()
    fn._register_reader(b"scan-token", reader, session_id)
    assert fn.active_scan_count() == 1
    engine.detach(session_id)
    assert fn.active_scan_count() == 0
    assert reader.closed is True


def test_resolve_is_strictly_sticky(engine: AdbcEngine, sqlite_db: Path) -> None:
    # Sessions are strictly sticky: if the session is gone from this process (a worker-pool
    # hop or restart), resolve() raises rather than silently rebuilding a fresh connection
    # (which would lose transactions / temp tables). The client must re-ATTACH.
    from vgi_adbc.engine import AdbcTarget, SessionNotFoundError

    session_id = engine.attach(AdbcTarget(driver="sqlite", location=str(sqlite_db)))
    assert engine.resolve(session_id).session_id == session_id  # present -> resolves
    engine._sessions.pop(session_id)  # simulate a fresh worker process
    with pytest.raises(SessionNotFoundError, match="not held by this worker"):
        engine.resolve(session_id)


def test_missing_session_raises(engine: AdbcEngine) -> None:
    try:
        engine.get(b"\x00" * 16)
    except SessionNotFoundError as exc:
        assert "not held by this worker" in str(exc).lower()
    else:  # pragma: no cover
        raise AssertionError("expected SessionNotFoundError")


def test_catalog_attach_introspect_route(engine: AdbcEngine, sqlite_db: Path, monkeypatch) -> None:  # noqa: ANN001
    # Point the catalog's module-level engine accessor at our fresh engine.
    import vgi_adbc.engine as engine_mod

    monkeypatch.setattr(engine_mod, "_engine", engine)

    catalog = AdbcCatalog()
    result = catalog.catalog_attach(name=CATALOG_NAME, options={"driver": "sqlite", "uri": str(sqlite_db)})
    aod = result.attach_opaque_data
    assert result.default_schema == "main"

    schemas = {s.name for s in catalog.schemas(attach_opaque_data=aod, transaction_opaque_data=None)}
    assert "main" in schemas

    tables = catalog.schema_contents(
        attach_opaque_data=aod, transaction_opaque_data=None, name="main", type=SchemaObjectType.TABLE
    )
    assert {t.name for t in tables} >= {"employees", "active_high"}

    info = catalog.table_get(attach_opaque_data=aod, transaction_opaque_data=None, schema_name="main", name="employees")
    assert info is not None
    scan_schema = pa.ipc.read_schema(pa.py_buffer(info.columns))
    assert scan_schema.field("name").type == pa.string()

    routing = catalog.table_scan_function_get(
        attach_opaque_data=aod,
        transaction_opaque_data=None,
        schema_name="main",
        name="employees",
        at_unit=None,
        at_value=None,
    )
    assert routing.function_name == "adbc_scan"
    assert [s.as_py() for s in routing.positional_arguments] == ["main", "employees"]

    catalog.catalog_detach(attach_opaque_data=aod)
