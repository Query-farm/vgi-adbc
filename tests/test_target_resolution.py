"""Unit tests for ATTACH option resolution and the ATTACH SQL builder (no network)."""

from __future__ import annotations

import pytest

from vgi_adbc.catalog import CATALOG_NAME, _resolve_target
from vgi_adbc.engine import AdbcEngine, AdbcTarget


def test_requires_driver() -> None:
    with pytest.raises(ValueError, match="requires a 'driver'"):
        _resolve_target(CATALOG_NAME, {"uri": "/tmp/x.sqlite"})


def test_requires_connection_string() -> None:
    with pytest.raises(ValueError, match="requires a connection string"):
        _resolve_target(CATALOG_NAME, {"driver": "sqlite"})


def test_uri_from_option() -> None:
    target = _resolve_target(CATALOG_NAME, {"driver": "sqlite", "uri": "/tmp/x.sqlite"})
    assert target.driver == "sqlite"
    assert target.location == "/tmp/x.sqlite"
    assert target.options == {}
    assert target.read_only is True


def test_uri_falls_back_to_attach_name() -> None:
    # When the ATTACH name is the connection string itself (mirrors native adbc_scanner).
    target = _resolve_target("postgresql://u:p@h:5432/db", {"driver": "postgresql"})
    assert target.location == "postgresql://u:p@h:5432/db"


def test_catalog_name_is_not_used_as_connection_string() -> None:
    with pytest.raises(ValueError, match="requires a connection string"):
        _resolve_target(CATALOG_NAME, {"driver": "postgresql"})


def test_driver_options_forwarded_and_empties_dropped() -> None:
    target = _resolve_target(
        CATALOG_NAME,
        {
            "driver": "postgresql",
            "uri": "postgresql://h/db",
            "username": "alice",
            "password": "",  # blank default -> dropped
            "database": "db2",
        },
    )
    assert target.options == {"username": "alice", "database": "db2"}


@pytest.mark.parametrize(
    ("value", "expected"),
    [("true", True), ("false", False), ("0", False), ("no", False), ("1", True), (True, True), (False, False)],
)
def test_read_only_parsing(value: object, expected: bool) -> None:
    target = _resolve_target(CATALOG_NAME, {"driver": "sqlite", "uri": "/x", "read_only": value})
    assert target.read_only is expected


def test_attach_sql_is_quoted_and_escaped() -> None:
    engine = AdbcEngine()
    target = AdbcTarget(
        driver="postgresql",
        location="postgresql://h/db'; DROP",
        options={"username": "a'b"},
        read_only=True,
    )
    sql = engine._attach_sql(target)
    assert sql.startswith("ATTACH 'postgresql://h/db''; DROP' AS \"remote\" (")
    assert "driver 'postgresql'" in sql
    assert "username 'a''b'" in sql
    assert sql.rstrip().endswith("READ_ONLY)")


def test_attach_sql_rejects_unsafe_option_key() -> None:
    engine = AdbcEngine()
    target = AdbcTarget(driver="sqlite", location="/x", options={"bad key": "v"})
    with pytest.raises(ValueError, match="Unsafe ADBC option key"):
        engine._attach_sql(target)


def test_attach_identity_roundtrip() -> None:
    from vgi_adbc.engine import decode_attach_identity, encode_attach_identity

    sid = b"0123456789abcdef"  # 16 bytes
    target = AdbcTarget(
        driver="postgresql",
        location="postgresql://u:p@h/db",
        entrypoint="custom_entry",
        options={"username": "alice", "sslmode": "require"},
        read_only=True,
        refresh_seconds=30.0,
    )
    blob = encode_attach_identity(sid, target)
    got_sid, got_target = decode_attach_identity(blob)
    assert got_sid == sid
    assert got_target == target


def test_attach_identity_legacy_16byte_has_no_target() -> None:
    from vgi_adbc.engine import decode_attach_identity

    sid = b"\x01" * 16
    got_sid, got_target = decode_attach_identity(sid)
    assert got_sid == sid
    assert got_target is None


@pytest.mark.parametrize(
    ("driver", "expected"),
    [
        ("sqlite", "PRAGMA query_only = ON"),
        ("SQLite", "PRAGMA query_only = ON"),
        ("postgresql", "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"),
        ("postgres", "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"),
        ("/opt/adbc/libadbc_driver_sqlite.dylib", "PRAGMA query_only = ON"),
        ("mysterydb", None),
        ("mysql", None),
    ],
)
def test_readonly_seal_sql(driver: str, expected: str | None) -> None:
    from vgi_adbc.engine import _readonly_seal_sql

    assert _readonly_seal_sql(driver) == expected
