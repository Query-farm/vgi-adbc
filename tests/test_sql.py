"""Unit tests for safe SQL construction (no engine, no network)."""

from __future__ import annotations

import pytest

from vgi_adbc.sql import (
    projection_list,
    qualified_relation,
    quote_identifier,
    quote_string,
    validate_option_key,
)


def test_quote_identifier_escapes_double_quotes() -> None:
    assert quote_identifier("events") == '"events"'
    assert quote_identifier('weird"name') == '"weird""name"'
    assert quote_identifier('a"b"c') == '"a""b""c"'


def test_quote_string_escapes_single_quotes() -> None:
    assert quote_string("hello") == "'hello'"
    assert quote_string("O'Brien") == "'O''Brien'"
    assert quote_string("'; DROP TABLE t; --") == "'''; DROP TABLE t; --'"


def test_qualified_relation_quotes_each_part() -> None:
    assert qualified_relation("remote", "public", "events") == '"remote"."public"."events"'
    assert qualified_relation("remote", "we ird", 'ta"ble') == '"remote"."we ird"."ta""ble"'


def test_projection_list() -> None:
    assert projection_list(None) == "*"
    assert projection_list([]) == "*"
    assert projection_list(["id", "name"]) == '"id", "name"'
    assert projection_list(['a"b']) == '"a""b"'


def test_validate_option_key_accepts_identifiers() -> None:
    for key in ("driver", "user_name", "_x", "Port2"):
        assert validate_option_key(key) == key


@pytest.mark.parametrize("bad", ["has space", "semi;colon", "quote'", "dash-key", "", "1leading"])
def test_validate_option_key_rejects_unsafe(bad: str) -> None:
    with pytest.raises(ValueError, match="Unsafe ADBC option key"):
        validate_option_key(bad)


def test_quote_identifier_rejects_empty_and_nul() -> None:
    with pytest.raises(ValueError, match="empty SQL identifier"):
        quote_identifier("")
    with pytest.raises(ValueError, match="NUL"):
        quote_identifier("a\x00b")


def test_quote_string_rejects_nul() -> None:
    with pytest.raises(ValueError, match="NUL"):
        quote_string("a\x00b")
    # An empty string literal is valid.
    assert quote_string("") == "''"
