# Copyright 2026 Query Farm LLC - https://query.farm

"""Safe SQL construction helpers for the embedded DuckDB engine.

The worker builds the SQL it runs against its in-process ``haybarn`` DuckDB from
three untrusted-ish sources: catalog/schema/table identifiers discovered from the
remote database, ADBC connection options supplied at ``ATTACH`` time, and column
names referenced by pushdown filters. Every one of them flows through the helpers
here so identifiers are quoted (and quote-escaped) and string values are escaped —
never string-concatenated raw.

We speak *DuckDB* SQL against the attached ``remote`` catalog; the ``adbc_scanner``
extension is what re-dialects and pushes predicates down to PostgreSQL / SQLite /
etc. So DuckDB's double-quote identifier and single-quote string rules are the only
dialect we render here.
"""

from __future__ import annotations

import re

# ADBC driver option keys are rendered as bare SQL identifiers inside the
# ``ATTACH ... (TYPE adbc, <key> '<value>')`` option list, so they must be simple
# identifiers — reject anything that could break out of that position.
_SAFE_OPTION_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def quote_identifier(name: str) -> str:
    """Return ``name`` as a double-quoted DuckDB identifier with quotes escaped.

    Args:
        name: The identifier (catalog, schema, table, or column name).

    Returns:
        The identifier wrapped in double quotes, with embedded ``"`` doubled.

    Raises:
        ValueError: If ``name`` is empty or contains a NUL (which a downstream C string
            parser would silently truncate on, cutting the rendered SQL short).

    """
    if not name:
        msg = "empty SQL identifier"
        raise ValueError(msg)
    if "\x00" in name:
        msg = "SQL identifier contains a NUL character"
        raise ValueError(msg)
    return '"' + name.replace('"', '""') + '"'


def quote_string(value: str) -> str:
    """Return ``value`` as a single-quoted DuckDB string literal, quotes escaped.

    Args:
        value: The raw string value.

    Returns:
        The value wrapped in single quotes, with embedded ``'`` doubled.

    Raises:
        ValueError: If ``value`` contains a NUL (which would silently truncate the
            rendered SQL at the C-string boundary).

    """
    if "\x00" in value:
        msg = "SQL string literal contains a NUL character"
        raise ValueError(msg)
    return "'" + value.replace("'", "''") + "'"


def qualified_relation(catalog: str, schema: str, table: str) -> str:
    """Build a fully-qualified ``"catalog"."schema"."table"`` reference.

    Args:
        catalog: The (worker-local) catalog alias, e.g. ``remote``.
        schema: The schema name.
        table: The table or view name.

    Returns:
        The three-part, individually-quoted relation reference.

    """
    return f"{quote_identifier(catalog)}.{quote_identifier(schema)}.{quote_identifier(table)}"


def validate_option_key(key: str) -> str:
    """Validate that ``key`` is a safe ADBC option identifier and return it.

    Args:
        key: A driver option key supplied via ATTACH options.

    Returns:
        The key unchanged, once validated.

    Raises:
        ValueError: If the key is not a simple SQL identifier.

    """
    if not _SAFE_OPTION_KEY.match(key):
        msg = f"Unsafe ADBC option key: {key!r} (must match [A-Za-z_][A-Za-z0-9_]*)"
        raise ValueError(msg)
    return key


def projection_list(columns: list[str] | None) -> str:
    """Render a projected column list, or ``*`` when no projection is requested.

    Args:
        columns: The projected column names, in output order, or None for all.

    Returns:
        A comma-separated, individually-quoted column list, or ``*``.

    """
    if not columns:
        return "*"
    return ", ".join(quote_identifier(c) for c in columns)
