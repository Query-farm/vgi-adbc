# Copyright 2026 Query Farm LLC - https://query.farm

"""Authored catalog + schema metadata (the ``vgi.*`` tags the lint gate scores).

This connector *mirrors a remote database*, so the per-table and per-column
metadata belongs to whatever database a user attaches — it is not authorable here
(the lint config exempts those rules). What we can and do document richly is the
connector itself: the catalog and its default schema. Example SQL uses
``duckdb_tables()`` scoped to the attached catalog (portable across any attached
database) plus a couple of queries against a demonstrative ``employees`` table (the
shape used by the test fixture).

JSON-valued tags (keywords / categories / example_queries / executable_examples /
agent_test_tasks) are JSON strings; every example is catalog-qualified. Agent test tasks
carry only ``name``/``prompt`` (grader fields are private and never advertised).
"""

from __future__ import annotations

import json

_REPO = "https://github.com/Query-farm/vgi-adbc"
_ISSUES = f"{_REPO}/issues"

CATALOG_COMMENT = (
    "Query PostgreSQL, SQLite, and any other ADBC database from DuckDB over VGI. The worker holds the "
    "native ADBC connection server-side and streams Arrow back, with projection and filter pushdown to "
    "the source, so clients that cannot load native ADBC drivers (including DuckDB-WASM) can still ATTACH "
    "and query a remote database. Tables and views are discovered live from the attached database."
)

SCHEMA_COMMENT = (
    "A schema of the attached remote database. Its tables and views are discovered live and scanned "
    "through ADBC with projection and filter pushdown to the source."
)

CATALOG_TAGS: dict[str, str] = {
    "vgi.title": "ADBC Database Connector",
    "vgi.subtitle": "Query PostgreSQL, SQLite, and other ADBC databases from DuckDB with pushdown to the source.",
    "vgi.doc_llm": (
        "Attach a remote ADBC database (PostgreSQL, SQLite, and others) and query it as if it were "
        "local, with WHERE clauses and column projection pushed down to the source. Attach with "
        "ATTACH 'adbc' AS <alias> (TYPE vgi, LOCATION <worker>, driver '<name>', uri '<connection>'), "
        "then query <alias>.<schema>.<table>. The tables and views come from the attached database, not "
        "from this worker; use duckdb_tables() or information_schema to discover what a given database "
        "exposes. Read-only."
    ),
    "vgi.doc_md": (
        "## ADBC Database Connector\n\n"
        "Attach a remote [ADBC](https://arrow.apache.org/adbc/) database over VGI and query it with "
        "ordinary SQL. The worker runs the query through an embedded DuckDB with the `adbc_scanner` "
        "extension and streams Arrow back, pushing projection and filters down to the source.\n\n"
        "Attach with the `driver` (e.g. `postgresql`, `sqlite`) and `uri` options; then query the "
        "attached catalog as `<alias>.<schema>.<table>`.\n\n"
        "Tables and views are **discovered live** from the attached database — this connector does not "
        "define them. The connection is held server-side, so clients that cannot load native ADBC "
        "drivers (including DuckDB-WASM) can use it too. Read-only."
    ),
    "vgi.keywords": json.dumps(
        [
            "ADBC",
            "database",
            "PostgreSQL",
            "SQLite",
            "federation",
            "connector",
            "Arrow",
            "pushdown",
            "query",
        ]
    ),
    "vgi.author": "Query Farm LLC",
    "vgi.copyright": "Copyright 2026 Query Farm LLC",
    "vgi.license": "MIT",
    "vgi.support_contact": _ISSUES,
    "vgi.support_policy_url": _ISSUES,
    "vgi.executable_examples": json.dumps(
        [
            {
                "name": "list_tables",
                "description": "List the tables and views the attached database exposes",
                "sql": "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'adbc' ORDER BY 1, 2",
            },
            {
                "name": "list_schemas",
                "description": "List the schemas of the attached database",
                "sql": "SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'adbc' ORDER BY 1",
            },
            {
                "name": "count_relations",
                "description": "Count the tables and views the attached database exposes",
                "sql": "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'adbc'",
            },
        ]
    ),
    "vgi.example_queries": json.dumps(
        [
            {
                "description": "Discover the tables and views in the attached database",
                "sql": "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'adbc'",
            },
            {
                "description": "Count the rows of a table (filter pushed down to the source)",
                "sql": "SELECT count(*) FROM adbc.main.employees WHERE active = 1",
            },
            {
                "description": "Project and filter a remote table",
                "sql": "SELECT id, name FROM adbc.main.employees WHERE salary > 90000 ORDER BY id",
            },
        ]
    ),
    "vgi.agent_test_tasks": json.dumps(
        [
            {
                "name": "discover_tables",
                "prompt": "What tables does the attached ADBC database expose?",
            },
            {
                "name": "count_rows",
                "prompt": "How many rows are in the employees table of the attached database?",
            },
            {
                "name": "filter_pushdown",
                "prompt": "List the employees earning more than 90000, showing id and name.",
            },
        ]
    ),
}

SCHEMA_TAGS: dict[str, str] = {
    "vgi.title": "Attached Database Schema",
    "vgi.doc_llm": (
        "A schema of the attached remote database. Its tables and views are discovered live from the "
        "source and scanned through ADBC. Query <alias>.<schema>.<table>; WHERE clauses and the selected "
        "column list are pushed down to the source database."
    ),
    "vgi.doc_md": (
        "## Attached database schema\n\n"
        "The tables and views in this schema are **discovered live** from the attached database. This "
        "connector does not define them — they are whatever the remote database contains.\n\n"
        "Each relation is scanned through ADBC with projection and filter pushdown: the columns you "
        "select and the `WHERE` predicates you write are translated and sent to the source database, so "
        "only the rows and columns you ask for cross the wire.\n\n"
        "Discover what a schema exposes with `duckdb_tables()` or `information_schema` scoped to the "
        "attached catalog, then query `<alias>.<schema>.<table>` directly."
    ),
    "vgi.keywords": json.dumps(["schema", "tables", "views", "remote", "ADBC", "pushdown"]),
    "domain": "database",
    "vgi.example_queries": json.dumps(
        [
            {
                "description": "List this schema's relations",
                "sql": "SELECT table_name FROM duckdb_tables() WHERE database_name = 'adbc' AND schema_name = 'main'",
            },
            {
                "description": "Scan a remote table with a pushed-down filter",
                "sql": "SELECT * FROM adbc.main.employees WHERE dept = 'eng'",
            },
            {
                "description": "Count rows of a remote table",
                "sql": "SELECT count(*) FROM adbc.main.employees",
            },
        ]
    ),
}
