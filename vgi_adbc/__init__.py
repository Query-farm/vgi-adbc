# Copyright 2026 Query Farm LLC - https://query.farm

"""vgi-adbc — query PostgreSQL, SQLite, and any ADBC database from DuckDB over VGI.

The worker holds native ADBC connections server-side (an embedded ``haybarn`` DuckDB
with the ``adbc_scanner`` community extension) and streams Apache Arrow back to the
DuckDB client, so even clients that cannot load native ADBC drivers — DuckDB-WASM in
the browser, for example — can ``ATTACH`` and query a remote database with pushdown.
"""

from __future__ import annotations

import vgi_adbc._filter_engine  # noqa: F401  (must precede any filter evaluation)
from vgi_adbc.catalog import CATALOG_NAME, AdbcCatalog
from vgi_adbc.engine import AdbcEngine, AdbcTarget, SessionNotFoundError, get_engine
from vgi_adbc.functions import ADBC_QUERY_FUNCTION_NAME, ADBC_SCAN_FUNCTION_NAME, AdbcQuery, AdbcScan
from vgi_adbc.worker import AdbcWorker, main

__all__ = [
    "ADBC_QUERY_FUNCTION_NAME",
    "ADBC_SCAN_FUNCTION_NAME",
    "CATALOG_NAME",
    "AdbcCatalog",
    "AdbcEngine",
    "AdbcQuery",
    "AdbcScan",
    "AdbcTarget",
    "AdbcWorker",
    "SessionNotFoundError",
    "get_engine",
    "main",
]
