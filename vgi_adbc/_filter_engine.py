# Copyright 2026 Query Farm LLC - https://query.farm

"""Point the vgi-python pushed-filter evaluator at stock DuckDB 1.5.5.

vgi-python binds every pushed filter predicate (``vgi.duckdb.standard.v1``) against an
in-process reference engine and accepts only DuckDB 1.5.5 or Haybarn 1.5.5-rc1. It
prefers ``haybarn`` when importable, but this worker's ADBC engine is haybarn 1.5.4:
the ``adbc_scanner`` build whose raw-handle API the ``adbc_query`` passthrough relies on
(the 1.5.5 build makes handles per-connection and needs explicit columns for SQLite
queries). So the filter oracle is served by the separately installed stock ``duckdb``
1.5.5 package instead, leaving haybarn to the ADBC engine. Imported first by
:mod:`vgi_adbc`, before any filter can be evaluated.
"""

from __future__ import annotations

import duckdb
from vgi import _duckdb as _vgi_engine

# vgi-python has no public engine-selection hook; its resolver caches this module.
_vgi_engine._engine = duckdb  # noqa: SLF001
