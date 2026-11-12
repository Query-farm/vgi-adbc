# Copyright 2026 Query Farm LLC - https://query.farm

"""The VGI worker process hosting the ADBC catalog.

Run over stdio (a DuckDB client spawns it) or over HTTP (``vgi-serve`` — see the
container entrypoint). Exposes the :class:`~vgi_adbc.catalog.AdbcCatalog` catalog
interface and the single :class:`~vgi_adbc.functions.AdbcScan` table function that
backs every remote table scan.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Annotated

from vgi.catalog.attach_option import AttachOption
from vgi.worker import Worker

from vgi_adbc.catalog import CATALOG_NAME, AdbcCatalog
from vgi_adbc.functions import ADBC_QUERY_FUNCTION_NAME, ADBC_SCAN_FUNCTION_NAME, AdbcQuery, AdbcScan

if TYPE_CHECKING:
    from vgi.function import Function


class AdbcWorker(Worker):
    """Worker exposing the ADBC connector catalog."""

    catalog_interface = AdbcCatalog
    catalog_name = CATALOG_NAME
    functions = [AdbcScan, AdbcQuery]

    class AttachOptions:
        """Options accepted on ``ATTACH ... (TYPE vgi, ...)``.

        ``driver`` and ``uri`` are the essentials; the rest are forwarded to the
        ADBC driver when set. Empty values are treated as "not supplied".
        """

        driver: Annotated[
            str,
            AttachOption(desc="ADBC driver name or path to a driver library / manifest (e.g. 'postgresql', 'sqlite')."),
        ] = ""
        uri: Annotated[
            str,
            AttachOption(desc="ADBC connection string (e.g. 'postgresql://user:pw@host:5432/db' or a SQLite path)."),
        ] = ""
        username: Annotated[str, AttachOption(desc="Database username, when the driver requires one.")] = ""
        password: Annotated[str, AttachOption(desc="Database password, when the driver requires one.")] = ""
        database: Annotated[str, AttachOption(desc="Database name for drivers that take it as a separate option.")] = ""
        entrypoint: Annotated[str, AttachOption(desc="Custom ADBC driver entry-point symbol.")] = ""
        search_paths: Annotated[
            str,
            AttachOption(desc="Extra directories to search for ADBC driver manifests (path-separator delimited)."),
        ] = ""
        read_only: Annotated[
            bool,
            AttachOption(desc="Attach the remote database read-only (v1 is read-only regardless)."),
        ] = True
        catalog_refresh_seconds: Annotated[
            float,
            AttachOption(
                desc="Re-read the remote catalog at most this often so remote DDL becomes visible; 0 disables."
            ),
        ] = 0.0

    def _candidates_for(self, function_name: str, schema_path: list[str] | None) -> list[type[Function]]:
        """Resolve ``adbc_scan`` / ``adbc_query`` in whichever remote schema called them.

        The catalog lists both functions under every remote schema (``public``, ...), which
        are discovered at attach time, so DuckDB dispatches them schema-qualified. The SDK
        registry only knows the worker-level registration under ``main``; these two
        functions are schema-agnostic (the schema arrives as an argument / attach state),
        so they are resolved unscoped instead of failing an exact-schema lookup.

        Args:
            function_name: The function DuckDB is calling.
            schema_path: The schema the call was qualified with, if any.

        Returns:
            The candidate function classes.

        """
        if function_name in _SCHEMA_AGNOSTIC_FUNCTIONS:
            schema_path = None
        return super()._candidates_for(function_name, schema_path)


_SCHEMA_AGNOSTIC_FUNCTIONS = frozenset({ADBC_SCAN_FUNCTION_NAME, ADBC_QUERY_FUNCTION_NAME})


def _install_sigterm_drain() -> None:
    """Drain ADBC connections on SIGTERM (containers stop with SIGTERM, not a clean exit).

    ``atexit`` (which closes the engine's connections before the ``adbc_scanner`` static
    destructors run) fires on a normal interpreter exit but not on a bare SIGTERM. This
    handler turns SIGTERM into a clean ``SystemExit`` so that teardown runs; it also closes
    the engine directly (idempotent) as belt-and-suspenders. Only installable from the main
    thread, so it is a no-op elsewhere.
    """
    import signal

    def _graceful(_signum: int, _frame: object) -> None:
        from vgi_adbc.engine import get_engine

        get_engine().close_all()
        raise SystemExit(0)

    with contextlib.suppress(ValueError):  # signal only works in the main thread
        signal.signal(signal.SIGTERM, _graceful)


def main() -> None:
    """Run the ADBC worker process (Arrow IPC over stdin/stdout)."""
    _install_sigterm_drain()
    AdbcWorker.main()


if __name__ == "__main__":
    main()
