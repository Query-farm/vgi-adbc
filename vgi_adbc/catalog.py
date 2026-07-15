# Copyright 2026 Query Farm LLC - https://query.farm

"""The ADBC catalog interface — ATTACH a remote database, introspect it, scan it.

A DuckDB client attaches a remote ADBC database over VGI like this::

    ATTACH 'adbc' AS pg (TYPE vgi, LOCATION '<worker>',
                         driver 'postgresql', uri 'postgresql://user:pw@host/db');
    SELECT * FROM pg.public.events WHERE id > 100;   -- pushed down to PostgreSQL

``catalog_attach`` opens the connection in *this* worker process (via the embedded
DuckDB + ``adbc_scanner``), stores it as a sticky session, and returns the session
id as the catalog's ``attach_opaque_data``. Introspection methods mirror the remote
catalog's schemas and relations (tables *and* views, all scannable). Every relation
routes through the single ``adbc_scan`` table function.

Read-only (v1): all DDL raises ``CatalogReadOnlyError`` via
:class:`ReadOnlyCatalogInterface`.
"""

from __future__ import annotations

import contextlib
import logging
import uuid
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from vgi.catalog import (
    AttachOpaqueData,
    CatalogAttachResult,
    CatalogInfo,
    ReadOnlyCatalogInterface,
    ScanFunctionResult,
    SchemaInfo,
    SchemaObjectType,
    SerializedSchema,
    TableInfo,
    TransactionOpaqueData,
)

from vgi_adbc.engine import AdbcTarget, get_engine
from vgi_adbc.functions import ADBC_SCAN_FUNCTION_NAME, AdbcQuery, AdbcScan
from vgi_adbc.metadata import CATALOG_COMMENT, CATALOG_TAGS, SCHEMA_COMMENT, SCHEMA_TAGS

if TYPE_CHECKING:
    from collections.abc import Sequence

    from vgi.catalog.catalog_interface import FunctionInfo, IndexInfo, MacroInfo, ViewInfo
    from vgi_rpc.rpc import CallContext

logger = logging.getLogger("vgi_adbc.catalog")

# The discovery name of this connector catalog. Users write
# ``ATTACH 'adbc' AS <alias> (...)``; the real connection lives in the options.
CATALOG_NAME = "adbc"
SOURCE_URL = "https://github.com/Query-farm/vgi-adbc"

# HTTP session-affinity cookie. Set at attach; asserted on follow-ups so a
# mis-routed request fails loudly instead of hitting a process with no live
# connection. Ignored by subprocess transport.
STICKY_COOKIE_NAME = "vgi_adbc_sticky"

# Options consumed by the connector itself (everything else is a driver option).
_RESERVED_OPTIONS = frozenset({"driver", "uri", "location", "entrypoint", "read_only", "catalog_refresh_seconds"})

# ATTACH option keys the framework injects that are not driver options.
_FRAMEWORK_OPTIONS = frozenset({"type", "vgi_data_version", "vgi_implementation_version"})


def _resolve_target(name: str, options: dict[str, Any]) -> AdbcTarget:
    """Build an :class:`AdbcTarget` from the ATTACH name + options.

    The connection string is taken from the ``uri`` (or ``location``) option, or,
    failing that, from the ATTACH name itself — so both of these work::

        ATTACH 'adbc' AS db (TYPE vgi, LOCATION w, driver 'sqlite', uri '/data.db');
        ATTACH '/data.db' AS db (TYPE vgi, LOCATION w, driver 'sqlite');

    Args:
        name: The ATTACH catalog name (first string in the ATTACH statement).
        options: The ATTACH options (driver, uri, credentials, ...).

    Returns:
        The resolved connection parameters.

    Raises:
        ValueError: If ``driver`` is missing or no connection string can be found.

    """
    # Declared options arrive with their defaults (e.g. empty strings); treat
    # blank values as "not supplied".
    provided = {k: v for k, v in options.items() if v is not None and v != ""}
    lowered = {k.lower(): v for k, v in provided.items()}
    driver = lowered.get("driver")
    if not driver:
        msg = "ADBC ATTACH requires a 'driver' option, e.g. driver 'postgresql' or driver 'sqlite'."
        raise ValueError(msg)

    location = lowered.get("uri") or lowered.get("location")
    if not location and name and name != CATALOG_NAME:
        location = name
    if not location:
        msg = "ADBC ATTACH requires a connection string via the 'uri' option (or as the ATTACH name)."
        raise ValueError(msg)

    driver_options = {
        k: str(v)
        for k, v in provided.items()
        if k.lower() not in _RESERVED_OPTIONS and k.lower() not in _FRAMEWORK_OPTIONS
    }
    read_only_opt = str(lowered.get("read_only", "true")).strip().lower()
    read_only = read_only_opt not in ("false", "0", "no")

    try:
        refresh_seconds = float(lowered.get("catalog_refresh_seconds", 0) or 0)
    except (TypeError, ValueError) as exc:
        msg = f"catalog_refresh_seconds must be a number, got {lowered.get('catalog_refresh_seconds')!r}"
        raise ValueError(msg) from exc

    return AdbcTarget(
        driver=str(driver),
        location=str(location),
        entrypoint=(str(lowered["entrypoint"]) if lowered.get("entrypoint") else None),
        options=driver_options,
        read_only=read_only,
        refresh_seconds=max(0.0, refresh_seconds),
    )


class AdbcCatalog(ReadOnlyCatalogInterface):
    """A read-only VGI catalog backed by a remote ADBC database."""

    catalog_name = CATALOG_NAME
    # Registering the functions on the worker is what makes them callable; listing them
    # here keeps the catalog self-describing.
    functions = [AdbcScan, AdbcQuery]

    # ------------------------------------------------------------------ discovery

    def catalogs(self) -> list[CatalogInfo]:
        """Advertise the single ``adbc`` connector catalog.

        Returns:
            A one-element list describing this connector.

        """
        return [
            CatalogInfo(
                name=CATALOG_NAME,
                implementation_version=None,
                data_version_spec=None,
                # Advertise the accepted ATTACH options so the extension's binder
                # accepts `driver`, `uri`, credentials, ... (declared on the Worker's
                # AttachOptions and injected onto this class as attach_option_specs).
                attach_option_specs=[spec.serialize() for spec in self.attach_option_specs],
                source_url=SOURCE_URL,
            )
        ]

    def loggable_attach_options(self, options: Mapping[str, Any]) -> Mapping[str, Any]:
        """Return the safe-to-log subset of ATTACH options (never credentials).

        Args:
            options: The raw ATTACH options.

        Returns:
            Only the driver name — the URI and every credential are withheld.

        """
        driver = options.get("driver") or options.get("Driver")
        return {"driver": driver} if driver else {}

    # ------------------------------------------------------------------ attach / detach

    def catalog_attach(
        self,
        *,
        name: str,
        options: dict[str, Any],
        data_version_spec: str | None = None,
        implementation_version: str | None = None,
        ctx: CallContext | None = None,
    ) -> CatalogAttachResult:
        """Open the remote ADBC database and register a sticky session.

        Args:
            name: The ATTACH catalog name.
            options: The ATTACH options (driver, uri, credentials, ...).
            data_version_spec: Ignored (no data versioning).
            implementation_version: Ignored.
            ctx: Call context; used to pin the HTTP session.

        Returns:
            The attach result whose ``attach_opaque_data`` is the sticky session id.

        """
        del data_version_spec, implementation_version
        target = _resolve_target(name, options)
        session_id = get_engine().attach(target)

        # Pin HTTP session affinity. No-op on subprocess transport.
        if ctx is not None:
            with contextlib.suppress(RuntimeError):
                ctx.set_cookie(STICKY_COOKIE_NAME, uuid.uuid4().hex)

        default_schema = self._default_schema(session_id)
        # The attach handle is the sticky session id (the VGI framework AEAD-seals it
        # per-principal on HTTP). Every follow-up must land on this process (see the sticky
        # sessions notes); a mis-routed call fails loudly rather than reconnecting.
        return CatalogAttachResult(
            attach_opaque_data=AttachOpaqueData(session_id),
            supports_transactions=False,
            supports_time_travel=False,
            catalog_version_frozen=False,
            catalog_version=1,
            attach_opaque_data_required=True,
            default_schema=default_schema,
            comment=CATALOG_COMMENT,
            tags=dict(CATALOG_TAGS),
            resolved_data_version=None,
            resolved_implementation_version=None,
        )

    def catalog_detach(self, *, attach_opaque_data: AttachOpaqueData) -> None:
        """Close the remote connection for a detached catalog.

        Args:
            attach_opaque_data: The sticky session id.

        """
        get_engine().detach(bytes(attach_opaque_data))

    def catalog_version(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None = None,
        ctx: CallContext | None = None,
    ) -> int:
        """Return a constant catalog version, asserting HTTP session stickiness.

        Args:
            attach_opaque_data: The sticky session id.
            transaction_opaque_data: Unused.
            ctx: Call context; its cookies are checked on HTTP transport.

        Returns:
            A constant non-zero version (the remote catalog is treated as stable
            for the life of an attachment).

        Raises:
            ValueError: If the sticky cookie is absent on an HTTP follow-up (a mis-routed
                request; enable load-balancer session affinity).

        """
        del transaction_opaque_data
        if ctx is not None and ctx.cookies and STICKY_COOKIE_NAME not in ctx.cookies:
            msg = (
                f"expected sticky cookie {STICKY_COOKIE_NAME!r} on follow-up request; "
                "enable load-balancer session affinity for the ADBC worker."
            )
            raise ValueError(msg)
        # DuckDB calls this at transaction boundaries — the safe point to re-read the
        # remote catalog when the session has a refresh interval configured, and a cheap,
        # rate-limited place to reap idle sessions/readers. The version only advances when
        # the remote actually changed, so unchanged catalogs stay cached.
        engine = get_engine()
        engine.reap()
        session = engine.resolve(bytes(attach_opaque_data))
        engine.maybe_refresh(session)
        return session.catalog_version

    # ------------------------------------------------------------------ schemas

    def schemas(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
    ) -> list[SchemaInfo]:
        """List the remote database's user schemas.

        Args:
            attach_opaque_data: The sticky session id.
            transaction_opaque_data: Unused.

        Returns:
            One :class:`SchemaInfo` per remote schema.

        """
        del transaction_opaque_data
        session = get_engine().resolve(bytes(attach_opaque_data))
        return [
            SchemaInfo(
                attach_opaque_data=attach_opaque_data,
                name=schema_name,
                comment=SCHEMA_COMMENT,
                tags=dict(SCHEMA_TAGS),
            )
            for schema_name in get_engine().schemas(session)
        ]

    def schema_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
    ) -> SchemaInfo | None:
        """Return a remote schema by name, or None if absent.

        Args:
            attach_opaque_data: The sticky session id.
            transaction_opaque_data: Unused.
            name: The schema name.

        Returns:
            The schema info, or None.

        """
        del transaction_opaque_data
        session = get_engine().resolve(bytes(attach_opaque_data))
        if name in get_engine().schemas(session):
            return SchemaInfo(
                attach_opaque_data=attach_opaque_data,
                name=name,
                comment=SCHEMA_COMMENT,
                tags=dict(SCHEMA_TAGS),
            )
        return None

    def schema_contents(  # type: ignore[override]
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
        type: SchemaObjectType,
    ) -> Sequence[TableInfo | ViewInfo | FunctionInfo | MacroInfo | IndexInfo]:
        """List a schema's relations (tables and views, both scannable).

        Args:
            attach_opaque_data: The sticky session id.
            transaction_opaque_data: Unused.
            name: The schema name.
            type: The object kind to return.

        Returns:
            :class:`TableInfo` objects for ``TABLE`` (remote tables and views); the
            ``adbc_query`` raw-SQL passthrough function for ``TABLE_FUNCTION``; empty
            for every other kind.

        """
        del transaction_opaque_data
        if type == SchemaObjectType.TABLE_FUNCTION:
            # Expose the raw-SQL passthrough as a callable, catalog-scoped function so
            # it receives this attachment's identity (attach_opaque_data) when called.
            session = get_engine().resolve(bytes(attach_opaque_data))
            if name in get_engine().schemas(session):
                return [self._function_to_info(AdbcQuery, name)]
            return []
        if type != SchemaObjectType.TABLE:
            return []
        session = get_engine().resolve(bytes(attach_opaque_data))
        result: list[TableInfo] = []
        for table_name, _table_type in get_engine().tables(session, name):
            info = self._table_info(session, name, table_name)
            if info is not None:
                result.append(info)
        return result

    def table_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> TableInfo | None:
        """Return a remote relation's table info, or None if it doesn't exist.

        Args:
            attach_opaque_data: The sticky session id.
            transaction_opaque_data: Unused.
            schema_name: The schema name.
            name: The table or view name.
            at_unit: Unused (no time travel).
            at_value: Unused.

        Returns:
            The table info, or None.

        """
        del transaction_opaque_data, at_unit, at_value
        session = get_engine().resolve(bytes(attach_opaque_data))
        return self._table_info(session, schema_name, name)

    def view_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
    ) -> None:
        """Views are surfaced as scannable tables, so this always returns None.

        Args:
            attach_opaque_data: Unused.
            transaction_opaque_data: Unused.
            schema_name: Unused.
            name: Unused.

        Returns:
            None.

        """
        del attach_opaque_data, transaction_opaque_data, schema_name, name
        return None

    def table_scan_function_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        at_unit: str | None,
        at_value: str | None,
    ) -> ScanFunctionResult:
        """Route a table scan to the generic ``adbc_scan`` function.

        Args:
            attach_opaque_data: The sticky session id (reaches the function as
                ``attach_opaque_data``).
            transaction_opaque_data: Unused.
            schema_name: The remote schema name (passed as positional arg 0).
            name: The remote table name (passed as positional arg 1).
            at_unit: Unused.
            at_value: Unused.

        Returns:
            The scan-function routing record.

        """
        del transaction_opaque_data, at_unit, at_value
        return ScanFunctionResult(
            function_name=ADBC_SCAN_FUNCTION_NAME,
            positional_arguments=[pa.scalar(schema_name), pa.scalar(name)],
            named_arguments={},
            required_extensions=[],
        )

    # ------------------------------------------------------------------ helpers

    def _default_schema(self, session_id: bytes) -> str:
        """Choose a sensible default schema for the attached database.

        Args:
            session_id: The sticky session id.

        Returns:
            ``public`` / ``main`` when present, else the first schema, else ``main``.

        """
        session = get_engine().get(session_id)
        schemas = get_engine().schemas(session)
        for preferred in ("public", "main"):
            if preferred in schemas:
                return preferred
        return schemas[0] if schemas else "main"

    @staticmethod
    def _table_info(session: Any, schema_name: str, name: str) -> TableInfo | None:
        """Build a :class:`TableInfo` for a remote relation, or None on failure.

        Args:
            session: The live ADBC session.
            schema_name: The schema name.
            name: The table or view name.

        Returns:
            The table info with the relation's Arrow schema, or None if it cannot
            be introspected (e.g. the relation vanished).

        """
        try:
            arrow_schema = get_engine().arrow_schema(session, schema_name, name)
        except Exception:  # noqa: BLE001 - a missing/renamed relation is not an error here
            # Observable (a relation silently dropping out of the catalog should not be
            # invisible), but not fatal — it may simply have been renamed/dropped upstream.
            logger.warning("could not introspect %s.%s; omitting from catalog", schema_name, name, exc_info=True)
            return None
        # Surface NOT NULL where the driver propagates it via Arrow field nullability.
        # (Some drivers — e.g. the SQLite ADBC driver — report every column nullable, so
        # this is empty for them; PostgreSQL generally populates it. PK/unique/check are
        # not exposed by adbc_scanner, so they stay empty.)
        not_null = [i for i, fld in enumerate(arrow_schema) if not fld.nullable]
        return TableInfo(
            name=name,
            schema_name=schema_name,
            columns=SerializedSchema(arrow_schema.serialize().to_pybytes()),
            not_null_constraints=not_null,
            unique_constraints=[],
            check_constraints=[],
            comment=None,
            tags={},
        )
