# Copyright 2026 Query Farm LLC - https://query.farm

"""The generic ADBC table-scan function hosted by the worker.

One function serves *every* remote table. The catalog's ``table_scan_function_get``
routes a scan of ``attached.schema.table`` here, passing ``schema_name`` and
``table_name`` as positional arguments and the attach identity (the sticky session
id) via ``attach_opaque_data``. The function:

* builds its output schema dynamically at bind (``on_bind``) by introspecting the
  remote relation through the sticky session's embedded DuckDB;
* honors **projection pushdown** — it scans only the columns DuckDB requests
  (``params.output_schema`` is already projected for us);
* honors **filter pushdown** — it renders the pushed predicates to a DuckDB WHERE
  clause, which ``adbc_scanner`` then pushes down to the remote database;
* streams the result one Arrow batch per ``process`` call, so memory stays flat and
  DuckDB's pipe backpressure paces the remote read.

The live ``RecordBatchReader`` is kept in a *process-local* registry keyed by a per-scan
token (the token, not the reader, lives in the serializable state). The *session* rebuilds
itself on a different worker-pool process (stateless reconnect), but a *streaming reader*
cannot — so if a follow-up ``process`` for an already-opened scan lands on a process without
the reader, we raise :class:`ScanInterruptedError` (re-run the query) rather than silently
restart the stream from row 0. Session affinity keeps a scan on one process in the happy path.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any

import pyarrow as pa
from vgi.arguments import Arg
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableCardinality,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi_adbc.engine import get_engine
from vgi_adbc.sql import quote_identifier

if TYPE_CHECKING:
    from vgi.table_filter_pushdown import PushdownFilters

logger = logging.getLogger("vgi_adbc.functions")

# The DuckDB function name the catalog routes table scans to. Kept as a module
# constant so the catalog and the function agree on one string.
ADBC_SCAN_FUNCTION_NAME = "adbc_scan"

# The user-callable raw-SQL passthrough function, advertised per-schema by the catalog.
ADBC_QUERY_FUNCTION_NAME = "adbc_query"

_SCAN_INTERRUPTED_MSG = (
    "ADBC scan interrupted: its streaming reader is no longer held by this worker process "
    "(the worker pool routed a follow-up batch to a different process mid-scan). A live scan "
    "cannot resume across processes; re-run the query to restart it."
)


@dataclass(slots=True)
class _ScanEntry:
    """A registered in-flight scan reader plus the bookkeeping used to reap it.

    Attributes:
        reader: The live ``pyarrow.RecordBatchReader`` streaming the scan.
        session_id: The attach session the scan belongs to, so a detach can release
            exactly this session's readers before closing its connection.
        last_used: ``time.monotonic()`` of the last batch read, for idle reaping.

    """

    reader: Any
    session_id: bytes
    last_used: float


# Process-local registry of in-flight scan readers, keyed by the per-scan token
# carried in the (serializable) function state. The live reader never leaves this
# process — strict sticky sessions guarantee every follow-up call lands here. Entries
# are reaped when idle (a client that abandons a scan without exhausting it or firing
# on_cancel would otherwise leak a cursor pinning a pooled ADBC connection forever).
_active_scans: dict[bytes, _ScanEntry] = {}
_active_lock = threading.Lock()


def _close_reader(reader: Any) -> None:
    """Close a scan reader, swallowing any error (best-effort).

    Args:
        reader: A ``pyarrow.RecordBatchReader`` (or anything with ``close``).

    """
    close = getattr(reader, "close", None)
    if close is None:
        return
    try:
        close()
    except Exception:  # noqa: BLE001 - best-effort release
        logger.debug("ignored error closing scan reader", exc_info=True)


def _register_reader(token: bytes, reader: Any, session_id: bytes) -> None:
    """Register a new in-flight reader, then enforce the process scan cap."""
    with _active_lock:
        _active_scans[token] = _ScanEntry(reader=reader, session_id=session_id, last_used=time.monotonic())
    _enforce_scan_limit()


def _get_reader(token: bytes) -> Any | None:
    """Return the reader for ``token`` (stamping its last-used time), or None."""
    with _active_lock:
        entry = _active_scans.get(token)
        if entry is None:
            return None
        entry.last_used = time.monotonic()
        return entry.reader


def _release_reader(token: bytes) -> None:
    """Drop and close a scan's reader (idempotent)."""
    with _active_lock:
        entry = _active_scans.pop(token, None)
    if entry is not None:
        _close_reader(entry.reader)


def reap_idle_scans(max_idle_seconds: float) -> int:
    """Close and drop readers not read within ``max_idle_seconds`` (0 disables).

    Args:
        max_idle_seconds: Idle threshold; readers older than this are reaped.

    Returns:
        The number of readers reaped.

    """
    if max_idle_seconds <= 0:
        return 0
    cutoff = time.monotonic() - max_idle_seconds
    with _active_lock:
        stale = [token for token, entry in _active_scans.items() if entry.last_used < cutoff]
        entries = [_active_scans.pop(token) for token in stale]
    for entry in entries:
        _close_reader(entry.reader)
    if entries:
        logger.info("adbc.scan_reap idle=%s reaped=%s", max_idle_seconds, len(entries))
    return len(entries)


def release_scans_for_session(session_id: bytes) -> int:
    """Close and drop every in-flight reader belonging to ``session_id``.

    Called before a session's DuckDB connection is closed so its readers' cursors are
    released first (they pin the connection), avoiding a teardown that races live scans.

    Args:
        session_id: The attach session whose readers to release.

    Returns:
        The number of readers released.

    """
    with _active_lock:
        tokens = [token for token, entry in _active_scans.items() if entry.session_id == session_id]
        entries = [_active_scans.pop(token) for token in tokens]
    for entry in entries:
        _close_reader(entry.reader)
    return len(entries)


def _enforce_scan_limit() -> None:
    """Evict the least-recently-used readers when over the configured scan cap."""
    from vgi_adbc.engine import get_engine

    max_scans = get_engine().max_scans
    if max_scans <= 0:
        return
    with _active_lock:
        overflow = len(_active_scans) - max_scans
        if overflow <= 0:
            return
        ordered = sorted(_active_scans.items(), key=lambda kv: kv[1].last_used)
        entries = [_active_scans.pop(token) for token, _ in ordered[:overflow]]
    for entry in entries:
        _close_reader(entry.reader)
    logger.info("adbc.scan_evict max=%s evicted=%s", max_scans, len(entries))


def active_scan_count() -> int:
    """Return the number of in-flight scan readers (for tests / observability)."""
    with _active_lock:
        return len(_active_scans)


def release_all_scans() -> None:
    """Close and drop all in-flight scan readers (called at worker shutdown).

    The readers pin DuckDB query results, which in turn pin pooled ADBC connections.
    Releasing them before the engine closes its connections lets the connections tear
    down cleanly during interpreter shutdown. See ``AdbcEngine.close_all``.
    """
    with _active_lock:
        entries = list(_active_scans.values())
        _active_scans.clear()
    for entry in entries:
        _close_reader(entry.reader)


@dataclass(slots=True, frozen=True)
class _ScanArgs:
    """Positional arguments injected by the catalog's scan-function routing."""

    schema_name: Annotated[str, Arg(0, doc="Remote schema that contains the table being scanned.")]
    table_name: Annotated[str, Arg(1, doc="Remote table or view being scanned.")]


class ScanInterruptedError(RuntimeError):
    """Raised when an in-flight scan's reader is gone and cannot be safely resumed.

    A scan streams from a process-local, non-serializable cursor. If a follow-up ``process``
    call for a scan that has already emitted batches lands on a worker-pool process that does
    not hold the reader (a process hop mid-scan), re-opening would silently restart the stream
    from row 0 and duplicate data. We fail loudly instead — the client should re-run the query.
    """


@dataclass(kw_only=True)
class _ScanState(ArrowSerializableDataclass):
    """Per-scan state. Only the token + flags round-trip; the reader stays process-local."""

    token: bytes = b""
    done: bool = False
    # True once this scan has opened its reader, so a later call that finds no reader knows the
    # stream was interrupted (process hop) rather than never started — and refuses to restart.
    opened: bool = False


def _build_where(params: ProcessParams[_ScanArgs]) -> tuple[str, list[Any]]:
    """Render pushed-down predicates to a DuckDB WHERE body and its parameters.

    Args:
        params: The process parameters carrying the init request's filters.

    Returns:
        A ``(where_body, params)`` tuple. ``where_body`` excludes the ``WHERE``
        keyword and is empty when nothing can be pushed; ``params`` are the
        positional ``?`` bind values. Unsupported filters degrade to no pushdown
        (DuckDB re-verifies every result, so correctness is preserved).

    """
    init_call = params.init_call
    if init_call is None or init_call.pushdown_filters is None:
        return "", []
    filters: PushdownFilters | None = TableFunctionGenerator.pushdown_filters(
        init_call.pushdown_filters, join_keys=init_call.join_keys
    )
    if not filters:
        return "", []
    try:
        where, bind_params = filters.to_sql(quote_identifier=quote_identifier, placeholder="?")
    except Exception:  # noqa: BLE001 - unsupported filter shapes fall back to no pushdown
        logger.debug("filter pushdown skipped (unsupported filter)", exc_info=True)
        return "", []
    return where, list(bind_params)


def _align(batch: pa.RecordBatch, schema: pa.Schema) -> pa.RecordBatch:
    """Coerce a scan batch to the exact output schema (order + types).

    A safety net: the batch already comes from the same embedded DuckDB that
    produced ``schema`` at bind, so this is normally a no-op, but it guards
    against field-order or nullability drift raising a VGI type-mismatch.

    Args:
        batch: A record batch streamed from the remote scan.
        schema: The function's projected output schema.

    Returns:
        A record batch whose schema equals ``schema``.

    """
    if batch.schema.equals(schema, check_metadata=False):
        return batch
    arrays = []
    for field in schema:
        idx = batch.schema.get_field_index(field.name)
        if idx < 0:
            msg = f"remote scan is missing expected column {field.name!r}"
            raise ValueError(msg)
        column = batch.column(idx)
        if not column.type.equals(field.type):
            column = column.cast(field.type)
        arrays.append(column)
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


@init_single_worker
class AdbcScan(TableFunctionGenerator[_ScanArgs, _ScanState]):
    """Stream one remote ADBC table with projection + filter pushdown."""

    class Meta:
        """Function metadata."""

        name = ADBC_SCAN_FUNCTION_NAME
        description = "Scan a remote ADBC table, pushing projection and filters down to the source database."
        projection_pushdown = True
        filter_pushdown = True
        # The catalog routes every remote-table scan here, so a plain
        # ``SELECT ... FROM <alias>.<schema>.<table>`` exercises this function.
        # These examples demonstrate that scan path with projection and filter
        # pushdown against a demonstrative ``employees`` table.
        examples = [
            FunctionExample(
                description="Scan a remote table",
                sql="SELECT * FROM adbc.main.employees LIMIT 5",
            ),
            FunctionExample(
                description="Projection + filter pushdown to the source",
                sql="SELECT id, name FROM adbc.main.employees WHERE salary > 90000 ORDER BY id",
            ),
            FunctionExample(
                description="Aggregate over a remote table",
                sql="SELECT count(*) FROM adbc.main.employees WHERE active = 1",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[_ScanArgs]) -> BindResponse:
        """Introspect the remote relation and return its Arrow schema.

        Args:
            params: Bind parameters carrying the table coordinates and attach id.

        Returns:
            A :class:`BindResponse` whose ``output_schema`` is the remote table's
            full Arrow schema (projection is applied by the framework at init).

        """
        session = get_engine().resolve(params.attach_opaque_data)
        output_schema = get_engine().arrow_schema(session, params.args.schema_name, params.args.table_name)
        return BindResponse(output_schema=output_schema)

    @classmethod
    def cardinality(cls, params: BindParams[_ScanArgs]) -> TableCardinality:
        """Estimate the remote table's row count so DuckDB can plan joins.

        Without this every remote table looks like one row to the planner, wrecking
        join orders against large tables. The count is cached per session.

        Args:
            params: Bind parameters carrying the table coordinates and attach id.

        Returns:
            A :class:`TableCardinality` with the remote row count (or unknown).

        """
        try:
            session = get_engine().resolve(params.attach_opaque_data)
            estimate = get_engine().row_count(session, params.args.schema_name, params.args.table_name)
        except Exception:  # noqa: BLE001 - cardinality is a best-effort hint
            logger.debug("cardinality estimate unavailable", exc_info=True)
            return TableCardinality(estimate=None, max=None)
        return TableCardinality(estimate=estimate, max=None)

    @classmethod
    def initial_state(cls, params: ProcessParams[_ScanArgs]) -> _ScanState:
        """Create fresh per-scan state with a unique reader token.

        Args:
            params: Process parameters (unused).

        Returns:
            A new :class:`_ScanState`.

        """
        del params
        return _ScanState(token=uuid.uuid4().bytes, done=False)

    @classmethod
    def process(cls, params: ProcessParams[_ScanArgs], state: _ScanState, out: OutputCollector) -> None:
        """Emit the next batch of the remote scan, or finish when exhausted.

        Args:
            params: Process parameters (table coordinates, attach id, filters).
            state: The per-scan state carrying the reader token.
            out: The output collector.

        """
        if state.done:
            out.finish()
            return
        reader = cls._reader_for(params, state)
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            _release_reader(state.token)
            state.done = True
            out.finish()
            return
        out.emit(_align(batch, params.output_schema))

    @classmethod
    def on_cancel(cls, params: ProcessParams[_ScanArgs], state: _ScanState) -> None:
        """Release the in-flight reader when a scan is torn down early.

        Args:
            params: Process parameters (unused).
            state: The per-scan state carrying the reader token.

        """
        del params
        _release_reader(state.token)

    # -- reader lifecycle ------------------------------------------------------------

    @classmethod
    def _reader_for(cls, params: ProcessParams[_ScanArgs], state: _ScanState) -> Any:
        """Return this scan's reader, opening it on first use.

        Args:
            params: Process parameters.
            state: The per-scan state carrying the reader token.

        Returns:
            The scan's ``pyarrow.RecordBatchReader``.

        """
        reader = _get_reader(state.token)
        if reader is not None:
            return reader
        if state.opened:
            raise ScanInterruptedError(_SCAN_INTERRUPTED_MSG)
        session = get_engine().resolve(params.attach_opaque_data)
        columns = [field.name for field in params.output_schema]
        where, bind_params = _build_where(params)
        reader = get_engine().scan(
            session,
            params.args.schema_name,
            params.args.table_name,
            columns,
            where,
            bind_params,
        )
        _register_reader(state.token, reader, session.session_id)
        state.opened = True
        return reader


@dataclass(slots=True, frozen=True)
class _QueryArgs:
    """The single positional argument for the raw-SQL passthrough function."""

    query: Annotated[
        str,
        Arg(0, doc="SQL to run on the attached remote database in its own dialect; results stream back as rows."),
    ]


@init_single_worker
class AdbcQuery(TableFunctionGenerator[_QueryArgs, _ScanState]):
    """Run native SQL on the attached remote database and stream the results.

    Unlike a plain table scan (which runs DuckDB SQL over the mirrored catalog), this
    executes the SQL **on the remote database itself**, in the remote's own dialect, so
    it can use engine-specific features and query objects the catalog does not mirror
    (other schemas, system catalogs, functions). The attachment is identified by
    ``params.attach_opaque_data``, exactly like the scan function.
    """

    class Meta:
        """Function metadata."""

        name = ADBC_QUERY_FUNCTION_NAME
        description = "Run native SQL on the attached remote database (its own dialect) and stream the rows back."
        examples = [
            FunctionExample(
                description="Project a column from a native query",
                sql="SELECT one FROM adbc.main.adbc_query('SELECT 1 AS one')",
            ),
            FunctionExample(
                description="Aggregate a native query reaching a table the catalog does not mirror",
                sql="SELECT count(*) FROM adbc.main.adbc_query('SELECT * FROM analytics.summary')",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[_QueryArgs]) -> BindResponse:
        """Prepare the query on the remote and return its output schema.

        Args:
            params: Bind parameters carrying the SQL and the attach id.

        Returns:
            A :class:`BindResponse` with the query's Arrow output schema.

        """
        session = get_engine().resolve(params.attach_opaque_data)
        return BindResponse(output_schema=get_engine().raw_query_schema(session, params.args.query))

    @classmethod
    def initial_state(cls, params: ProcessParams[_QueryArgs]) -> _ScanState:
        """Create fresh per-query state with a unique reader token.

        Args:
            params: Process parameters (unused).

        Returns:
            A new :class:`_ScanState`.

        """
        del params
        return _ScanState(token=uuid.uuid4().bytes, done=False)

    @classmethod
    def process(cls, params: ProcessParams[_QueryArgs], state: _ScanState, out: OutputCollector) -> None:
        """Emit the next batch of the raw query, or finish when exhausted.

        Args:
            params: Process parameters (the SQL, the attach id).
            state: The per-query state carrying the reader token.
            out: The output collector.

        """
        if state.done:
            out.finish()
            return
        reader = _get_reader(state.token)
        if reader is None:
            if state.opened:
                raise ScanInterruptedError(_SCAN_INTERRUPTED_MSG)
            session = get_engine().resolve(params.attach_opaque_data)
            reader = get_engine().raw_query(session, params.args.query)
            _register_reader(state.token, reader, session.session_id)
            state.opened = True
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            _release_reader(state.token)
            state.done = True
            out.finish()
            return
        out.emit(_align(batch, params.output_schema))

    @classmethod
    def on_cancel(cls, params: ProcessParams[_QueryArgs], state: _ScanState) -> None:
        """Release the in-flight reader when a query is torn down early.

        Args:
            params: Process parameters (unused).
            state: The per-query state carrying the reader token.

        """
        del params
        _release_reader(state.token)
