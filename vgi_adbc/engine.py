# Copyright 2026 Query Farm LLC - https://query.farm

"""The embedded DuckDB engine that talks to remote ADBC databases.

The worker runs its ADBC queries inside an in-process ``haybarn`` DuckDB (a
duckdb-python drop-in, pinned recent so its community extension repository serves
a recent ``adbc_scanner`` build). Each attached remote database gets its own
DuckDB instance with ``adbc_scanner`` loaded and the remote ``ATTACH``-ed under
the local alias ``remote`` — so the worker queries it with ordinary DuckDB SQL
(``SELECT ... FROM remote.<schema>.<table>``) and ``adbc_scanner`` re-dialects and
pushes predicates/projection down to PostgreSQL, SQLite, or any other ADBC target.

**Strictly sticky sessions.** A live ADBC connection is a native, process-local object; it
can't be serialized into a VGI state token. Each attach is held in *this* process, keyed by
a random session id packed into ``attach_opaque_data``. Every follow-up call **must** land on
the same worker process — deployments MUST enable load-balancer session affinity on HTTP. A
call that reaches a different process (the VGI worker pool can spawn/route to a sibling
process — notably after any worker error) raises :class:`SessionNotFoundError` rather than
reconnecting: a rebuilt connection would silently lose any connection-local state (an open
transaction, temp tables, session settings), so a lost session fails loudly and the client
re-``ATTACH``es. A *live streaming scan* likewise cannot resume across processes — enforced
loudly at the scan layer (``ScanInterruptedError``), never silently restarted.
"""

from __future__ import annotations

import atexit
import hashlib
import logging
import threading
import time
import uuid
import warnings
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from vgi_adbc.sql import (
    projection_list,
    qualified_relation,
    quote_identifier,
    quote_string,
    validate_option_key,
)

if TYPE_CHECKING:
    import pyarrow as pa

logger = logging.getLogger("vgi_adbc.engine")

# The community/extension name is ``adbc_scanner``; it exposes ``TYPE adbc`` + the
# ``adbc_*`` functions once loaded.
_ADBC_EXTENSION = "adbc_scanner"

# The local alias each per-attach DuckDB instance uses for its remote catalog.
_REMOTE = "remote"

# Schemas DuckDB/adbc surface that are never user data.
_SYSTEM_SCHEMAS = frozenset({"information_schema", "pg_catalog"})

# Read-only "seal" statements for the raw ``adbc_query`` handle, keyed by driver name.
# adbc_scanner honors ``READ_ONLY`` on the ATTACH scan path, but ``adbc_query`` runs on a
# *separate* ``adbc_connect`` handle that does not inherit it — so under a read-only attach
# we run one of these once (via ``adbc_execute``) to make that connection reject writes at
# the source. A driver with no entry here cannot be sealed, so ``adbc_query`` is refused
# under a read-only attach rather than silently permitting writes (see ``_ensure_query_handle``).
_READONLY_SEAL: dict[str, str] = {
    "sqlite": "PRAGMA query_only = ON",
    "postgresql": "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
    "postgres": "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
}


def _readonly_seal_sql(driver: str) -> str | None:
    """Return the read-only seal statement for ``driver``, or None if unknown.

    Matches the driver name case-insensitively, and (so a driver given as a path to a
    shared library still resolves) falls back to a substring match against the known
    driver names.

    Args:
        driver: The ADBC driver name (or path) from the resolved target.

    Returns:
        The seal SQL to run once on the raw handle, or None when the driver has no
        known read-only mechanism.

    """
    key = driver.strip().lower()
    seal = _READONLY_SEAL.get(key)
    if seal is not None:
        return seal
    for name, statement in _READONLY_SEAL.items():
        if name in key:
            return statement
    return None


# A DuckDB connect callable: takes optional config kwargs, returns a connection.
ConnectFn = Callable[..., Any]


def _default_connect() -> ConnectFn:
    """Resolve the DuckDB connect function — **haybarn**, not stock DuckDB.

    The ``vgi`` client extension and the ``adbc_scanner`` extension this worker loads are
    served by *haybarn's* community extension repository, not stock DuckDB's — so a stock
    ``duckdb`` build would fail to resolve them (different version + different repo). haybarn
    is therefore required. Set ``VGI_ADBC_ALLOW_DUCKDB=1`` to opt into a stock ``duckdb``
    connection anyway (only sensible when you supply a compatible extension locally via
    ``VGI_ADBC_EXTENSION_PATH``).

    Returns:
        ``haybarn.connect`` (or ``duckdb.connect`` under the explicit opt-out).

    Raises:
        RuntimeError: If haybarn is not importable (and the duckdb opt-out is not set / also
            unavailable).

    """
    try:
        import haybarn

        return haybarn.connect
    except ImportError:
        pass
    import os

    if os.environ.get("VGI_ADBC_ALLOW_DUCKDB", "").strip().lower() in ("1", "true", "yes"):
        try:
            import duckdb

            logger.warning("haybarn not importable; using stock duckdb (VGI_ADBC_ALLOW_DUCKDB set)")
            return duckdb.connect  # type: ignore[no-any-return]
        except ImportError:
            pass
    msg = (
        "haybarn is required but not importable. This worker loads the vgi + adbc_scanner "
        "extensions from haybarn's community repository, which stock DuckDB cannot resolve. "
        "Install the project's dependencies (they pin haybarn). To force a stock duckdb "
        "connection anyway, set VGI_ADBC_ALLOW_DUCKDB=1 (and supply VGI_ADBC_EXTENSION_PATH)."
    )
    raise RuntimeError(msg)


class SessionNotFoundError(RuntimeError):
    """Raised when an attach session isn't held by this worker process.

    Sessions are strictly sticky: an attached connection lives only in the process that
    served its ATTACH and is never rebuilt elsewhere — a fresh connection would silently lose
    connection-local state (an open transaction, temp tables, session settings). A call that
    reaches a different process (worker-pool hop, or a restarted worker) fails loudly instead.
    Recover by re-``ATTACH``-ing the database.
    """


class SessionLimitError(RuntimeError):
    """Raised when a new attach would exceed the configured ``max_sessions`` cap.

    Idle sessions are reaped first; this is raised only when the cap is still reached by
    live sessions. Detach an unused attachment or raise ``VGI_ADBC_MAX_SESSIONS``.
    """


@dataclass(frozen=True, slots=True)
class AdbcTarget:
    """Resolved connection parameters for one remote ADBC database.

    Attributes:
        driver: ADBC driver name (``postgresql``, ``sqlite``, ...) or a path to a
            driver shared library / manifest.
        location: The ADBC connection string passed as the ATTACH target — a URI
            (``postgresql://...``) or a file path (for SQLite).
        entrypoint: Optional custom driver entry-point symbol.
        options: Extra driver options (``username``, ``password``, ...), rendered
            into the ATTACH option list. Keys must be simple identifiers.
        read_only: When True (the default and only v1 value) the remote is attached
            READ_ONLY so writes are rejected at the source.
        refresh_seconds: When > 0, re-read the remote catalog at most this often so
            remote DDL (added/dropped tables, changed columns) becomes visible without
            a manual re-ATTACH. 0 (the default) keeps the attach-time snapshot stable.

    """

    driver: str
    location: str
    entrypoint: str | None = None
    options: dict[str, str] = field(default_factory=dict)
    read_only: bool = True
    refresh_seconds: float = 0.0


@dataclass
class AdbcSession:
    """A live, process-local attachment to one remote ADBC database.

    Attributes:
        session_id: Random 16-byte identifier, packed into ``attach_opaque_data``.
        target: The resolved connection parameters.
        con: The per-attach DuckDB connection (its own instance) with
            ``adbc_scanner`` loaded and the remote attached as ``remote``.
        lifecycle_lock: Serializes attach/detach mutation of ``con``; reads use
            independent cursors and do not take it.
        catalog_version: Monotonic version reported to DuckDB; bumped when a refresh
            observes the remote catalog changed, so DuckDB re-introspects.
        fingerprint: Hash of the remote catalog's columns, used to detect change.
        last_refresh: ``time.monotonic()`` of the last refresh check.
        last_used: ``time.monotonic()`` of the last activity on the session, for idle
            eviction.

    """

    session_id: bytes
    target: AdbcTarget
    con: Any
    lifecycle_lock: threading.Lock = field(default_factory=threading.Lock)
    schema_cache: dict[tuple[str, str], pa.Schema] = field(default_factory=dict)
    count_cache: dict[tuple[str, str], int] = field(default_factory=dict)
    catalog_version: int = 1
    fingerprint: str = ""
    last_refresh: float = 0.0
    last_used: float = 0.0
    # Lazily-opened raw ADBC connection handle (adbc_connect) for adbc_query() —
    # a persistent connection the remote's native SQL runs on, separate from the
    # ATTACH scan pool. None until the first raw query.
    query_handle: int | None = None
    query_lock: threading.Lock = field(default_factory=threading.Lock)


class EngineStats:
    """Thread-safe lifecycle counters for the engine (the source for logs + /metrics).

    Monotonic counters (``attaches``, ``detaches``, ``attach_errors``, ``sessions_evicted``,
    ``scans_opened``, ``scans_reaped``, ``scans_evicted``, ``cardinality_errors``,
    ``refresh_failures``) plus the live gauges ``active_sessions`` / ``active_scans`` supplied
    at snapshot time. See :mod:`vgi_adbc.metrics` for the Prometheus rendering.
    """

    def __init__(self) -> None:
        """Initialize empty counters."""
        self._lock = threading.Lock()
        self._counters: Counter[str] = Counter()

    def incr(self, name: str, n: int = 1) -> None:
        """Increment a monotonic counter (no-op for n<=0)."""
        if n <= 0:
            return
        with self._lock:
            self._counters[name] += n

    def snapshot(self, *, active_sessions: int, active_scans: int) -> dict[str, int]:
        """Return a snapshot: all counters plus the two live gauges."""
        with self._lock:
            snap = dict(self._counters)
        snap["active_sessions"] = active_sessions
        snap["active_scans"] = active_scans
        return snap


def _format_stats(snapshot: dict[str, int]) -> str:
    """Render a stats snapshot as a stable ``key=value`` string for logging."""
    return " ".join(f"{k}={snapshot[k]}" for k in sorted(snapshot))


class AdbcEngine:
    """Process-local registry of sticky ADBC attachments over an embedded DuckDB."""

    def __init__(
        self,
        *,
        connect_fn: ConnectFn | None = None,
        extension_path: str | None = None,
        session_idle_seconds: float = 0.0,
        scan_idle_seconds: float = 0.0,
        max_sessions: int = 0,
        max_scans: int = 0,
        reap_interval_seconds: float = 60.0,
        query_timeout_seconds: float = 0.0,
        cardinality_mode: str = "estimate",
        cardinality_max_count: int = 0,
    ) -> None:
        """Initialize the engine.

        Args:
            connect_fn: DuckDB connect callable (defaults to ``haybarn.connect``).
                Injected in tests.
            extension_path: Optional path to a local ``adbc_scanner.duckdb_extension``
                to ``LOAD`` directly (unsigned) instead of installing from the
                community repository. Set via ``VGI_ADBC_EXTENSION_PATH``.
            session_idle_seconds: Evict a session idle longer than this (0 disables).
            scan_idle_seconds: Reap an in-flight scan reader idle longer than this
                (0 disables).
            max_sessions: Reject a new attach once this many sessions are live, after
                first reaping idle ones (0 = unlimited).
            max_scans: Cap on concurrent in-flight scan readers; the least-recently-used
                are evicted beyond it (0 = unlimited).
            reap_interval_seconds: Minimum spacing between opportunistic reaper passes.
            query_timeout_seconds: Per-statement timeout hint applied where the driver
                supports it (0 disables).
            cardinality_mode: ``estimate`` (cheap driver stats when available, else a
                bounded count), ``count`` (always ``count(*)``), or ``off``.
            cardinality_max_count: When counting, skip (return no estimate) for relations
                whose count would exceed this (0 = no cap).

        """
        self._connect_fn = connect_fn or _default_connect()
        self._extension_path = extension_path
        self._sessions: dict[bytes, AdbcSession] = {}
        self._registry_lock = threading.Lock()
        self._install_lock = threading.Lock()
        self._installed = False
        self.stats = EngineStats()
        # Resource-lifecycle bounds (B2/B3).
        self._session_idle = max(0.0, session_idle_seconds)
        self._scan_idle = max(0.0, scan_idle_seconds)
        self._max_sessions = max(0, max_sessions)
        self.max_scans = max(0, max_scans)
        self._reap_interval = max(0.0, reap_interval_seconds)
        self._last_reap = 0.0
        self._reap_lock = threading.Lock()
        # Timeout / cardinality policy (B4).
        self._query_timeout = max(0.0, query_timeout_seconds)
        self._cardinality_mode = cardinality_mode
        self._cardinality_max_count = max(0, cardinality_max_count)

    # -- extension + connection setup ------------------------------------------------

    def _new_connection(self) -> Any:
        """Open a fresh DuckDB instance with ``adbc_scanner`` loaded.

        Returns:
            A new DuckDB connection ready to ATTACH a remote ADBC database.

        """
        con = self._connect_fn(config={"allow_unsigned_extensions": "true"})
        self._load_extension(con)
        return con

    def _load_extension(self, con: Any) -> None:
        """Ensure ``adbc_scanner`` is loaded into ``con``.

        Uses an explicit local extension path when configured, else installs once
        per process from the community repository and loads it into each instance.

        Args:
            con: The DuckDB connection to load the extension into.

        """
        if self._extension_path:
            con.execute(f"LOAD {quote_string(self._extension_path)}")
            return
        with self._install_lock:
            if not self._installed:
                con.execute(f"INSTALL {_ADBC_EXTENSION} FROM community")
                self._installed = True
        con.execute(f"LOAD {_ADBC_EXTENSION}")

    def _attach_sql(self, target: AdbcTarget) -> str:
        """Render the ``ATTACH ... (TYPE adbc, ...)`` statement for ``target``.

        Args:
            target: The resolved connection parameters.

        Returns:
            A DuckDB ATTACH statement string with all values safely quoted.

        """
        parts = ["TYPE adbc", f"driver {quote_string(target.driver)}"]
        if target.entrypoint:
            parts.append(f"entrypoint {quote_string(target.entrypoint)}")
        for key, value in target.options.items():
            parts.append(f"{validate_option_key(key)} {quote_string(value)}")
        if target.read_only:
            parts.append("READ_ONLY")
        options = ", ".join(parts)
        return f"ATTACH {quote_string(target.location)} AS {quote_identifier(_REMOTE)} ({options})"

    def _fingerprint(self, con: Any) -> str:
        """Hash the remote catalog's columns to detect metadata changes.

        Args:
            con: A DuckDB connection (or cursor) with the remote attached.

        Returns:
            A short hex digest of every ``(schema, table, column, position, type)`` in
            the attached remote catalog.

        """
        cursor = con.cursor()
        try:
            cursor.execute(
                "SELECT table_schema, table_name, column_name, ordinal_position, data_type "
                "FROM information_schema.columns "
                f"WHERE table_catalog = {quote_string(_REMOTE)} ORDER BY 1, 2, 4"
            )
            rows = cursor.fetchall()
        finally:
            _safe_close(cursor)
        digest = hashlib.sha256(repr(rows).encode("utf-8")).hexdigest()
        return digest[:32]

    # -- session lifecycle -----------------------------------------------------------

    def attach(self, target: AdbcTarget) -> bytes:
        """Open a remote ADBC database and register a sticky session.

        Args:
            target: The resolved connection parameters.

        Returns:
            The 16-byte session id to pack into ``attach_opaque_data``.

        """
        # Reclaim idle sessions/readers before we take on another, then enforce the cap.
        self.reap()
        self._enforce_session_limit()
        session_id = uuid.uuid4().bytes
        try:
            session = self._build_session(session_id, target)
        except Exception:
            self.stats.incr("attach_errors")
            raise
        with self._registry_lock:
            self._sessions[session_id] = session
        self.stats.incr("attaches")
        logger.info(
            "adbc.attach driver=%s read_only=%s refresh_seconds=%s sessions=%s",
            target.driver,
            target.read_only,
            target.refresh_seconds,
            len(self._sessions),
        )
        return session_id

    def _build_session(self, session_id: bytes, target: AdbcTarget) -> AdbcSession:
        """Open a fresh connection for ``target`` and return an (unregistered) session.

        Args:
            session_id: The id to stamp on the session.
            target: The resolved connection parameters.

        Returns:
            A new :class:`AdbcSession` (caller registers it).

        """
        con = self._new_connection()
        try:
            con.execute(self._attach_sql(target))
            fingerprint = self._fingerprint(con)
        except Exception:
            _safe_close(con)
            raise
        now = time.monotonic()
        return AdbcSession(
            session_id=session_id,
            target=target,
            con=con,
            fingerprint=fingerprint,
            last_refresh=now,
            last_used=now,
        )

    def resolve(self, attach_opaque_data: bytes | None) -> AdbcSession:
        """Look up the session an attach handle refers to (strictly sticky, no reconnect).

        This is the entry point catalog/function calls use. ``attach_opaque_data`` is the
        16-byte session id. If the session isn't held by *this* worker process the call
        raises :class:`SessionNotFoundError` rather than rebuilding a fresh connection — a
        rebuilt connection would silently lose connection-local state (an open transaction,
        temp tables, session settings), so a lost session fails loudly and the client
        re-``ATTACH``es.

        Args:
            attach_opaque_data: The 16-byte session id from the attach handle.

        Returns:
            The live :class:`AdbcSession`.

        Raises:
            ValueError: If the attach data is missing.
            SessionNotFoundError: If the session isn't held by this process.

        """
        if not attach_opaque_data:
            msg = "ADBC function invoked without an attachment; ATTACH the catalog first."
            raise ValueError(msg)
        return self.get(bytes(attach_opaque_data))

    def _enforce_session_limit(self) -> None:
        """Refuse a new attach when at the session cap (after idle reaping).

        Raises:
            SessionLimitError: If ``max_sessions`` live sessions already exist.

        """
        if self._max_sessions <= 0:
            return
        with self._registry_lock:
            count = len(self._sessions)
        if count >= self._max_sessions:
            msg = (
                f"ADBC session limit reached ({self._max_sessions} live attachments); refusing a new "
                "ATTACH. Detach an unused attachment, raise VGI_ADBC_MAX_SESSIONS, or enable idle "
                "eviction with VGI_ADBC_SESSION_IDLE_SECONDS."
            )
            raise SessionLimitError(msg)

    def reap(self, *, force: bool = False) -> None:
        """Opportunistically evict idle sessions and reap idle scan readers.

        Called at natural boundaries (attach, ``catalog_version``) rather than from a
        background thread, and rate-limited to at most once per ``reap_interval_seconds``
        so it stays cheap on the hot path. No-op unless an idle bound is configured.

        Args:
            force: Skip the rate limit (used when making room at the session cap).

        """
        if self._session_idle <= 0 and self._scan_idle <= 0:
            return
        now = time.monotonic()
        with self._reap_lock:
            if not force and (now - self._last_reap) < self._reap_interval:
                return
            self._last_reap = now
        if self._session_idle > 0:
            with self._registry_lock:
                stale = [sid for sid, s in self._sessions.items() if (now - s.last_used) > self._session_idle]
            for sid in stale:
                logger.info("adbc.session_evict idle=%s", self._session_idle)
                self.stats.incr("sessions_evicted")
                self.detach(sid)
        if self._scan_idle > 0:
            from vgi_adbc.functions import reap_idle_scans

            self.stats.incr("scans_reaped", reap_idle_scans(self._scan_idle))
        # Emit a periodic stats snapshot (rate-limited by the reap interval) for log scraping.
        logger.info("adbc.stats %s", _format_stats(self.stats_snapshot()))

    def stats_snapshot(self) -> dict[str, int]:
        """Return the engine's lifecycle counters plus live gauges (for logs / metrics)."""
        from vgi_adbc.functions import active_scan_count

        with self._registry_lock:
            active_sessions = len(self._sessions)
        return self.stats.snapshot(active_sessions=active_sessions, active_scans=active_scan_count())

    def maybe_refresh(self, session: AdbcSession) -> None:
        """Re-read the remote catalog if the session's refresh interval has elapsed.

        No-op when ``refresh_seconds`` is 0 (the snapshot is kept stable). Called at
        transaction boundaries (via ``catalog_version``), never mid-scan.

        Args:
            session: The live session.

        """
        interval = session.target.refresh_seconds
        if interval <= 0:
            return
        if (time.monotonic() - session.last_refresh) < interval:
            return
        try:
            self.refresh(session)
        except Exception:  # noqa: BLE001 - keep serving the cached catalog on a transient blip
            # Advance the clock so a remote outage doesn't retry on every catalog_version
            # call (a refresh storm that would fail otherwise-cacheable queries).
            session.last_refresh = time.monotonic()
            self.stats.incr("refresh_failures")
            logger.warning("adbc.refresh failed; serving cached catalog", exc_info=True)

    def refresh(self, session: AdbcSession) -> int:
        """Re-read the remote catalog, bumping the catalog version if it changed.

        ``adbc_scanner`` snapshots the remote catalog at ATTACH, so observing remote
        DDL means re-attaching. To stay safe against concurrent scans (which stream
        from cursors on the live connection), this opens a **fresh** connection with
        the remote attached and atomically swaps it in — rather than detaching the
        live connection out from under in-flight scans. In-flight cursors keep their
        own reference to the previous connection's database until they finish; the
        previous connection is closed once the swap completes.

        Args:
            session: The live session.

        Returns:
            The (possibly bumped) catalog version.

        """
        new_con = self._new_connection()
        try:
            new_con.execute(self._attach_sql(session.target))
            new_fingerprint = self._fingerprint(new_con)
        except Exception:
            _safe_close(new_con)
            raise

        with session.lifecycle_lock:
            session.last_refresh = time.monotonic()
            if new_fingerprint == session.fingerprint:
                # Nothing changed — keep the current connection, discard the probe.
                _safe_close(new_con)
                return session.catalog_version
            previous_con = session.con
            session.con = new_con
            session.fingerprint = new_fingerprint
            session.schema_cache.clear()
            session.count_cache.clear()
            session.catalog_version += 1
            logger.info("adbc.refresh changed driver=%s version=%s", session.target.driver, session.catalog_version)
        # Close the previous connection outside the lock. Independent cursors created
        # from it keep the underlying database alive until they finish streaming.
        _safe_close(previous_con)
        return session.catalog_version

    def get(self, session_id: bytes) -> AdbcSession:
        """Look up a live session by id.

        Args:
            session_id: The 16-byte session id.

        Returns:
            The live :class:`AdbcSession`.

        Raises:
            SessionNotFoundError: If no session with that id lives in this process.

        """
        with self._registry_lock:
            session = self._sessions.get(session_id)
        if session is None:
            raise SessionNotFoundError(self._session_missing_msg(session_id))
        session.last_used = time.monotonic()
        return session

    @staticmethod
    def _session_missing_msg(session_id: bytes) -> str:
        """Build the error text for a session that isn't held by this worker process."""
        return (
            f"ADBC attach session {session_id.hex()[:12]} is not held by this worker process. Each "
            "attached connection lives only in the process that served its ATTACH (strictly sticky "
            "sessions) and is not rebuilt elsewhere — a fresh connection would silently lose any "
            "transaction, temp tables, or session state. This request reached a different process, or "
            "the worker was restarted. Re-ATTACH the database to recover. On HTTP transport, enable "
            "load-balancer session affinity; on subprocess transport the VGI worker pool can spawn "
            "additional worker processes (notably after a prior error)."
        )

    def detach(self, session_id: bytes) -> None:
        """Close and forget a session, if present (idempotent).

        Releases the session's in-flight scan readers first so their cursors are gone
        before the connection is torn down (they pin it), then closes the connection.

        Args:
            session_id: The 16-byte session id.

        """
        with self._registry_lock:
            session = self._sessions.pop(session_id, None)
        if session is not None:
            _release_session_scans(session_id)
            with session.lifecycle_lock:
                _close_session(session)
            self.stats.incr("detaches")
            logger.info("adbc.detach driver=%s", session.target.driver)

    def close_all(self) -> None:
        """Detach and close every live session (idempotent).

        Registered as an ``atexit`` handler so the worker releases its ADBC
        connections during interpreter shutdown — which runs *before* C++ static
        destructors (``__cxa_finalize``). Draining the connections here means
        ``adbc_scanner``'s global ``ConnectionRegistry`` / connection pools are empty
        by the time their static destructors run, avoiding a throwing-destructor abort
        at process exit (observed with the community ``adbc_scanner`` build). Also
        releases any in-flight scan readers first, since they hold query results that
        pin pooled connections.
        """
        try:
            from vgi_adbc.functions import release_all_scans

            release_all_scans()
        except Exception:  # noqa: BLE001 - best-effort teardown
            logger.debug("error releasing scan readers at shutdown", exc_info=True)
        with self._registry_lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
        for session in sessions:
            with session.lifecycle_lock:
                _close_session(session)

    # -- introspection ---------------------------------------------------------------

    def schemas(self, session: AdbcSession) -> list[str]:
        """List user schemas in the attached remote catalog.

        Args:
            session: The live session.

        Returns:
            Schema names (system schemas excluded), sorted.

        """
        rows = self._fetchall(
            session,
            "SELECT schema_name FROM information_schema.schemata "
            f"WHERE catalog_name = {quote_string(_REMOTE)} ORDER BY schema_name",
        )
        return [row[0] for row in rows if row[0] not in _SYSTEM_SCHEMAS]

    def tables(self, session: AdbcSession, schema: str) -> list[tuple[str, str]]:
        """List tables and views in a remote schema.

        Args:
            session: The live session.
            schema: The remote schema name.

        Returns:
            ``(name, table_type)`` pairs where ``table_type`` is DuckDB's
            information_schema value (``BASE TABLE`` or ``VIEW``), sorted by name.

        """
        rows = self._fetchall(
            session,
            "SELECT table_name, table_type FROM information_schema.tables "
            f"WHERE table_catalog = {quote_string(_REMOTE)} AND table_schema = {quote_string(schema)} "
            "ORDER BY table_name",
        )
        return [(row[0], row[1]) for row in rows]

    def view_definition(self, session: AdbcSession, schema: str, name: str) -> str | None:
        """Return a view's SQL definition, if the remote exposes one.

        Args:
            session: The live session.
            schema: The remote schema name.
            name: The view name.

        Returns:
            The view definition SQL, or None when unavailable.

        """
        rows = self._fetchall(
            session,
            "SELECT view_definition FROM information_schema.views "
            f"WHERE table_catalog = {quote_string(_REMOTE)} "
            f"AND table_schema = {quote_string(schema)} AND table_name = {quote_string(name)}",
        )
        if rows and rows[0][0]:
            return str(rows[0][0])
        return None

    def arrow_schema(
        self,
        session: AdbcSession,
        schema: str,
        table: str,
        columns: list[str] | None = None,
    ) -> pa.Schema:
        """Fetch the Arrow schema of a remote relation (optionally projected).

        Runs a zero-row scan so ``adbc_scanner`` reports exact Arrow types
        (timestamps with unit/timezone, decimals, lists, ...).

        Args:
            session: The live session.
            schema: The remote schema name.
            table: The remote table/view name.
            columns: Projected column names, or None for all columns.

        Returns:
            The relation's (projected) Arrow schema.

        """
        cache_key = (schema, table)
        if columns is None:
            cached = session.schema_cache.get(cache_key)
            if cached is not None:
                return cached
        rel = qualified_relation(_REMOTE, schema, table)
        sql = f"SELECT {projection_list(columns)} FROM {rel} LIMIT 0"
        reader = self._reader(session, sql)
        arrow_schema: pa.Schema = reader.schema
        if columns is None:
            session.schema_cache[cache_key] = arrow_schema
        return arrow_schema

    def row_count(self, session: AdbcSession, schema: str, table: str) -> int | None:
        """Return a cached cardinality estimate for a remote relation, for planning.

        DuckDB plans joins and memory blindly without this (every remote table looks
        like one row). The estimate is cached per session (cleared on refresh) and is
        governed by ``cardinality_mode``:

        * ``off`` — no estimate (returns None; the planner assumes one row).
        * ``estimate`` (default) — cheap driver statistics when available (PostgreSQL
          ``pg_class.reltuples``), else a bounded ``count(*)``.
        * ``count`` — always an exact ``count(*)``.

        A bounded count (``cardinality_max_count`` > 0) counts at most that many rows and
        returns None past the cap, so planning never triggers an unbounded full scan of a
        huge remote table. Returns None whenever an estimate cannot be obtained cheaply.

        Args:
            session: The live session.
            schema: The remote schema name.
            table: The remote table/view name.

        Returns:
            The estimated row count, or None.

        """
        if self._cardinality_mode == "off":
            return None
        cache_key = (schema, table)
        cached = session.count_cache.get(cache_key)
        if cached is not None:
            return cached
        count = self._estimate_cardinality(session, schema, table)
        if count is not None:
            session.count_cache[cache_key] = count
        return count

    def _estimate_cardinality(self, session: AdbcSession, schema: str, table: str) -> int | None:
        """Compute a cardinality estimate per the configured policy (uncached)."""
        if self._cardinality_mode == "estimate":
            est = self._pg_reltuples(session, schema, table)
            if est is not None:
                return est
        rel = qualified_relation(_REMOTE, schema, table)
        cap = self._cardinality_max_count
        try:
            if cap > 0:
                rows = self._fetchall(session, f"SELECT count(*) FROM (SELECT 1 FROM {rel} LIMIT {cap + 1}) AS _c")
                if not rows or rows[0][0] is None:
                    return None
                counted = int(rows[0][0])
                # Past the cap the true count is unknown-but-large; don't pretend precision.
                return None if counted > cap else counted
            rows = self._fetchall(session, f"SELECT count(*) FROM {rel}")
        except Exception:  # noqa: BLE001 - cardinality is best-effort
            self.stats.incr("cardinality_errors")
            logger.debug("could not count %s.%s for cardinality", schema, table, exc_info=True)
            return None
        if not rows or rows[0][0] is None:
            return None
        return int(rows[0][0])

    def _pg_reltuples(self, session: AdbcSession, schema: str, table: str) -> int | None:
        """Return PostgreSQL's cheap planner row estimate (``pg_class.reltuples``), or None.

        Read natively through the read-only raw handle. ``reltuples`` is -1 for a relation
        that has never been analyzed, which we treat as "no estimate".

        Args:
            session: The live session.
            schema: The remote schema name.
            table: The remote table/view name.

        Returns:
            The estimated row count, or None (non-PostgreSQL, never analyzed, or error).

        """
        if "postgres" not in session.target.driver.strip().lower():
            return None
        regclass = quote_string(f"{quote_identifier(schema)}.{quote_identifier(table)}")
        sql = f"SELECT reltuples::BIGINT AS n FROM pg_class WHERE oid = {regclass}::regclass"
        try:
            rows = self.raw_query(session, sql).read_all().to_pylist()
        except Exception:  # noqa: BLE001 - stats are best-effort
            logger.debug("pg reltuples estimate unavailable for %s.%s", schema, table, exc_info=True)
            return None
        if rows and rows[0].get("n") is not None:
            n = int(rows[0]["n"])
            return n if n >= 0 else None
        return None

    # -- raw passthrough query -------------------------------------------------------

    def _adbc_connect_struct(self, target: AdbcTarget) -> str:
        """Render the DuckDB STRUCT literal passed to ``adbc_connect`` for raw queries.

        Args:
            target: The resolved connection parameters.

        Returns:
            A ``{'driver': ..., 'uri': ..., ...}`` STRUCT literal with values quoted.

        """
        entries = [
            f"'driver': {quote_string(target.driver)}",
            f"'uri': {quote_string(target.location)}",
        ]
        if target.entrypoint:
            entries.append(f"'entrypoint': {quote_string(target.entrypoint)}")
        for key, value in target.options.items():
            entries.append(f"{quote_string(validate_option_key(key))}: {quote_string(value)}")
        return "{" + ", ".join(entries) + "}"

    def _ensure_query_handle(self, session: AdbcSession) -> int:
        """Return the session's raw ADBC connection handle, opening it on first use.

        The handle lives in ``adbc_scanner``'s process-global connection registry, so it
        survives a refresh connection-swap. It is released (``adbc_disconnect``) when the
        session is closed.

        Under a **read-only** attach the freshly-opened handle is *sealed* read-only (a
        driver-specific statement run once via ``adbc_execute``) so native passthrough SQL
        cannot mutate the source — the ATTACH ``READ_ONLY`` flag does not reach this
        separate connection. A driver with no known seal is refused here rather than
        silently permitting writes.

        Args:
            session: The live session.

        Returns:
            The ADBC connection handle.

        Raises:
            ValueError: If the attach is read-only but the driver has no known read-only
                seal, so ``adbc_query`` cannot be offered safely.

        """
        with session.query_lock:
            if session.query_handle is not None:
                return session.query_handle
            target = session.target
            seal: str | None = None
            if target.read_only:
                seal = _readonly_seal_sql(target.driver)
                if seal is None:
                    msg = (
                        f"adbc_query is unavailable for driver {target.driver!r} under a read-only attach: "
                        "a read-only guarantee cannot be enforced on the raw passthrough connection for this "
                        "driver, and writes must not be silently allowed. Attach with read_only 'false' to "
                        "enable adbc_query (this permits writes through it), or use a driver with read-only "
                        f"passthrough support ({', '.join(sorted(set(_READONLY_SEAL)))})."
                    )
                    raise ValueError(msg)
            struct = self._adbc_connect_struct(target)
            cursor = session.con.cursor()
            try:
                row = cursor.execute(f"SELECT adbc_connect({struct})").fetchone()
                handle = int(row[0])
                if seal is not None:
                    try:
                        cursor.execute(f"SELECT adbc_execute({handle}, {quote_string(seal)})")
                    except Exception:
                        # Never hand back a handle we could not seal read-only.
                        self._disconnect_handle(session.con, handle)
                        raise
                # Best-effort per-statement timeout on the raw handle (PostgreSQL). Scans
                # go through the ATTACH connection instead; bound those with the server's
                # statement_timeout on the connecting role.
                timeout_sql = self._statement_timeout_sql(target.driver)
                if timeout_sql is not None:
                    try:
                        cursor.execute(f"SELECT adbc_execute({handle}, {quote_string(timeout_sql)})")
                    except Exception:  # noqa: BLE001 - timeout is best-effort
                        logger.debug("could not set statement timeout on raw handle", exc_info=True)
            finally:
                _safe_close(cursor)
            session.query_handle = handle
            logger.info("adbc.query_connect driver=%s read_only=%s", target.driver, target.read_only)
            return handle

    def _statement_timeout_sql(self, driver: str) -> str | None:
        """Return the raw-handle statement-timeout statement for ``driver``, or None.

        Only PostgreSQL is supported (``SET statement_timeout``); other drivers return
        None and rely on server/driver-side configuration.

        Args:
            driver: The ADBC driver name.

        Returns:
            The ``SET statement_timeout = <ms>`` statement, or None.

        """
        if self._query_timeout <= 0 or "postgres" not in driver.strip().lower():
            return None
        return f"SET statement_timeout = {int(self._query_timeout * 1000)}"

    @staticmethod
    def _disconnect_handle(con: Any, handle: int) -> None:
        """Release a raw ADBC handle (``adbc_disconnect``), swallowing errors.

        Args:
            con: The DuckDB connection the handle was opened on.
            handle: The ADBC connection handle to disconnect.

        """
        try:
            con.execute(f"SELECT adbc_disconnect({int(handle)})")
        except Exception:  # noqa: BLE001 - handle already gone / connection dead
            logger.debug("ignored error disconnecting raw query handle", exc_info=True)

    def raw_query_schema(self, session: AdbcSession, sql: str) -> pa.Schema:
        """Return the Arrow schema of a raw remote query without fetching rows.

        Args:
            session: The live session.
            sql: The native SQL to run on the remote database.

        Returns:
            The query's Arrow output schema.

        """
        handle = self._ensure_query_handle(session)
        probe = f"SELECT * FROM adbc_scan({handle}, {quote_string(sql)}) LIMIT 0"
        schema: pa.Schema = self._reader(session, probe).schema
        return schema

    def raw_query(self, session: AdbcSession, sql: str, batch_size: int | None = None) -> Any:
        """Stream the results of a raw remote query (native SQL, run on the remote).

        Args:
            session: The live session.
            sql: The native SQL to run on the remote database.
            batch_size: Optional rows-per-batch hint.

        Returns:
            A ``pyarrow.RecordBatchReader`` streaming the result.

        """
        handle = self._ensure_query_handle(session)
        query = f"SELECT * FROM adbc_scan({handle}, {quote_string(sql)})"
        return self._reader(session, query, None, batch_size)

    # -- scanning --------------------------------------------------------------------

    def scan(
        self,
        session: AdbcSession,
        schema: str,
        table: str,
        columns: list[str] | None,
        where_clause: str,
        params: list[Any],
        batch_size: int | None = None,
    ) -> Any:
        """Stream a remote relation with projection + filter pushdown.

        Args:
            session: The live session.
            schema: The remote schema name.
            table: The remote table/view name.
            columns: Projected column names (output order), or None for all.
            where_clause: A DuckDB WHERE clause body (no ``WHERE`` keyword), or "".
            params: Positional parameters (``?`` placeholders) for ``where_clause``.
            batch_size: Optional rows-per-batch hint.

        Returns:
            A ``pyarrow.RecordBatchReader`` streaming the result.

        """
        rel = qualified_relation(_REMOTE, schema, table)
        sql = f"SELECT {projection_list(columns)} FROM {rel}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        return self._reader(session, sql, params or None, batch_size)

    # -- low-level execution ---------------------------------------------------------

    def _fetchall(self, session: AdbcSession, sql: str, params: list[Any] | None = None) -> list[tuple[Any, ...]]:
        """Execute a query on an independent cursor and return all rows.

        Args:
            session: The live session.
            sql: The SQL to run.
            params: Optional positional parameters.

        Returns:
            All result rows as tuples.

        """
        with session.lifecycle_lock:
            cursor = session.con.cursor()
        try:
            cursor.execute(sql, params) if params else cursor.execute(sql)
            return list(cursor.fetchall())
        finally:
            _safe_close(cursor)

    def _reader(
        self,
        session: AdbcSession,
        sql: str,
        params: list[Any] | None = None,
        batch_size: int | None = None,
    ) -> Any:
        """Execute a query and return a streaming Arrow record-batch reader.

        The cursor stays open for the lifetime of the returned reader (the reader
        streams from it); it is released when the reader is exhausted/collected.

        Args:
            session: The live session.
            sql: The SQL to run.
            params: Optional positional parameters.
            batch_size: Optional rows-per-batch hint.

        Returns:
            A ``pyarrow.RecordBatchReader``.

        """
        with session.lifecycle_lock:
            cursor = session.con.cursor()
        result = cursor.execute(sql, params) if params else cursor.execute(sql)
        return _to_reader(result, batch_size)


def _to_reader(result: Any, batch_size: int | None = None) -> Any:
    """Adapt a DuckDB result to a ``pyarrow.RecordBatchReader``.

    Prefers the non-deprecated ``to_arrow_reader`` when present, falling back to
    ``fetch_record_batch`` (suppressing its deprecation warning) for older builds.

    Args:
        result: A DuckDB query result object.
        batch_size: Optional rows-per-batch hint.

    Returns:
        A ``pyarrow.RecordBatchReader``.

    """
    if hasattr(result, "to_arrow_reader"):
        return result.to_arrow_reader(batch_size) if batch_size else result.to_arrow_reader()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        return result.fetch_record_batch(batch_size) if batch_size else result.fetch_record_batch()


def _safe_close(closeable: Any) -> None:
    """Close a cursor/connection, swallowing any error.

    Args:
        closeable: An object with a ``close()`` method.

    """
    try:
        closeable.close()
    except Exception:  # noqa: BLE001 - best-effort cleanup
        logger.debug("ignored error closing DuckDB handle", exc_info=True)


def _release_session_scans(session_id: bytes) -> None:
    """Release a session's in-flight scan readers before its connection is closed.

    Args:
        session_id: The session whose readers to release.

    """
    try:
        from vgi_adbc.functions import release_scans_for_session

        release_scans_for_session(session_id)
    except Exception:  # noqa: BLE001 - best-effort teardown
        logger.debug("error releasing session scan readers", exc_info=True)


def _close_session(session: AdbcSession) -> None:
    """Detach the remote and close a session's DuckDB connection (best-effort).

    Detaching first lets ``adbc_scanner`` release the remote's ADBC connections through
    the normal path before the DuckDB instance is torn down.

    Args:
        session: The session to close.

    """
    if session.query_handle is not None:
        AdbcEngine._disconnect_handle(session.con, session.query_handle)
        session.query_handle = None
    try:
        session.con.execute(f"DETACH {quote_identifier(_REMOTE)}")
    except Exception:  # noqa: BLE001 - already detached / connection dead
        logger.debug("ignored error detaching remote at close", exc_info=True)
    _safe_close(session.con)


_engine: AdbcEngine | None = None
_engine_lock = threading.Lock()


def get_engine() -> AdbcEngine:
    """Return the process-wide engine singleton, creating it on first use.

    Returns:
        The shared :class:`AdbcEngine`.

    """
    global _engine  # noqa: PLW0603 - intentional process-wide singleton
    if _engine is None:
        with _engine_lock:
            if _engine is None:
                _engine = AdbcEngine(**_engine_config_from_env())
                # Release ADBC connections during interpreter shutdown, before C++
                # static destructors run — see AdbcEngine.close_all.
                atexit.register(_engine.close_all)
                # Opt-in Prometheus /metrics endpoint (VGI_ADBC_METRICS_PORT); no-op otherwise.
                from vgi_adbc.metrics import maybe_start_metrics_server

                maybe_start_metrics_server(_engine.stats_snapshot)
    return _engine


def _engine_config_from_env() -> dict[str, Any]:
    """Read the engine's optional tuning knobs from the environment.

    All are optional with production-safe defaults; see :class:`AdbcEngine`.

    Returns:
        Keyword arguments for :class:`AdbcEngine`.

    """
    import os

    def _f(name: str, default: float) -> float:
        raw = os.environ.get(name)
        if not raw:
            return default
        try:
            return float(raw)
        except ValueError:
            logger.warning("ignoring non-numeric %s=%r", name, raw)
            return default

    def _i(name: str, default: int) -> int:
        return int(_f(name, default))

    mode = (os.environ.get("VGI_ADBC_CARDINALITY") or "estimate").strip().lower()
    if mode not in ("estimate", "count", "off"):
        logger.warning("ignoring unknown VGI_ADBC_CARDINALITY=%r (using 'estimate')", mode)
        mode = "estimate"
    # Resource-safe defaults so a crashed/disconnected client can't leak a session (a whole
    # DuckDB instance + ADBC connection) or an abandoned scan reader forever:
    #  - session idle 1h: any query resets the timer (get() stamps last_used), so only a truly
    #    dormant attach is evicted; the client transparently re-ATTACHes on next use.
    #  - scan idle 5m: far longer than any real inter-batch gap, so a live scan is never reaped
    #    mid-stream (a reaped reader would raise ScanInterruptedError). Set either to 0 to disable.
    return {
        "extension_path": os.environ.get("VGI_ADBC_EXTENSION_PATH") or None,
        "session_idle_seconds": _f("VGI_ADBC_SESSION_IDLE_SECONDS", 3600.0),
        "scan_idle_seconds": _f("VGI_ADBC_SCAN_IDLE_SECONDS", 300.0),
        "max_sessions": _i("VGI_ADBC_MAX_SESSIONS", 0),
        "max_scans": _i("VGI_ADBC_MAX_SCANS", 0),
        "reap_interval_seconds": _f("VGI_ADBC_REAP_INTERVAL_SECONDS", 60.0),
        "query_timeout_seconds": _f("VGI_ADBC_QUERY_TIMEOUT_SECONDS", 0.0),
        "cardinality_mode": mode,
        "cardinality_max_count": _i("VGI_ADBC_CARDINALITY_MAX_COUNT", 0),
    }
