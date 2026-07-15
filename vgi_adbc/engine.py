# Copyright 2026 Query Farm LLC - https://query.farm

"""The embedded DuckDB engine that talks to remote ADBC databases.

The worker runs its ADBC queries inside an in-process ``haybarn`` DuckDB (a
duckdb-python drop-in, pinned recent so its community extension repository serves
a recent ``adbc_scanner`` build). Each attached remote database gets its own
DuckDB instance with ``adbc_scanner`` loaded and the remote ``ATTACH``-ed under
the local alias ``remote`` — so the worker queries it with ordinary DuckDB SQL
(``SELECT ... FROM remote.<schema>.<table>``) and ``adbc_scanner`` re-dialects and
pushes predicates/projection down to PostgreSQL, SQLite, or any other ADBC target.

**Sessions + stateless reconnect.** A live ADBC connection is a native, process-local
object; it can't be serialized into a VGI state token. Each attach is held in *this*
process, keyed by a random session id. The VGI worker pool (subprocess *and* HTTP) can
route a follow-up call to a sibling/fresh process — notably after any worker error — so the
session id alone is not enough. Instead the catalog packs ``session_id || json(target)`` (the
reconnect recipe) into ``attach_opaque_data`` (:func:`encode_attach_identity`), which the
framework AEAD-seals per-principal on HTTP. On a registry miss, :meth:`AdbcEngine.resolve`
rebuilds the session from that sealed target (:meth:`AdbcEngine._reconnect_session`), so an
attachment survives a process hop; ``reconnect=False`` reverts to raising
:class:`SessionNotFoundError`. A *live streaming scan* cannot resume across a hop — that is
enforced loudly at the scan layer (``ScanInterruptedError``), not silently restarted.
"""

from __future__ import annotations

import atexit
import hashlib
import json
import logging
import threading
import time
import uuid
import warnings
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
    """Resolve the DuckDB connect function, preferring haybarn.

    Returns:
        ``haybarn.connect`` when available (recent DuckDB + recent adbc_scanner),
        otherwise ``duckdb.connect`` as a fallback.

    Raises:
        RuntimeError: If neither ``haybarn`` nor ``duckdb`` can be imported.

    """
    try:
        import haybarn

        return haybarn.connect
    except ImportError:
        pass
    try:
        import duckdb

        return duckdb.connect  # type: ignore[no-any-return]
    except ImportError as exc:  # pragma: no cover - environment misconfiguration
        msg = "Neither 'haybarn' nor 'duckdb' is importable; install vgi-adbc's dependencies."
        raise RuntimeError(msg) from exc


class SessionNotFoundError(RuntimeError):
    """Raised when an attach session isn't held here and can't be reconnected.

    A session normally rebuilds itself from its sealed target when a worker-pool process hop
    routes a call to a fresh process (:meth:`AdbcEngine.resolve`). This is raised only when
    that can't happen — reconnect is disabled (``VGI_ADBC_RECONNECT=0``) or the token is a
    legacy id-only one with no embedded target. Recover by re-``ATTACH``-ing the database.
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


# The attach identity carried in ``attach_opaque_data`` is ``session_id(16) || json(target)``
# — the random per-attach id plus the recipe to rebuild the connection. The VGI framework
# AEAD-seals this per-principal on HTTP (so credentials never leave the worker in the clear
# there) and passes it through on the local stdio pipe. Any pool worker that receives a call
# for a session it doesn't hold reconstructs it from this recipe (see ``AdbcEngine.resolve``).
_SESSION_ID_LEN = 16


def encode_attach_identity(session_id: bytes, target: AdbcTarget) -> bytes:
    """Serialize ``(session_id, target)`` into the attach-identity bytes.

    Args:
        session_id: The 16-byte per-attach id.
        target: The resolved connection parameters (the reconnect recipe).

    Returns:
        ``session_id || json(target)`` bytes.

    """
    payload = {
        "d": target.driver,
        "l": target.location,
        "e": target.entrypoint,
        "o": target.options,
        "r": target.read_only,
        "rs": target.refresh_seconds,
    }
    return session_id + json.dumps(payload, separators=(",", ":")).encode("utf-8")


def decode_attach_identity(blob: bytes) -> tuple[bytes, AdbcTarget | None]:
    """Parse attach-identity bytes back into ``(session_id, target)``.

    Tolerates a legacy 16-byte-only token (returns ``target=None``, so the caller falls back
    to strict lookup without reconnect).

    Args:
        blob: The attach-identity bytes from ``attach_opaque_data``.

    Returns:
        ``(session_id, target)``; ``target`` is None for a legacy/opaque token.

    """
    session_id = bytes(blob[:_SESSION_ID_LEN])
    rest = bytes(blob[_SESSION_ID_LEN:])
    if not rest:
        return session_id, None
    try:
        payload = json.loads(rest.decode("utf-8"))
        target = AdbcTarget(
            driver=payload["d"],
            location=payload["l"],
            entrypoint=payload["e"],
            options=dict(payload["o"]),
            read_only=payload["r"],
            refresh_seconds=payload["rs"],
        )
    except (ValueError, KeyError, TypeError):
        logger.debug("could not decode attach target; treating as opaque", exc_info=True)
        return session_id, None
    return session_id, target


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
        reconnect: bool = True,
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
            reconnect: When True (default), rebuild a session from its sealed target on a
                registry miss (so an attachment survives a worker-pool process hop/respawn),
                rather than raising. Streaming scans still can't resume across a hop.

        """
        self._connect_fn = connect_fn or _default_connect()
        self._extension_path = extension_path
        self._sessions: dict[bytes, AdbcSession] = {}
        self._registry_lock = threading.Lock()
        self._install_lock = threading.Lock()
        self._installed = False
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
        # Stateless reconnect: rebuild a session from its sealed target on a miss.
        self._reconnect = reconnect

    @property
    def reconnect_enabled(self) -> bool:
        """Whether a missing session is rebuilt from its sealed target (vs raising)."""
        return self._reconnect

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
        session = self._build_session(session_id, target)
        with self._registry_lock:
            self._sessions[session_id] = session
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
        """Return the session for an attachment, reconnecting from its sealed target on a miss.

        This is the entry point catalog/function calls use (they carry the sealed attach
        identity). When the session isn't held by *this* worker process — because the
        worker-pool routed the call to a sibling/fresh process — it is rebuilt from the
        target packed into ``attach_opaque_data`` (unless reconnect is disabled or the token
        is a legacy id-only one), so an attachment survives a process hop. A live streaming
        scan still cannot resume across a hop; that is handled at the scan layer.

        Args:
            attach_opaque_data: The attach identity bytes (``session_id || json(target)``).

        Returns:
            The live (possibly just-reconnected) :class:`AdbcSession`.

        Raises:
            SessionNotFoundError: If the session is absent and cannot be reconnected.

        """
        if not attach_opaque_data:
            msg = "ADBC function invoked without an attachment; ATTACH the catalog first."
            raise ValueError(msg)
        blob = bytes(attach_opaque_data)
        # Fast path: only the 16-byte id is needed for a registry hit — don't decode the
        # (JSON) target on every call, only on a miss when we actually reconnect.
        session_id = blob[:_SESSION_ID_LEN]
        with self._registry_lock:
            session = self._sessions.get(session_id)
        if session is not None:
            session.last_used = time.monotonic()
            return session
        _session_id, target = decode_attach_identity(blob)
        if not self._reconnect or target is None:
            raise SessionNotFoundError(self._session_missing_msg(session_id))
        return self._reconnect_session(session_id, target)

    def _reconnect_session(self, session_id: bytes, target: AdbcTarget) -> AdbcSession:
        """Rebuild a session from its target under the same id (idempotent across threads)."""
        self.reap()  # reclaim idle sessions before rebuilding one
        session = self._build_session(session_id, target)
        with self._registry_lock:
            existing = self._sessions.get(session_id)
            if existing is None:
                self._sessions[session_id] = session
        if existing is not None:
            # A concurrent call already rebuilt it; discard our duplicate.
            with session.lifecycle_lock:
                _close_session(session)
            existing.last_used = time.monotonic()
            return existing
        logger.info("adbc.reconnect driver=%s sessions=%s", target.driver, len(self._sessions))
        return session

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
                self.detach(sid)
        if self._scan_idle > 0:
            from vgi_adbc.functions import reap_idle_scans

            reap_idle_scans(self._scan_idle)

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
        """Build the error text for a session that isn't held and can't be reconnected."""
        return (
            f"ADBC attach session {session_id.hex()[:12]} is not held by this worker process and "
            "could not be reconnected (reconnect disabled, or a legacy id-only attach token). Each "
            "attachment normally rebuilds itself from its sealed target when a worker-pool process "
            "hop routes a call to a fresh process; when it can't, re-ATTACH to recover."
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
    reconnect = (os.environ.get("VGI_ADBC_RECONNECT") or "1").strip().lower() not in ("0", "false", "no")
    # With reconnect on, an idle-evicted session is transparently rebuilt on next use, so idle
    # eviction is safe to enable by default — it also bounds the connection copies a worker-pool
    # process hop can leave behind. Disable by setting VGI_ADBC_SESSION_IDLE_SECONDS=0.
    session_idle_default = 900.0 if reconnect else 0.0
    return {
        "extension_path": os.environ.get("VGI_ADBC_EXTENSION_PATH") or None,
        "session_idle_seconds": _f("VGI_ADBC_SESSION_IDLE_SECONDS", session_idle_default),
        "scan_idle_seconds": _f("VGI_ADBC_SCAN_IDLE_SECONDS", 0.0),
        "max_sessions": _i("VGI_ADBC_MAX_SESSIONS", 0),
        "max_scans": _i("VGI_ADBC_MAX_SCANS", 0),
        "reap_interval_seconds": _f("VGI_ADBC_REAP_INTERVAL_SECONDS", 60.0),
        "query_timeout_seconds": _f("VGI_ADBC_QUERY_TIMEOUT_SECONDS", 0.0),
        "cardinality_mode": mode,
        "cardinality_max_count": _i("VGI_ADBC_CARDINALITY_MAX_COUNT", 0),
        "reconnect": reconnect,
    }
