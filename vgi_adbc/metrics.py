# Copyright 2026 Query Farm LLC - https://query.farm

"""Optional Prometheus ``/metrics`` endpoint exposing the engine's live stats.

Off by default. Set ``VGI_ADBC_METRICS_PORT`` to serve ``text/plain`` Prometheus metrics
(plus ``/healthz``) from a background daemon thread that reads the engine's stats snapshot.
It is a plain stdlib HTTP server — framework-agnostic, so it runs in the worker's HTTP
server process without touching the VGI app (whose ``create_app`` has no route hook). In
stdio deployments the env var is simply left unset (short-lived per-attach workers).

For request-level tracing/metrics, the base worker also supports OpenTelemetry — see the
README's *Observability* section.
"""

from __future__ import annotations

import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger("vgi_adbc.metrics")

# Live gauges (current value); everything else is a monotonically increasing counter.
_GAUGES = frozenset({"active_sessions", "active_scans"})

_lock = threading.Lock()
_server: ThreadingHTTPServer | None = None


def render_prometheus(snapshot: dict[str, int]) -> str:
    """Render a stats snapshot as Prometheus text (counters get the ``_total`` suffix)."""
    lines: list[str] = []
    for name in sorted(snapshot):
        is_gauge = name in _GAUGES
        metric = f"vgi_adbc_{name}" if is_gauge else f"vgi_adbc_{name}_total"
        lines.append(f"# TYPE {metric} {'gauge' if is_gauge else 'counter'}")
        lines.append(f"{metric} {snapshot[name]}")
    return "\n".join(lines) + "\n"


def maybe_start_metrics_server(snapshot_fn: Callable[[], dict[str, int]]) -> bool:
    """Start the ``/metrics`` server if ``VGI_ADBC_METRICS_PORT`` is set (idempotent).

    Args:
        snapshot_fn: Returns the current stats snapshot to render on each scrape.

    Returns:
        True if a server was started (or already running), False if disabled/failed.

    """
    raw = os.environ.get("VGI_ADBC_METRICS_PORT")
    if not raw:
        return False
    try:
        port = int(raw)
    except ValueError:
        logger.warning("ignoring non-numeric VGI_ADBC_METRICS_PORT=%r", raw)
        return False
    if port <= 0:
        return False
    host = os.environ.get("VGI_ADBC_METRICS_HOST", "0.0.0.0")  # noqa: S104 - scrape target

    global _server  # noqa: PLW0603 - process-wide singleton server
    with _lock:
        if _server is not None:
            return True

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
                path = self.path.split("?", 1)[0].rstrip("/")
                if path in ("/metrics", ""):
                    body = render_prometheus(snapshot_fn()).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                elif path == "/healthz":
                    self.send_response(200)
                    self.send_header("Content-Length", "2")
                    self.end_headers()
                    self.wfile.write(b"ok")
                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, *args: object) -> None:  # keep the scrape server quiet
                del args

        try:
            _server = ThreadingHTTPServer((host, port), _Handler)
        except OSError as exc:
            logger.warning("could not start metrics server on %s:%s: %s", host, port, exc)
            return False
        threading.Thread(target=_server.serve_forever, name="vgi-adbc-metrics", daemon=True).start()
        logger.info("adbc.metrics serving on http://%s:%s/metrics", host, port)
        return True
