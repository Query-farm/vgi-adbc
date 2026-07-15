"""Unit tests for the stats counters and the Prometheus rendering (no network)."""

from __future__ import annotations

from vgi_adbc.engine import EngineStats, _format_stats
from vgi_adbc.metrics import render_prometheus


def test_engine_stats_counters_and_snapshot() -> None:
    stats = EngineStats()
    stats.incr("attaches")
    stats.incr("attaches")
    stats.incr("scans_opened", 3)
    stats.incr("detaches", 0)  # no-op
    stats.incr("scans_reaped", -1)  # no-op (negative)
    snap = stats.snapshot(active_sessions=2, active_scans=1)
    assert snap["attaches"] == 2
    assert snap["scans_opened"] == 3
    assert "detaches" not in snap
    assert "scans_reaped" not in snap
    assert snap["active_sessions"] == 2
    assert snap["active_scans"] == 1


def test_format_stats_is_stable_and_sorted() -> None:
    s = _format_stats({"b": 2, "a": 1, "active_sessions": 5})
    assert s == "a=1 active_sessions=5 b=2"


def test_render_prometheus_counter_vs_gauge() -> None:
    out = render_prometheus({"attaches": 7, "active_sessions": 3, "active_scans": 1})
    # Counters get the _total suffix + counter TYPE; gauges do not.
    assert "# TYPE vgi_adbc_attaches_total counter" in out
    assert "vgi_adbc_attaches_total 7" in out
    assert "# TYPE vgi_adbc_active_sessions gauge" in out
    assert "vgi_adbc_active_sessions 3" in out
    assert "vgi_adbc_active_scans 1" in out
    # Well-formed Prometheus text ends with a newline.
    assert out.endswith("\n")


def test_metrics_server_serves_scrape(monkeypatch) -> None:  # noqa: ANN001
    import socket
    import urllib.request

    import vgi_adbc.metrics as metrics

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    monkeypatch.setattr(metrics, "_server", None)  # ignore any server from another test
    monkeypatch.setenv("VGI_ADBC_METRICS_PORT", str(port))
    monkeypatch.setenv("VGI_ADBC_METRICS_HOST", "127.0.0.1")
    started = metrics.maybe_start_metrics_server(lambda: {"attaches": 4, "active_sessions": 1, "active_scans": 0})
    assert started is True
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=3) as resp:  # noqa: S310
            body = resp.read().decode()
        assert "vgi_adbc_attaches_total 4" in body
        assert "vgi_adbc_active_sessions 1" in body
    finally:
        if metrics._server is not None:
            metrics._server.shutdown()
            metrics._server = None


def test_metrics_server_disabled_without_port(monkeypatch) -> None:  # noqa: ANN001
    import vgi_adbc.metrics as metrics

    monkeypatch.setattr(metrics, "_server", None)
    monkeypatch.delenv("VGI_ADBC_METRICS_PORT", raising=False)
    assert metrics.maybe_start_metrics_server(dict) is False
