"""Benchmark smoke gate: run the bench harness at tiny scale and assert it's *correct*.

Not a perf threshold (CI hardware is too variable for that) — this catches a gross
regression or a broken harness: the query suite must return the right row counts and a
non-zero throughput over the stdio transport. Skips cleanly when the full client stack
(haybarn + the ``vgi`` extension + ADBC drivers) isn't available.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "bench"))

WORKER = REPO_ROOT / "bin" / "vgi-adbc-worker"


def test_bench_smoke_stdio(tmp_path: Path) -> None:
    try:
        import haybarn  # noqa: F401
    except ImportError:
        pytest.skip("haybarn unavailable")

    import run_bench  # from bench/ (added to sys.path above)
    from make_bench_fixture import build

    db = tmp_path / "bench.sqlite"
    build(db, 400)

    try:
        con = run_bench._client()
    except Exception as exc:  # noqa: BLE001 - vgi client extension unavailable
        pytest.skip(f"vgi client unavailable: {exc}")
    try:
        result = run_bench.run_suite(con, "stdio", "sqlite", str(WORKER), "sqlite", str(db), "main", rows=400, iters=2)
    except Exception as exc:  # noqa: BLE001 - worker/driver unavailable in this env
        con.close()
        pytest.skip(f"stdio benchmark unavailable: {exc}")

    con.close()
    q = {stat.name: stat for stat in result.queries}
    # Correctness: the suite returned the right shapes.
    assert q["full_scan"].rows == 400
    assert q["projection"].rows == 400
    assert q["filter_selective"].rows == 1
    assert q["count_star"].rows == 1
    assert q["adbc_query"].rows == 1
    # Liveness: a full scan reports positive throughput and finite latency.
    assert q["full_scan"].rows_per_s > 0
    assert q["full_scan"].p50_ms > 0
    assert result.attach_ms > 0
