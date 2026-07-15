#!/usr/bin/env python3
# Copyright 2026 Query Farm LLC - https://query.farm
"""Benchmark the vgi-adbc worker end-to-end through a real DuckDB client.

Drives the worker the way a real client does — a DuckDB (``haybarn``) session with the
``vgi`` extension that ``ATTACH``es the worker and queries it — over **both** transports
(stdio subprocess and HTTP) against SQLite and/or PostgreSQL, timing a representative
query suite (full scan, projection, selective/broad filter pushdown, ``count(*)``,
``GROUP BY``, and the ``adbc_query`` passthrough) plus the cold ATTACH cost.

Results print as a Markdown table and are written as JSON + Markdown under ``--out``.

Example::

    uv run python bench/run_bench.py \\
        --sqlite /tmp/bench.sqlite --transport both --iters 20 --out bench/results
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# (name, sql_template) — {rel} is the attached relation, {qrel} the adbc_query relation,
# {mid} the id used for the selective (single-row) pushdown probe.
QUERIES: list[tuple[str, str]] = [
    ("full_scan", "SELECT * FROM {rel}"),
    ("projection", "SELECT id, name FROM {rel}"),
    ("filter_selective", "SELECT * FROM {rel} WHERE id = {mid}"),
    ("filter_broad", "SELECT * FROM {rel} WHERE active = 1"),
    ("count_star", "SELECT count(*) FROM {rel}"),
    ("group_by", "SELECT dept, count(*), avg(salary) FROM {rel} GROUP BY dept ORDER BY dept"),
    ("adbc_query", "SELECT count(*) FROM {qrel}('SELECT * FROM bench')"),
]


@dataclass
class QueryStat:
    name: str
    rows: int
    p50_ms: float
    p95_ms: float
    min_ms: float
    rows_per_s: float


@dataclass
class RunResult:
    transport: str
    target: str
    attach_ms: float
    queries: list[QueryStat] = field(default_factory=list)


def _client():  # noqa: ANN202 - haybarn connection
    import haybarn

    con = haybarn.connect(config={"allow_unsigned_extensions": "true"})
    try:
        con.execute("LOAD vgi")
    except Exception:  # noqa: BLE001 - not installed yet
        con.execute("INSTALL vgi FROM community")
        con.execute("LOAD vgi")
    return con


def _percentile(samples: list[float], q: float) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    idx = min(len(ordered) - 1, int(round(q * (len(ordered) - 1))))
    return ordered[idx]


def _time_query(con, sql: str, iters: int) -> tuple[int, list[float]]:  # noqa: ANN001
    # One warm-up (opens the scan, primes caches) that is not counted.
    rows = con.execute(sql).fetchall()
    samples: list[float] = []
    for _ in range(iters):
        t0 = time.perf_counter()
        rows = con.execute(sql).fetchall()
        samples.append((time.perf_counter() - t0) * 1000.0)
    return len(rows), samples


def run_suite(
    con, transport: str, target: str, location: str, driver: str, uri: str, schema: str, rows: int, iters: int
) -> RunResult:  # noqa: ANN001, E501
    rel = f"db.{schema}.bench"
    qrel = f"db.{schema}.adbc_query"
    mid = max(1, rows // 2)

    t0 = time.perf_counter()
    con.execute(f"ATTACH 'adbc' AS db (TYPE vgi, LOCATION '{location}', driver '{driver}', uri '{uri}')")
    # Touch the catalog so the attach/introspection cost is included in attach_ms.
    con.execute("SELECT count(*) FROM duckdb_tables() WHERE database_name = 'db'").fetchall()
    attach_ms = (time.perf_counter() - t0) * 1000.0

    result = RunResult(transport=transport, target=target, attach_ms=attach_ms)
    for name, template in QUERIES:
        sql = template.format(rel=rel, qrel=qrel, mid=mid)
        n_rows, samples = _time_query(con, sql, iters)
        p50 = _percentile(samples, 0.50)
        result.queries.append(
            QueryStat(
                name=name,
                rows=n_rows,
                p50_ms=round(p50, 3),
                p95_ms=round(_percentile(samples, 0.95), 3),
                min_ms=round(min(samples), 3),
                rows_per_s=round((n_rows / (p50 / 1000.0)) if p50 > 0 else 0.0, 1),
            )
        )
    con.execute("DETACH db")
    return result


def _vgi_serve_path() -> str:
    found = shutil.which("vgi-serve")
    if found:
        return found
    candidate = REPO_ROOT / ".venv" / "bin" / "vgi-serve"
    if candidate.exists():
        return str(candidate)
    msg = "vgi-serve not found (install the 'serve' extra: uv sync --extra dev --extra serve)"
    raise RuntimeError(msg)


class HttpServer:
    """A single vgi-serve HTTP worker process (single-process = sticky session survives)."""

    def __init__(self, port: int, log_path: Path) -> None:
        self.port = port
        self.log_path = log_path
        self.proc: subprocess.Popen | None = None

    def __enter__(self) -> HttpServer:
        self._log = self.log_path.open("w")
        self.proc = subprocess.Popen(
            [
                _vgi_serve_path(),
                "vgi_adbc.worker:AdbcWorker",
                "--http",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--quiet",
            ],
            stdout=self._log,
            stderr=subprocess.STDOUT,
            cwd=str(REPO_ROOT),
        )
        self._await_ready()
        return self

    def _await_ready(self, timeout: float = 30.0) -> None:
        deadline = time.time() + timeout
        url = f"http://127.0.0.1:{self.port}/health"
        while time.time() < deadline:
            if self.proc is not None and self.proc.poll() is not None:
                raise RuntimeError(f"HTTP worker exited early; see {self.log_path}")
            try:
                with urllib.request.urlopen(url, timeout=1) as resp:  # noqa: S310 - localhost
                    if resp.status == 200:
                        return
            except Exception:  # noqa: BLE001 - not up yet
                time.sleep(0.2)
        raise RuntimeError(f"HTTP worker not ready on :{self.port} within {timeout}s")

    def __exit__(self, *exc: object) -> None:
        if self.proc is not None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        self._log.close()


def to_markdown(results: list[RunResult], meta: dict) -> str:  # noqa: ANN001
    lines = [
        f"# vgi-adbc benchmark — {meta['rows']:,} rows, {meta['iters']} iterations/query",
        "",
        f"Generated {meta['timestamp']}. Times are per-query wall clock from a DuckDB client "
        "(haybarn + vgi) attached to the worker; `attach_ms` is the one-time cold ATTACH + "
        "catalog introspection.",
        "",
        "| transport | target | attach ms | query | rows | p50 ms | p95 ms | rows/s |",
        "|-----------|--------|-----------|-------|------|--------|--------|--------|",
    ]
    for r in results:
        for i, q in enumerate(r.queries):
            attach = f"{r.attach_ms:.1f}" if i == 0 else ""
            lines.append(
                f"| {r.transport if i == 0 else ''} | {r.target if i == 0 else ''} | {attach} | "
                f"{q.name} | {q.rows:,} | {q.p50_ms:.3f} | {q.p95_ms:.3f} | {q.rows_per_s:,.0f} |"
            )
    return "\n".join(lines) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sqlite", help="path to the SQLite benchmark fixture")
    ap.add_argument("--pg", help="PostgreSQL DSN (postgresql://...) to benchmark")
    ap.add_argument("--transport", choices=["stdio", "http", "both"], default="both")
    ap.add_argument("--iters", type=int, default=20)
    ap.add_argument("--rows", type=int, default=100_000, help="row count (for the report + rows/s)")
    ap.add_argument("--worker", default=str(REPO_ROOT / "bin" / "vgi-adbc-worker"), help="stdio worker binary")
    ap.add_argument("--http-port", type=int, default=8134)
    ap.add_argument("--out", default=str(REPO_ROOT / "bench" / "results"))
    args = ap.parse_args()

    targets: list[tuple[str, str, str, str]] = []  # (target_label, driver, uri, schema)
    if args.sqlite:
        targets.append(("sqlite", "sqlite", str(Path(args.sqlite).resolve()), "main"))
    if args.pg:
        targets.append(("postgresql", "postgresql", args.pg, "public"))
    if not targets:
        print("nothing to benchmark: pass --sqlite and/or --pg", file=sys.stderr)
        raise SystemExit(2)

    transports = ["stdio", "http"] if args.transport == "both" else [args.transport]
    worker = str(Path(args.worker).resolve())
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    results: list[RunResult] = []
    for target_label, driver, uri, schema in targets:
        for transport in transports:
            print(f"==> {transport} / {target_label}", file=sys.stderr)
            if transport == "stdio":
                con = _client()
                try:
                    results.append(
                        run_suite(con, transport, target_label, worker, driver, uri, schema, args.rows, args.iters)
                    )
                finally:
                    con.close()
            else:
                log = out_dir / f"http-{target_label}.log"
                with HttpServer(args.http_port, log):
                    location = f"http://127.0.0.1:{args.http_port}"
                    con = _client()
                    try:
                        results.append(
                            run_suite(
                                con, transport, target_label, location, driver, uri, schema, args.rows, args.iters
                            )
                        )
                    finally:
                        con.close()

    meta = {
        "rows": args.rows,
        "iters": args.iters,
        "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    payload = {"meta": meta, "runs": [asdict(r) for r in results]}
    (out_dir / "results.json").write_text(json.dumps(payload, indent=2))
    md = to_markdown(results, meta)
    (out_dir / "results.md").write_text(md)
    print(md)
    print(f"wrote {out_dir / 'results.json'} and {out_dir / 'results.md'}", file=sys.stderr)


if __name__ == "__main__":
    main()
