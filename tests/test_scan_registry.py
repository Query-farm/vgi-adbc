"""Unit tests for the in-flight scan-reader registry (idle reaping, caps, release).

Pure logic: the registry stores opaque readers, so these use a fake reader and never
touch ADBC.
"""

from __future__ import annotations

import pytest

import vgi_adbc.functions as fn


class FakeReader:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _reset() -> None:
    fn.release_all_scans()


def test_register_get_release_roundtrip() -> None:
    _reset()
    r = FakeReader()
    token = b"tok-1"
    fn._register_reader(token, r, b"sess-1")
    assert fn.active_scan_count() == 1
    assert fn._get_reader(token) is r
    fn._release_reader(token)
    assert fn.active_scan_count() == 0
    assert r.closed is True


def test_release_all_closes_readers() -> None:
    _reset()
    readers = [FakeReader() for _ in range(3)]
    for i, r in enumerate(readers):
        fn._register_reader(f"t{i}".encode(), r, b"s")
    fn.release_all_scans()
    assert fn.active_scan_count() == 0
    assert all(r.closed for r in readers)


def test_reap_idle_scans() -> None:
    _reset()
    old, fresh = FakeReader(), FakeReader()
    fn._register_reader(b"old", old, b"s")
    fn._register_reader(b"fresh", fresh, b"s")
    # Backdate the "old" entry well beyond the idle threshold.
    fn._active_scans[b"old"].last_used -= 1000.0
    reaped = fn.reap_idle_scans(60.0)
    assert reaped == 1
    assert old.closed is True
    assert fn._get_reader(b"fresh") is fresh
    # Disabled (0) reaps nothing.
    assert fn.reap_idle_scans(0.0) == 0


def test_release_scans_for_session() -> None:
    _reset()
    a1, a2, b1 = FakeReader(), FakeReader(), FakeReader()
    fn._register_reader(b"a1", a1, b"sess-a")
    fn._register_reader(b"a2", a2, b"sess-a")
    fn._register_reader(b"b1", b1, b"sess-b")
    released = fn.release_scans_for_session(b"sess-a")
    assert released == 2
    assert a1.closed and a2.closed
    assert not b1.closed
    assert fn.active_scan_count() == 1


def test_scan_interrupted_when_reader_lost_after_open() -> None:
    # If a scan already opened its reader (state.opened) but the reader is gone from this
    # process (a worker-pool hop mid-scan), re-entry must raise rather than silently restart.
    _reset()
    from vgi_adbc.functions import AdbcQuery, AdbcScan, ScanInterruptedError, _ScanState

    state = _ScanState(token=b"lost-token", done=False, opened=True)
    # No reader registered for the token; params/out are never touched on this guard path.
    with pytest.raises(ScanInterruptedError, match="cannot resume"):
        AdbcScan._reader_for(None, state)  # type: ignore[arg-type]

    class _Out:
        def finish(self) -> None: ...
        def emit(self, _b: object) -> None: ...

    with pytest.raises(ScanInterruptedError, match="cannot resume"):
        AdbcQuery.process(None, state, _Out())  # type: ignore[arg-type]


def test_max_scans_evicts_lru(monkeypatch) -> None:  # noqa: ANN001
    _reset()
    import vgi_adbc.engine as engine_mod

    monkeypatch.setattr(engine_mod, "_engine", engine_mod.AdbcEngine(max_scans=2))
    r0, r1, r2 = FakeReader(), FakeReader(), FakeReader()
    fn._register_reader(b"r0", r0, b"s")
    fn._register_reader(b"r1", r1, b"s")
    fn._register_reader(b"r2", r2, b"s")  # over cap -> evict the LRU (r0)
    assert fn.active_scan_count() == 2
    assert r0.closed is True
    assert not r1.closed and not r2.closed
    _reset()
