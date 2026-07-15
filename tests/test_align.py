"""Unit tests for the scan-batch alignment helper (pyarrow only, no network)."""

from __future__ import annotations

import pyarrow as pa

from vgi_adbc.functions import _align


def test_align_noop_when_schema_matches() -> None:
    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    batch = pa.RecordBatch.from_pydict({"id": [1, 2], "name": ["a", "b"]}, schema=schema)
    aligned = _align(batch, schema)
    assert aligned is batch


def test_align_reorders_columns() -> None:
    out_schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    batch = pa.RecordBatch.from_pydict(
        {"name": ["a", "b"], "id": [1, 2]},
        schema=pa.schema([("name", pa.string()), ("id", pa.int64())]),
    )
    aligned = _align(batch, out_schema)
    assert aligned.schema.equals(out_schema)
    assert aligned.to_pydict() == {"id": [1, 2], "name": ["a", "b"]}


def test_align_casts_types() -> None:
    out_schema = pa.schema([("id", pa.int64())])
    batch = pa.RecordBatch.from_pydict({"id": [1, 2]}, schema=pa.schema([("id", pa.int32())]))
    aligned = _align(batch, out_schema)
    assert aligned.schema.field("id").type == pa.int64()
    assert aligned.to_pydict() == {"id": [1, 2]}


def test_align_raises_on_missing_column() -> None:
    out_schema = pa.schema([("id", pa.int64()), ("missing", pa.string())])
    batch = pa.RecordBatch.from_pydict({"id": [1]}, schema=pa.schema([("id", pa.int64())]))
    try:
        _align(batch, out_schema)
    except ValueError as exc:
        assert "missing" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected ValueError")
