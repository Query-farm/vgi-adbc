"""Keep the published composed examples, agent tasks and their executed copies in sync.

The composed examples are asserted row-for-row by ``test/sql/adbc_composed.test`` (haybarn
E2E); these checks make sure what that suite executes is exactly what the catalog publishes,
and that every published agent task has a private grader.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from vgi_adbc.functions import AdbcQuery
from vgi_adbc.metadata import (
    CATALOG_TAGS,
    COMPOSED_SOURCE_DEFINITION_EXAMPLE,
    COMPOSED_TYPE_MAPPING_EXAMPLE,
    SCHEMA_TAGS,
)

_ROOT = Path(__file__).resolve().parent.parent
_COMPOSED = [COMPOSED_TYPE_MAPPING_EXAMPLE, COMPOSED_SOURCE_DEFINITION_EXAMPLE]


@pytest.mark.parametrize("example", _COMPOSED, ids=["type_mapping", "source_definition"])
def test_composed_example_is_published_and_executed(example: dict[str, str]) -> None:
    schema_examples = json.loads(SCHEMA_TAGS["vgi.example_queries"])
    assert example in schema_examples
    # The agent simulator only shows an object's first 5 examples.
    driving = [(e.sql, e.description) for e in AdbcQuery.Meta.examples[:5]]
    assert (example["sql"], example["description"]) in driving
    assert example["sql"] + ";" in (_ROOT / "test/sql/adbc_composed.test").read_text()


def test_source_views_task_has_an_executed_grader() -> None:
    published = {t["name"] for t in json.loads(CATALOG_TAGS["vgi.agent_test_tasks"])}
    assert "source_views" in published
    graders = (_ROOT / "vgi-agent-tests.yaml").read_text()
    # One task, one reference_sql line (plain-text parse: PyYAML is not a project dep).
    assert "- name: source_views" in graders
    ref = graders.split("reference_sql: ", 1)[1].splitlines()[0]
    sql = ref[1:-1].replace("''", "'") if ref.startswith("'") else json.loads(ref)
    assert sql + ";" in (_ROOT / "test/sql/adbc_composed.test").read_text()
