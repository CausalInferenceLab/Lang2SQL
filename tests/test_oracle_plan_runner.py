"""Oracle-plan execution tests stay isolated from the production runtime."""

from __future__ import annotations

import asyncio
from pathlib import Path
import sqlite3
import sys

import pytest


_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "bench"))
from eval_contract import EvalCase  # noqa: E402
import oracle_plan_runner as runner  # noqa: E402
from lang2sql.semantic.catalog import DimensionSpec, SemanticCatalog  # noqa: E402


def _case(**overrides) -> EvalCase:
    raw = {
        "case_id": "fixture.count_by_kind",
        "dataset_id": "fixture",
        "dataset_version": "1",
        "source_sha256": "a" * 64,
        "license": "test-only",
        "db_id": "fixture",
        "domain": "test",
        "topology_family": "flat_event",
        "dialect": "sqlite",
        "split": "dev",
        "question": "How many rows by kind?",
        "gold_operators": ["count_star", "group_by"],
        "expected_state": "ready",
        "gold_semantic_plan": {
            "metric": {
                "table_id": "events",
                "aggregate": "count",
                "source_record_count": True,
            },
            "dimensions": [{"table_id": "events", "column": "kind"}],
        },
        "safety_tags": ["test_only"],
        "oracle_sql": (
            "SELECT kind, COUNT(*) AS metric_value "
            "FROM events GROUP BY kind ORDER BY kind"
        ),
    }
    raw.update(overrides)
    return EvalCase.from_mapping(raw)


def test_oracle_plan_executes_count_star_and_persists_no_values(tmp_path):
    database = tmp_path / "fixture.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE events (kind VARCHAR(20), payload TEXT)")
        connection.executemany(
            "INSERT INTO events VALUES (?, ?)",
            [("a", None)] * 5 + [("b", "unused")] * 5,
        )

    result = asyncio.run(runner.evaluate_case(_case(), database))

    assert result == {
        "case_id": "fixture.count_by_kind",
        "db_id": "fixture",
        "split": "dev",
        "expected_state": "ready",
        "status": "exact_match",
        "compiled_row_count": 2,
        "oracle_row_count": 2,
        "release_required_dimension_ids": ["dimension:events.kind"],
        "pre_release_status": "blocked_without_execution",
        "pre_release_execution_count": 0,
        "policy_assisted_public_scope": False,
        "raw_values_persisted": False,
    }
    assert "rows" not in result and "oracle_rows" not in result


def _duplicate_name_catalog() -> SemanticCatalog:
    return SemanticCatalog(
        fingerprint="duplicate-name-oracle",
        dimensions=[
            DimensionSpec(
                id="dimension:constructors.name",
                label="constructors.name",
                table_id="constructors",
                column="name",
                data_type="TEXT",
            ),
            DimensionSpec(
                id="dimension:races.name",
                label="races.name",
                table_id="races",
                column="name",
                data_type="TEXT",
            ),
        ],
    )


def test_oracle_slots_preserve_duplicate_physical_dimension_identity():
    catalog = _duplicate_name_catalog()
    decoded = runner._decode_oracle_rows(
        catalog,
        ["dimension:constructors.name", "dimension:races.name"],
        [
            {
                "__oracle_dimension_0": "Ferrari",
                "__oracle_dimension_1": "Monaco Grand Prix",
                "name": "must-not-win",
                "metric_value": 25,
            }
        ],
    )
    assert decoded == [
        {
            "constructors.name": "Ferrari",
            "races.name": "Monaco Grand Prix",
            "metric_value": 25,
        }
    ]


def test_swapped_oracle_slots_are_a_result_mismatch():
    catalog = _duplicate_name_catalog()
    dimension_ids = ["dimension:constructors.name", "dimension:races.name"]
    expected = runner._decode_oracle_rows(
        catalog,
        dimension_ids,
        [
            {
                "__oracle_dimension_0": "Ferrari",
                "__oracle_dimension_1": "Monaco Grand Prix",
                "metric_value": 25,
            }
        ],
    )
    swapped = runner._decode_oracle_rows(
        catalog,
        dimension_ids,
        [
            {
                "__oracle_dimension_0": "Monaco Grand Prix",
                "__oracle_dimension_1": "Ferrari",
                "metric_value": 25,
            }
        ],
    )
    assert runner._row_multiset(expected) != runner._row_multiset(swapped)


def test_only_null_join_groups_are_classified_as_coverage_policy_difference():
    oracle = [{"parents.label": "known", "metric_value": 5}]
    compiled = [
        {"parents.label": "known", "metric_value": 5},
        {"parents.label": None, "metric_value": 1},
    ]

    assert runner._join_coverage_policy_difference(
        compiled, oracle, ["parents.label"], has_join=True
    )
    assert not runner._join_coverage_policy_difference(
        compiled, oracle, ["parents.label"], has_join=False
    )
    assert not runner._join_coverage_policy_difference(
        [
            {"parents.label": "known", "metric_value": 4},
            {"parents.label": None, "metric_value": 1},
        ],
        oracle,
        ["parents.label"],
        has_join=True,
    )


def test_duplicate_raw_dimension_name_without_all_slots_fails_loudly():
    with pytest.raises(ValueError, match="__oracle_dimension_1"):
        runner._decode_oracle_rows(
            _duplicate_name_catalog(),
            ["dimension:constructors.name", "dimension:races.name"],
            [
                {
                    "__oracle_dimension_0": "Ferrari",
                    "name": "Monaco Grand Prix",
                    "metric_value": 25,
                }
            ],
        )


def test_oracle_row_requires_metric_value():
    with pytest.raises(ValueError, match="metric_value"):
        runner._decode_oracle_rows(
            _duplicate_name_catalog(),
            ["dimension:constructors.name", "dimension:races.name"],
            [
                {
                    "__oracle_dimension_0": "Ferrari",
                    "__oracle_dimension_1": "Monaco Grand Prix",
                }
            ],
        )


def test_oracle_plan_simulates_release_but_blocks_small_groups(tmp_path):
    database = tmp_path / "release-required.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE events (opaque_label TEXT)")
        connection.executemany(
            "INSERT INTO events VALUES (?)",
            [("common",)] * 5 + [("rare",)],
        )

    result = asyncio.run(
        runner.evaluate_case(
            _case(
                gold_semantic_plan={
                    "metric": {
                        "table_id": "events",
                        "aggregate": "count",
                        "source_record_count": True,
                    },
                    "dimensions": [{"table_id": "events", "column": "opaque_label"}],
                },
                oracle_sql=(
                    "SELECT opaque_label, COUNT(*) AS metric_value "
                    "FROM events GROUP BY opaque_label"
                ),
            ),
            database,
        )
    )

    assert result["status"] == "output_policy_blocked"
    assert result["reason"] == "metric_contributor_count_too_small"
    assert result["release_required_dimension_ids"] == ["dimension:events.opaque_label"]
    assert result["pre_release_status"] == "blocked_without_execution"
    assert result["pre_release_execution_count"] == 0
    assert result["policy_assisted_public_scope"] is False
    assert "compiled_row_count" not in result


def test_controlled_extreme_metric_isolated_without_any_sql_execution(tmp_path):
    database = tmp_path / "controlled-extreme.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE events (opaque_label TEXT, amount REAL)")
        connection.executemany(
            "INSERT INTO events VALUES (?, ?)",
            [("a", 1.0)] * 5 + [("b", 2.0)] * 5,
        )
    case = _case(
        gold_semantic_plan={
            "metric": {
                "table_id": "events",
                "column": "amount",
                "aggregate": "max",
            },
            "dimensions": [{"table_id": "events", "column": "opaque_label"}],
        },
        oracle_sql="SELECT definitely_not_executed FROM missing_table",
    )

    result = asyncio.run(runner.evaluate_case(case, database))

    assert result["status"] == "compile_policy_blocked"
    assert result["reason"] == "controlled_group_extreme_metric_blocked"
    assert result["sql_execution_count"] == 0
    assert result["sql_prepared"] is False
    assert result["raw_values_persisted"] is False


def test_public_extreme_metric_still_compares_with_oracle(tmp_path):
    database = tmp_path / "public-extreme.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE events (opaque_label TEXT, amount REAL)")
        connection.executemany(
            "INSERT INTO events VALUES (?, ?)",
            [("a", 1.0), ("b", 2.0)],
        )
    case = _case(
        gold_semantic_plan={
            "metric": {
                "table_id": "events",
                "column": "amount",
                "aggregate": "max",
            },
            "dimensions": [{"table_id": "events", "column": "opaque_label"}],
        },
        oracle_sql=(
            "SELECT opaque_label, MAX(amount) AS metric_value "
            "FROM events GROUP BY opaque_label ORDER BY opaque_label"
        ),
    )

    result = asyncio.run(
        runner.evaluate_case(case, database, disclosure_mode="public")
    )
    assert result["status"] == "exact_match"


def test_unexpected_compiler_value_error_still_fails_suite(tmp_path, monkeypatch):
    database = tmp_path / "unexpected-compile.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE events (kind TEXT)")

    def fail_compile(**_kwargs):
        raise ValueError("unexpected compiler defect")

    monkeypatch.setattr(runner, "_compile_sql", fail_compile)
    with pytest.raises(ValueError, match="unexpected compiler defect"):
        asyncio.run(runner.evaluate_case(_case(), database))


def test_oracle_plan_reports_missing_semantics_without_executing_gold(tmp_path):
    database = tmp_path / "blocked.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE events (description TEXT)")

    result = asyncio.run(
        runner.evaluate_case(
            _case(
                gold_semantic_plan={
                    "metric": {
                        "table_id": "events",
                        "aggregate": "count",
                        "source_record_count": True,
                    },
                    "dimensions": [{"table_id": "events", "column": "description"}],
                },
                oracle_sql=(
                    "SELECT description, COUNT(*) AS metric_value "
                    "FROM events GROUP BY description"
                ),
            ),
            database,
        )
    )

    assert result["status"] == "semantic_catalog_gap"
    assert result["missing_semantic_objects"] == ["dimension:events.description"]
    assert "compiled_row_count" not in result


def test_expected_blocked_cases_are_not_executed(tmp_path):
    database = tmp_path / "blocked-runtime.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE events (kind BOOLEAN)")
    result = asyncio.run(
        runner.evaluate_case(
            _case(
                expected_state="blocked",
                gold_semantic_plan={"unresolved_obligations": ["row_projection"]},
                adversarial_semantic_call={
                    "metric_id": "metric:events.source_record_count",
                    "metric_phrase": "rows",
                    "aggregate": "count",
                    "dimensions": [],
                    "unresolved_obligations": ["row_projection"],
                },
            ),
            database,
        )
    )

    assert result["status"] == "expected_blocked_verified"
    assert result["reason"] == "unsupported_obligations"
    assert result["safe_nonexecution_verified"] is True
    assert result["target_guard_verified"] is True
    assert result["sql_execution_count"] == 0
