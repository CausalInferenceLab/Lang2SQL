"""Structural baseline tests that avoid public-network and raw-value access."""

from __future__ import annotations

import asyncio
from pathlib import Path
import sqlite3
import sys

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "bench"))
import cross_domain_baseline as baseline  # noqa: E402


def test_baseline_reports_cross_cutting_onboarding_risks(tmp_path):
    database = tmp_path / "events.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE observations ("
            "year INTEGER, fips_code INTEGER, latitude REAL, "
            "observed_at TEXT, amount REAL, user_email TEXT)"
        )
        connection.execute(
            "INSERT INTO observations VALUES (2025, 101, 37.5, "
            "'2025-01-01T00:00:00', 10.5, 'hidden@example.test')"
        )

    result = asyncio.run(
        baseline.evaluate_database(
            {
                "dataset_id": "fixture",
                "db_id": "fixture",
                "domain": "test",
                "topology_family": "flat_event",
                "dialect": "sqlite",
                "split": "dev",
            },
            database,
        )
    )

    assert result["source_count_missing"] == []
    assert result["metric_role_risks"] == []
    assert result["string_time_not_typed"] == [
        {
            "column": "observations.observed_at",
            "data_type": "TEXT",
            "observed_role": "categorical",
        }
    ]
    assert result["potential_pii_exposure"] == []


def test_aggregate_keeps_failure_families_separate():
    summary = baseline._aggregate(
        [
            {
                "db_id": "a",
                "table_count": 1,
                "column_count": 4,
                "source_count_missing": ["a.events"],
                "metric_role_risks": [
                    {"column": "a.events.year", "role_evidence": "calendar"}
                ],
                "string_time_not_typed": [],
                "potential_pii_exposure": [],
                "composite_foreign_keys_blocked": [],
                "catalog_json_chars": 100,
                "elapsed_ms": 1.5,
            },
            {
                "db_id": "b",
                "table_count": 2,
                "column_count": 5,
                "source_count_missing": [],
                "metric_role_risks": [],
                "string_time_not_typed": [
                    {"column": "b.events.at", "observed_role": "blocked"}
                ],
                "potential_pii_exposure": ["b.users.name"],
                "composite_foreign_keys_blocked": [],
                "catalog_json_chars": 80,
                "elapsed_ms": 2.5,
            },
        ]
    )
    assert summary["database_count"] == 2
    assert summary["source_count_missing_tables"] == 1
    assert summary["numeric_metric_role_risks"] == 1
    assert summary["string_time_not_typed"] == 1
    assert summary["potential_pii_exposure"] == 1
    assert summary["max_catalog_db_id"] == "a"
