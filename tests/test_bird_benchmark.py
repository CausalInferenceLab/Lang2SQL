import sqlite3

import pytest

from bench.bird.comparison import paired_metrics
from bench.bird.dataset import COHORT_PATH, manifest_metadata, selected_metadata
from bench.bird.io import ProtocolError, read_json
from bench.bird.scoring import BUDGETS, QueryFailure, calculate_ex, read_rows


def test_frozen_cohort_and_smoke_cases():
    cohort = read_json(COHORT_PATH)
    cases = cohort["cases"]
    question_ids = sorted({case["question_id"] for case in cases})
    assert len(question_ids) == 100
    assert [case["question_id"] for case in cases] == question_ids
    assert len({case["db_id"] for case in cases}) == 11

    smoke = selected_metadata(cohort, 2)
    assert len({case["db_id"] for case in smoke}) == 2
    assert all(case in cases for case in smoke)
    assert manifest_metadata(cohort, 2)["cases"] == smoke
    with pytest.raises(ProtocolError, match="Smoke cases"):
        selected_metadata(cohort, 100)


def test_ex_compares_sqlite_rows_and_denies_writes(tmp_path):
    db = tmp_path / "fixture.sqlite"
    with sqlite3.connect(db) as connection:
        connection.execute("CREATE TABLE result (first INT, second INT)")
        connection.executemany("INSERT INTO result VALUES (?, ?)", [(1, 2), (3, 4)])
    gold = read_rows(db, "SELECT first, second FROM result", BUDGETS)
    duplicate = read_rows(
        db,
        "SELECT * FROM result UNION ALL SELECT * FROM result ORDER BY first DESC",
        BUDGETS,
    )
    swapped = read_rows(db, "SELECT second, first FROM result", BUDGETS)
    assert calculate_ex(duplicate, gold) == 1
    assert calculate_ex(swapped, gold) == 0
    with pytest.raises(QueryFailure, match="sql_error"):
        read_rows(db, "DELETE FROM result RETURNING *", BUDGETS)


def test_paired_metrics_count_changes_and_reject_misaligned_ids():
    before = [{"question_id": 1, "ex": 0}, {"question_id": 2, "ex": 1}]
    after = [{"question_id": 1, "ex": 1}, {"question_id": 2, "ex": 0}]
    report = paired_metrics(before, after)
    assert report["delta_ex_pp"] == 0
    assert report["wrong_to_right"] == report["right_to_wrong"] == 1
    with pytest.raises(ProtocolError, match="paired by question ID"):
        paired_metrics(before, after[::-1])
