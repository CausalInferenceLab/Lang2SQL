"""The public semantic case set is frozen before production behavior changes."""

from __future__ import annotations

from pathlib import Path
import sys

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "bench"))
import dataset_cache  # noqa: E402
import eval_contract  # noqa: E402

_CASES = _ROOT / "bench" / "cases" / "public_semantic_cases.jsonl"


def test_cases_cover_every_database_with_db_level_holdout():
    manifest = dataset_cache.load_manifest()
    cases = eval_contract.load_cases(_CASES)
    expected = {
        item["db_id"]: item["split"]
        for item in dataset_cache.declared_databases(manifest)
    }

    # The corpus may add multiple questions per database; its portability gate
    # is database coverage and split isolation, not one hand-picked case each.
    assert len(expected) >= 20
    assert len(cases) >= len(expected) + 5
    assert {case.db_id for case in cases} == set(expected)
    assert all(case.split == expected[case.db_id] for case in cases)
    assert sum(case.split == "dev" for case in cases) >= 10
    assert sum(case.split == "holdout" for case in cases) >= 10


def test_supported_and_expected_blocked_cases_are_not_mixed():
    cases = eval_contract.load_cases(_CASES)
    supported = [case for case in cases if case.expected_state != "blocked"]
    blocked = [case for case in cases if case.expected_state == "blocked"]

    assert len(supported) >= 15
    assert len(blocked) >= 10
    assert len({case.domain for case in supported}) >= 12
    assert len({case.topology_family for case in cases}) >= 6
    assert (
        sum(
            bool({"join", "bridge_join"}.intersection(case.gold_operators))
            for case in supported
        )
        >= 6
    )
    assert all(
        "unresolved_obligations" not in case.gold_semantic_plan for case in supported
    )
    assert all(
        case.gold_semantic_plan.get("unresolved_obligations") for case in blocked
    )
    assert all(case.oracle_sql for case in cases)


def test_source_identity_is_stable_inside_each_dataset():
    cases = eval_contract.load_cases(_CASES)
    identities: dict[str, set[tuple[str, str, str]]] = {}
    for case in cases:
        identities.setdefault(case.dataset_id, set()).add(
            (case.dataset_version, case.source_sha256, case.license)
        )
    assert all(len(identity) == 1 for identity in identities.values())


def test_runtime_does_not_import_eval_contract_or_case_file():
    forbidden = ("eval_contract", "public_semantic_cases", "cross_domain_baseline")
    for path in (_ROOT / "src" / "lang2sql").rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert all(token not in text for token in forbidden)
