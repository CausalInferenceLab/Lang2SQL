#!/usr/bin/env python3
"""Execute frozen semantic plans against public SQLite databases.

This is an evaluator, not a production planner. Gold SQL is used only as an
offline result oracle and is never passed to the model, semantic service, or
runtime tool path. Result rows are compared in memory and never persisted.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer
from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.core.ports.safety import SafetyContext, Verdict
from lang2sql.safety.pipeline import SafetyPipeline
from lang2sql.semantic.catalog import Aggregate, SemanticCatalog
from lang2sql.semantic.onboarding import build_catalog
from lang2sql.semantic.service import (
    SemanticService,
    StewardAssertion,
    _compile_sql,
    _unique_safe_path,
    decode_semantic_query_rows,
    enforce_metric_disclosure_output,
    enforce_released_dimension_output,
)

import cross_domain_baseline
import dataset_cache
from eval_contract import EvalCase, load_cases


DEFAULT_CASES = Path(__file__).parent / "cases" / "public_semantic_cases.jsonl"
DEFAULT_OUTPUT = Path("lang2sql-datasets/reports/public_oracle_plan_execution.json")
_CONTROLLED_EXTREME_POLICY_ERROR = "controlled metrics cannot compile MIN/MAX"


def _metric_id(plan: Mapping[str, Any]) -> str:
    table_id = str(plan["table_id"])
    if plan.get("source_record_count") is True:
        return f"metric:{table_id}.source_record_count"
    return f"metric:{table_id}.{plan['column']}"


def _dimension_ids(plan: Mapping[str, Any]) -> list[str]:
    return [
        f"dimension:{item['table_id']}.{item['column']}"
        for item in plan.get("dimensions", [])
    ]


def _canonical_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value.normalize())
    if isinstance(value, float):
        return round(value, 12)
    if isinstance(value, bytes):
        return {"bytes_hex": value.hex()}
    return value


def _row_multiset(rows: Sequence[Mapping[str, Any]]) -> Counter[str]:
    encoded = []
    for row in rows:
        # Preserve column identity. Sorting bare values made rows with swapped
        # dimension and metric fields compare equal.
        values = sorted(
            (str(key), _canonical_value(value)) for key, value in row.items()
        )
        encoded.append(
            json.dumps(
                values, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            )
        )
    return Counter(encoded)


def _join_coverage_policy_difference(
    compiled_rows: Sequence[Mapping[str, Any]],
    oracle_rows: Sequence[Mapping[str, Any]],
    dimension_keys: Sequence[str],
    *,
    has_join: bool,
) -> bool:
    """Identify the bounded LEFT-vs-INNER coverage difference in frozen gold.

    Production deliberately preserves facts whose nullable/orphan FK has no
    parent. This is not an exact match to an older INNER JOIN oracle, but it is
    only classified separately when every non-NULL group still matches and all
    additional production rows are NULL-dimension groups.
    """

    if not has_join or not dimension_keys:
        return False
    non_null_rows = [
        row
        for row in compiled_rows
        if all(row.get(key) is not None for key in dimension_keys)
    ]
    null_coverage_rows = [
        row
        for row in compiled_rows
        if any(row.get(key) is None for key in dimension_keys)
    ]
    return bool(null_coverage_rows) and _row_multiset(non_null_rows) == _row_multiset(
        oracle_rows
    )


def _gate(sql: str) -> str:
    decision = SafetyPipeline().evaluate(sql, SafetyContext(row_limit=1000))
    if decision.verdict is not Verdict.PASS:
        raise ValueError(f"benchmark SQL failed the read-only gate: {decision.reason}")
    return decision.sql


async def evaluate_case(
    case: EvalCase, path: Path, *, disclosure_mode: str = "controlled"
) -> dict[str, Any]:
    """Compile one frozen semantic plan and compare it with its offline oracle."""

    explorer = SqlAlchemyExplorer(f"sqlite:///{path.resolve()}")
    try:
        catalog = (await build_catalog(explorer)).catalog
        if case.expected_state == "blocked":
            call = case.adversarial_semantic_call
            service = SemanticService(SqliteStore())
            service.save("benchmark", catalog)
            outcome = service.prepare_query(
                scope="benchmark",
                review_scope=f"review:{case.case_id}",
                explorer=explorer,
                question=case.question,
                metric_id=str(call["metric_id"]),
                metric_phrase=str(call["metric_phrase"]),
                aggregate=str(call["aggregate"]),
                dimension_bindings=list(call["dimensions"]),
                unresolved_obligations=[
                    str(item) for item in call["unresolved_obligations"]
                ],
                limit=100,
                requester_id="benchmark-adversary",
            )
            safe_nonexecution = bool(
                outcome.status in {"blocked", "clarification"} and not outcome.sql
            )
            target_guard = outcome.blocker == "unsupported_obligations"
            return {
                "case_id": case.case_id,
                "db_id": case.db_id,
                "split": case.split,
                "expected_state": case.expected_state,
                "status": (
                    "expected_blocked_verified"
                    if safe_nonexecution
                    else "expected_blocked_contract_mismatch"
                ),
                "runtime_status": outcome.status,
                "reason": outcome.blocker,
                "safe_nonexecution_verified": safe_nonexecution,
                "target_guard_verified": target_guard,
                # prepare_query is synchronous and receives no executor. This
                # counter documents that the production planning path returned
                # before the only execute call site in SemanticQuery.run.
                "sql_execution_count": 0,
                "sql_prepared": bool(outcome.sql),
            }
        plan = case.gold_semantic_plan
        metric_plan = plan.get("metric")
        if not isinstance(metric_plan, Mapping):
            raise ValueError(f"{case.case_id} has no metric plan")
        metric_id = _metric_id(metric_plan)
        dimension_ids = _dimension_ids(plan)
        aggregate = Aggregate(str(metric_plan["aggregate"]))

        missing = []
        if catalog.metric(metric_id) is None:
            missing.append(metric_id)
        missing.extend(
            dimension_id
            for dimension_id in dimension_ids
            if catalog.dimension(dimension_id) is None
        )
        if missing:
            return {
                "case_id": case.case_id,
                "db_id": case.db_id,
                "split": case.split,
                "expected_state": case.expected_state,
                "status": "semantic_catalog_gap",
                "reason": "planned_semantic_object_missing",
                "missing_semantic_objects": sorted(missing),
            }

        metric = catalog.metric(metric_id)
        assert metric is not None
        if aggregate not in metric.allowed_aggregates:
            return {
                "case_id": case.case_id,
                "db_id": case.db_id,
                "split": case.split,
                "expected_state": case.expected_state,
                "status": "semantic_catalog_gap",
                "reason": "planned_aggregate_not_allowed",
            }

        paths = []
        for dimension_id in dimension_ids:
            dimension = catalog.dimension(dimension_id)
            assert dimension is not None
            path_to_dimension, error = _unique_safe_path(
                catalog, metric.table_id, dimension.table_id
            )
            if error:
                return {
                    "case_id": case.case_id,
                    "db_id": case.db_id,
                    "split": case.split,
                    "expected_state": case.expected_state,
                    "status": "semantic_catalog_gap",
                    "reason": error,
                }
            paths.append(path_to_dimension)

        release_required = [
            dimension_id
            for dimension_id in dimension_ids
            if not catalog.dimension(dimension_id).raw_output_allowed
        ]
        service = SemanticService(SqliteStore())
        service.save("benchmark", catalog)
        pre_release_status = "not_required"
        policy_assisted_public_scope = False
        if disclosure_mode not in {"controlled", "public"}:
            raise ValueError(f"unsupported disclosure mode: {disclosure_mode}")
        steward_assertion = StewardAssertion(
            scope="benchmark",
            reviewer_id="benchmark-steward",
            authorized=True,
            public_data_confirmed=disclosure_mode == "public",
        )
        # Dataset-wide metric policy is independent of whether this particular
        # plan happens to select a release-required dimension.
        if disclosure_mode == "public":
            public_scope = service.confirm_public_data_scope(
                "benchmark", steward_assertion
            )
            if public_scope.status != "confirmed":
                raise ValueError(public_scope.message)
            policy_assisted_public_scope = True
            catalog = service.load("benchmark")
            assert catalog is not None
        if release_required:
            pre_release_status = "blocked_without_execution"
            try:
                _compile_sql(
                    catalog=catalog,
                    explorer=explorer,
                    metric_id=metric_id,
                    aggregate=aggregate,
                    dimension_ids=dimension_ids,
                    paths=paths,
                    limit=1000,
                )
            except ValueError as exc:
                if str(exc) != "released dimensions required":
                    raise
            else:
                raise AssertionError("unreleased benchmark dimension compiled")
            for dimension_id in release_required:
                released = service.release_dimension(
                    "benchmark",
                    dimension_id,
                    steward_assertion,
                    disclosure_tier=f"{disclosure_mode}_grouped",
                )
                if released.status != "confirmed":
                    raise ValueError(
                        f"benchmark release failed for {dimension_id}: {released.message}"
                    )
            catalog = service.load("benchmark")
            assert catalog is not None

        try:
            compiled_sql = _compile_sql(
                catalog=catalog,
                explorer=explorer,
                metric_id=metric_id,
                aggregate=aggregate,
                dimension_ids=dimension_ids,
                paths=paths,
                limit=1000,
            )
        except ValueError as exc:
            # Only this exact, intentional production policy signal becomes a
            # per-case result. Any other compiler failure still aborts the run.
            if str(exc) != _CONTROLLED_EXTREME_POLICY_ERROR:
                raise
            return {
                "case_id": case.case_id,
                "db_id": case.db_id,
                "split": case.split,
                "expected_state": case.expected_state,
                "status": "compile_policy_blocked",
                "reason": "controlled_group_extreme_metric_blocked",
                "release_required_dimension_ids": release_required,
                "pre_release_status": pre_release_status,
                "pre_release_execution_count": 0,
                "policy_assisted_public_scope": policy_assisted_public_scope,
                "sql_execution_count": 0,
                "sql_prepared": False,
                "raw_values_persisted": False,
            }
        compiled_rows = await explorer.execute(_gate(compiled_sql), limit=1000)
        compiled_rows, output_blocker = enforce_metric_disclosure_output(
            catalog,
            metric_id,
            aggregate.value,
            dimension_ids,
            compiled_rows,
        )
        if output_blocker:
            return {
                "case_id": case.case_id,
                "db_id": case.db_id,
                "split": case.split,
                "expected_state": case.expected_state,
                "status": "output_policy_blocked",
                "reason": output_blocker,
                "release_required_dimension_ids": release_required,
                "pre_release_status": pre_release_status,
                "pre_release_execution_count": 0,
                "policy_assisted_public_scope": policy_assisted_public_scope,
                "raw_values_persisted": False,
            }
        compiled_rows, output_blocker = enforce_released_dimension_output(
            catalog, dimension_ids, compiled_rows
        )
        if output_blocker:
            return {
                "case_id": case.case_id,
                "db_id": case.db_id,
                "split": case.split,
                "expected_state": case.expected_state,
                "status": "output_policy_blocked",
                "reason": output_blocker,
                "release_required_dimension_ids": release_required,
                "pre_release_status": pre_release_status,
                "pre_release_execution_count": 0,
                "policy_assisted_public_scope": policy_assisted_public_scope,
                "raw_values_persisted": False,
            }
        compiled_rows, layout_blocker = decode_semantic_query_rows(
            catalog, dimension_ids, compiled_rows
        )
        if layout_blocker:
            return {
                "case_id": case.case_id,
                "db_id": case.db_id,
                "split": case.split,
                "expected_state": case.expected_state,
                "status": "output_layout_blocked",
                "reason": layout_blocker,
            }
        oracle_rows = await explorer.execute(_gate(case.oracle_sql), limit=1000)
        try:
            oracle_rows = _decode_oracle_rows(catalog, dimension_ids, oracle_rows)
        except ValueError as exc:
            return {
                "case_id": case.case_id,
                "db_id": case.db_id,
                "split": case.split,
                "expected_state": case.expected_state,
                "status": "oracle_layout_error",
                "reason": str(exc),
                "release_required_dimension_ids": release_required,
                "pre_release_status": pre_release_status,
                "pre_release_execution_count": 0,
                "policy_assisted_public_scope": policy_assisted_public_scope,
                "raw_values_persisted": False,
            }
        exact_match = _row_multiset(compiled_rows) == _row_multiset(oracle_rows)
        dimension_keys = [
            f"{dimension.table_id}.{dimension.column}"
            for dimension_id in dimension_ids
            if (dimension := catalog.dimension(dimension_id)) is not None
        ]
        join_coverage_difference = not exact_match and _join_coverage_policy_difference(
            compiled_rows,
            oracle_rows,
            dimension_keys,
            has_join=any(paths),
        )
        result = {
            "case_id": case.case_id,
            "db_id": case.db_id,
            "split": case.split,
            "expected_state": case.expected_state,
            "status": (
                "exact_match"
                if exact_match
                else "oracle_join_coverage_policy_difference"
                if join_coverage_difference
                else "result_mismatch"
            ),
            "compiled_row_count": len(compiled_rows),
            "oracle_row_count": len(oracle_rows),
            "release_required_dimension_ids": release_required,
            "pre_release_status": pre_release_status,
            "pre_release_execution_count": 0,
            "policy_assisted_public_scope": policy_assisted_public_scope,
            "raw_values_persisted": False,
        }
        if join_coverage_difference:
            result["reason"] = "production_left_join_preserves_null_or_orphan_fact"
        return result
    finally:
        if explorer._engine is not None:
            # SqlAlchemyExplorer has no public close method yet; benchmark code
            # disposes the lazily created engine explicitly to bound resources.
            explorer._engine.dispose()


def _decode_oracle_rows(
    catalog: SemanticCatalog,
    dimension_ids: list[str],
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Map benchmark oracle columns onto the production display contract."""

    if len(set(dimension_ids)) != len(dimension_ids):
        raise ValueError("oracle plan contains duplicate dimension IDs")
    dimensions = []
    display_keys: list[str] = []
    for dimension_id in dimension_ids:
        dimension = catalog.dimension(dimension_id)
        if dimension is None:
            raise ValueError(f"oracle dimension is absent from catalog: {dimension_id}")
        dimensions.append(dimension)
        display_keys.append(f"{dimension.table_id}.{dimension.column}")
    if len(set(display_keys)) != len(display_keys):
        raise ValueError("oracle plan contains duplicate dimension display keys")
    column_counts = Counter(item.column for item in dimensions)

    decoded: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise ValueError("oracle result row is not a mapping")
        if "metric_value" not in row:
            raise ValueError("oracle result is missing metric_value")
        visible: dict[str, Any] = {}
        for index, dimension in enumerate(dimensions):
            slot = f"__oracle_dimension_{index}"
            if slot in row:
                value = row[slot]
            elif column_counts[dimension.column] == 1 and dimension.column in row:
                # Legacy single-name fixtures stay valid. Duplicate physical
                # names must use explicit positional aliases so one value can
                # never be silently reused for two semantic dimensions.
                value = row[dimension.column]
            else:
                raise ValueError(
                    f"oracle result is missing positional dimension slot: {slot}"
                )
            visible[f"{dimension.table_id}.{dimension.column}"] = value
        visible["metric_value"] = row["metric_value"]
        decoded.append(visible)
    return decoded


def _summary(results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    statuses = Counter(str(result["status"]) for result in results)
    supported = [result for result in results if result["expected_state"] != "blocked"]
    split_summary = {
        split: {
            "supported_cases": sum(result["split"] == split for result in supported),
            "exact_matches": sum(
                result["split"] == split and result["status"] == "exact_match"
                for result in supported
            ),
        }
        for split in ("dev", "holdout")
    }
    return {
        "case_count": len(results),
        "supported_case_count": len(supported),
        "exact_match_count": statuses["exact_match"],
        "semantic_catalog_gap_count": statuses["semantic_catalog_gap"],
        "output_policy_blocked_count": statuses["output_policy_blocked"],
        "compile_policy_blocked_count": statuses["compile_policy_blocked"],
        "result_mismatch_count": (
            statuses["result_mismatch"] + statuses["oracle_layout_error"]
        ),
        "oracle_join_coverage_policy_difference_count": statuses[
            "oracle_join_coverage_policy_difference"
        ],
        "oracle_layout_error_count": statuses["oracle_layout_error"],
        "expected_blocked_verified": statuses["expected_blocked_verified"],
        "expected_blocked_contract_mismatch": statuses[
            "expected_blocked_contract_mismatch"
        ],
        "status_counts": dict(sorted(statuses.items())),
        "by_split": split_summary,
    }


async def run_suite(
    manifest: Mapping[str, Any],
    cases: Sequence[EvalCase],
    cache_root: Path,
    concurrency: int = 4,
    disclosure_mode: str = "controlled",
) -> dict[str, Any]:
    paths = cross_domain_baseline._materialized_paths(manifest, cache_root)
    hashes = cross_domain_baseline._lock_hashes(cache_root)
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def bounded(case: EvalCase) -> dict[str, Any]:
        if hashes.get(case.dataset_id) != case.source_sha256:
            raise ValueError(f"source checksum drift for {case.case_id}")
        async with semaphore:
            return await evaluate_case(
                case, paths[case.db_id], disclosure_mode=disclosure_mode
            )

    results = await asyncio.gather(*(bounded(case) for case in cases))
    return {
        "report_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "evaluation_boundary": {
            "dialects": ["sqlite"],
            "oracle_planner": True,
            "production_natural_language_planner": False,
            "gold_sql_fed_to_runtime_or_model": False,
            "raw_values_persisted": False,
            "disclosure_mode": disclosure_mode,
            "policy_assisted_public_scope": disclosure_mode == "public",
        },
        "summary": _summary(results),
        "cases": results,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=dataset_cache.DEFAULT_MANIFEST)
    parser.add_argument("--cases", type=Path, default=DEFAULT_CASES)
    parser.add_argument(
        "--cache-root", type=Path, default=dataset_cache.DEFAULT_CACHE_ROOT
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument(
        "--public-data-scope",
        action="store_true",
        help=(
            "Policy-assisted public-data run. Default uses controlled_grouped "
            "and retains the minimum-group guard."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    manifest = dataset_cache.load_manifest(args.manifest)
    cases = load_cases(args.cases)
    report = asyncio.run(
        run_suite(
            manifest,
            cases,
            args.cache_root,
            concurrency=args.concurrency,
            disclosure_mode="public" if args.public_data_scope else "controlled",
        )
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(report["summary"], indent=2, sort_keys=True))
    print(f"report: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
