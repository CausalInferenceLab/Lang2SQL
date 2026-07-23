#!/usr/bin/env python3
"""Evaluate a local model's first governed semantic tool selection.

The model receives only the production system prompt, the catalog-derived
``semantic_query`` schema, table names, and the public question. Frozen gold
plans are consulted only after completion for scoring. No SQL is generated or
executed and no database values are read.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any, Mapping, Sequence

from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer
from lang2sql.adapters.llm.openai_ import OpenAILLM
from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.core.types import Completion, Message, Role, ToolSpec
from lang2sql.harness.system_prompt import _GOVERNED_BASE
from lang2sql.semantic.catalog import SemanticCatalog
from lang2sql.semantic.onboarding import build_catalog
from lang2sql.semantic.service import SemanticService, StewardAssertion
from lang2sql.semantic.shortlist import (
    build_attention_envelope,
    prompt_table_section,
)
from lang2sql.tools.semantic_query import SemanticQuery

import cross_domain_baseline
import dataset_cache
from eval_contract import EvalCase, load_cases

DEFAULT_CASES = Path(__file__).parent / "cases" / "public_semantic_cases.jsonl"
DEFAULT_OUTPUT = Path("lang2sql-datasets/reports/local_model_semantic_eval.json")


def _expected_slots(case: EvalCase) -> tuple[str, str, list[str]]:
    plan = case.gold_semantic_plan
    metric = plan["metric"]
    metric_id = (
        f"metric:{metric['table_id']}.source_record_count"
        if metric.get("source_record_count") is True
        else f"metric:{metric['table_id']}.{metric['column']}"
    )
    dimension_ids = sorted(
        f"dimension:{item['table_id']}.{item['column']}"
        for item in plan.get("dimensions", [])
    )
    return metric_id, str(metric["aggregate"]), dimension_ids


def _gold_available(case: EvalCase, catalog: SemanticCatalog) -> bool:
    metric_id, aggregate, dimension_ids = _expected_slots(case)
    metric = catalog.metric(metric_id)
    return bool(
        metric is not None
        and aggregate in {item.value for item in metric.allowed_aggregates}
        and all(
            (dimension := catalog.dimension(dimension_id)) is not None
            and dimension.raw_output_allowed
            for dimension_id in dimension_ids
        )
    )


def _normalize_phrase(value: object) -> str:
    import re

    return " ".join(re.sub(r"[^0-9a-zA-Z가-힣]+", " ", str(value).lower()).split())


def _grounded(phrase: object, question: str) -> bool:
    normalized_phrase = _normalize_phrase(phrase)
    normalized_question = _normalize_phrase(question)
    return bool(
        normalized_phrase and f" {normalized_phrase} " in f" {normalized_question} "
    )


def score_completion(case: EvalCase, completion: Completion) -> dict[str, Any]:
    """Score typed slots without persisting assistant prose or hidden reasoning."""

    calls = [call for call in completion.tool_calls if call.name == "semantic_query"]
    if len(calls) != 1 or len(completion.tool_calls) != 1:
        return {
            "status": (
                "no_semantic_call"
                if not calls
                else (
                    "semantic_call_with_sibling_tools"
                    if len(completion.tool_calls) != 1
                    else "multiple_semantic_calls"
                )
            ),
            "semantic_call_count": len(calls),
            "total_tool_call_count": len(completion.tool_calls),
            "assistant_content_present": bool(completion.content),
            "slot_exact": False,
            "usable_selection": False,
        }
    arguments = calls[0].arguments
    if not isinstance(arguments, Mapping) or "__invalid_argument_shape__" in arguments:
        return {
            "status": "invalid_argument_shape",
            "semantic_call_count": 1,
            "total_tool_call_count": 1,
            "assistant_content_present": bool(completion.content),
            "slot_exact": False,
            "usable_selection": False,
        }
    expected_metric, expected_aggregate, expected_dimensions = _expected_slots(case)
    raw_dimensions = arguments.get("dimensions")
    dimensions_shape_valid = bool(
        isinstance(raw_dimensions, list)
        and all(
            isinstance(item, Mapping)
            and set(item) == {"dimension_id", "phrase"}
            and isinstance(item.get("dimension_id"), str)
            and isinstance(item.get("phrase"), str)
            for item in raw_dimensions
        )
    )
    dimension_items = (
        [item for item in raw_dimensions if isinstance(item, Mapping)]
        if isinstance(raw_dimensions, list)
        else []
    )
    observed_dimensions = (
        sorted(str(item.get("dimension_id", "")) for item in dimension_items)
        if isinstance(raw_dimensions, list)
        else []
    )
    obligations = arguments.get("unresolved_obligations")
    metric_match = arguments.get("metric_id") == expected_metric
    aggregate_match = arguments.get("aggregate") == expected_aggregate
    dimensions_match = observed_dimensions == expected_dimensions
    obligations_empty = obligations == []
    metric_phrase_grounded = _grounded(
        arguments.get("metric_phrase", ""), case.question
    )
    dimension_phrases_grounded = bool(
        dimensions_shape_valid
        and len(dimension_items) == len(raw_dimensions)
        and all(
            _grounded(item.get("phrase", ""), case.question) for item in dimension_items
        )
    )
    selection_grounded = metric_phrase_grounded and dimension_phrases_grounded
    slot_exact = (
        metric_match and aggregate_match and dimensions_match and dimensions_shape_valid
    )
    return {
        "status": "semantic_call",
        "semantic_call_count": 1,
        "total_tool_call_count": 1,
        "assistant_content_present": bool(completion.content),
        "metric_match": metric_match,
        "aggregate_match": aggregate_match,
        "dimensions_match": dimensions_match,
        "dimensions_shape_valid": dimensions_shape_valid,
        "obligations_empty": obligations_empty,
        "metric_phrase_grounded": metric_phrase_grounded,
        "dimension_phrases_grounded": dimension_phrases_grounded,
        "selection_grounded": selection_grounded,
        "slot_exact": slot_exact,
        "usable_selection": slot_exact and obligations_empty and selection_grounded,
        "observed_metric_id": str(arguments.get("metric_id", "")),
        "observed_dimension_ids": observed_dimensions,
        "unresolved_obligations": (
            [str(item) for item in obligations]
            if isinstance(obligations, list)
            else ["<invalid-shape>"]
        ),
    }


async def _complete(
    llm: OpenAILLM,
    question: str,
    table_names: Sequence[str],
    tool: ToolSpec,
) -> Completion:
    system = _GOVERNED_BASE + "\n\n" + prompt_table_section(table_names)
    # The gold plan and oracle SQL are deliberately absent from this boundary.
    return await llm.complete(
        [
            Message(role=Role.SYSTEM, content=system),
            Message(role=Role.USER, content=question),
        ],
        [tool],
    )


async def evaluate_case(
    case: EvalCase,
    path: Path,
    *,
    model: str,
    base_url: str,
    timeout: float,
    release_mode: str,
) -> dict[str, Any]:
    explorer = SqlAlchemyExplorer(f"sqlite:///{path.resolve()}")
    started = time.perf_counter()
    try:
        catalog = (await build_catalog(explorer)).catalog
        released_dimension_ids: list[str] = []
        if release_mode != "none":
            service = SemanticService(SqliteStore())
            service.save("benchmark", catalog)
            public_assertion = StewardAssertion(
                scope="benchmark",
                reviewer_id="benchmark-steward",
                authorized=True,
                public_data_confirmed=True,
            )
            public_scope = service.confirm_public_data_scope(
                "benchmark", public_assertion
            )
            if public_scope.status != "confirmed":
                raise ValueError(public_scope.message)
            if release_mode == "gold":
                _metric_id, _aggregate, dimension_ids = _expected_slots(case)
            elif release_mode == "all":
                dimension_ids = [
                    item.id for item in service.release_candidates("benchmark")
                ]
            else:
                raise ValueError(f"unsupported release mode: {release_mode}")
            for dimension_id in dimension_ids:
                dimension = catalog.dimension(dimension_id)
                if dimension is None or dimension.raw_output_allowed:
                    continue
                outcome = service.release_dimension(
                    "benchmark",
                    dimension_id,
                    public_assertion,
                    disclosure_tier="public_grouped",
                )
                if outcome.status == "confirmed":
                    released_dimension_ids.append(dimension_id)
            catalog = service.load("benchmark")
            assert catalog is not None
        attention = build_attention_envelope(catalog, case.question)
        if not attention.ready:
            return {
                "case_id": case.case_id,
                "db_id": case.db_id,
                "split": case.split,
                "status": "candidate_clarification",
                "candidate_state": attention.state,
                "candidate_message": attention.message,
                "slot_exact": False,
                "usable_selection": False,
                "model_called": False,
                "latency_ms": round((time.perf_counter() - started) * 1000, 3),
            }
        semantic_tool = SemanticQuery(
            SemanticService(SqliteStore()),
            catalog,
            attention,
        )
        tool = semantic_tool.spec
        prompt = _GOVERNED_BASE + "\n\n" + prompt_table_section(attention.table_ids)
        tool_schema_bytes = len(
            json.dumps(
                {"description": tool.description, "parameters": tool.parameters},
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        )
        llm = OpenAILLM(
            model=model,
            api_key="local",
            base_url=base_url,
            timeout=timeout,
        )
        completion = await _complete(llm, case.question, attention.table_ids, tool)
        score = score_completion(case, completion)
        expected_metric_id, _expected_aggregate, expected_dimension_ids = (
            _expected_slots(case)
        )
        gold_shortlisted = bool(
            expected_metric_id in attention.metric_ids
            and all(
                dimension_id in attention.dimension_ids
                for dimension_id in expected_dimension_ids
            )
        )
        validation_status = "not_run"
        validation_blocker = ""
        validation_sql_prepared = False
        validation_review_pending = False
        semantic_calls = [
            call for call in completion.tool_calls if call.name == "semantic_query"
        ]
        candidate_membership_valid = False
        if len(completion.tool_calls) == 1 and len(semantic_calls) == 1:
            arguments = semantic_calls[0].arguments
            if not isinstance(arguments, Mapping) or (
                "__invalid_argument_shape__" in arguments
            ):
                validation_status = "blocked"
                validation_blocker = "invalid_argument_shape"
                arguments = {}
            raw_dimensions = arguments.get("dimensions", [])
            raw_obligations = arguments.get("unresolved_obligations", [])
            observed_dimension_ids = (
                [
                    str(item.get("dimension_id", ""))
                    for item in raw_dimensions
                    if isinstance(item, Mapping)
                ]
                if isinstance(raw_dimensions, list)
                else []
            )
            candidate_membership_valid = bool(
                str(arguments.get("metric_id", "")) in attention.metric_ids
                and all(
                    dimension_id in attention.dimension_ids
                    for dimension_id in observed_dimension_ids
                )
            )
            if not candidate_membership_valid:
                validation_status = "blocked"
                validation_blocker = "candidate_not_shortlisted"
            if (
                candidate_membership_valid
                and score.get("dimensions_shape_valid") is True
                and isinstance(raw_dimensions, list)
                and isinstance(raw_obligations, list)
            ):
                service = SemanticService(SqliteStore())
                service.save("benchmark", catalog)
                outcome = service.prepare_query(
                    scope="benchmark",
                    review_scope=f"review:{case.case_id}",
                    requester_id="local-model-eval",
                    explorer=explorer,
                    question=case.question,
                    metric_id=str(arguments.get("metric_id", "")),
                    metric_phrase=str(arguments.get("metric_phrase", "")),
                    aggregate=str(arguments.get("aggregate", "")),
                    dimension_bindings=[
                        {
                            "dimension_id": str(item.get("dimension_id", "")),
                            "phrase": str(item.get("phrase", "")),
                        }
                        for item in raw_dimensions
                        if isinstance(item, Mapping)
                    ],
                    unresolved_obligations=[str(item) for item in raw_obligations],
                    limit=int(arguments.get("limit", 100)),
                )
                validation_status = outcome.status
                validation_blocker = outcome.blocker
                validation_sql_prepared = bool(outcome.sql)
                pending = service.pending_review(f"review:{case.case_id}")
                normalized_dimensions = [
                    {
                        "dimension_id": str(item.get("dimension_id", "")),
                        "phrase": _normalize_phrase(item.get("phrase", "")),
                    }
                    for item in raw_dimensions
                ]
                validation_review_pending = bool(
                    outcome.status == "clarification"
                    and not outcome.blocker
                    and pending is not None
                    and pending.metric_id == str(arguments.get("metric_id", ""))
                    and pending.metric_phrase
                    == _normalize_phrase(arguments.get("metric_phrase", ""))
                    # Metric reviews intentionally persist no unrelated draft
                    # dimensions; dimension reviews retain only the one safe
                    # binding currently presented to the steward.
                    and len(pending.dimension_bindings) <= 1
                    and all(
                        binding in normalized_dimensions
                        for binding in pending.dimension_bindings
                    )
                )
        score["production_draft_status"] = validation_status
        score["production_draft_blocker"] = validation_blocker
        score["production_sql_prepared"] = validation_sql_prepared
        score["production_draft_checked"] = validation_status != "not_run"
        score["production_review_pending"] = validation_review_pending
        score["candidate_membership_valid"] = candidate_membership_valid
        score["usable_selection"] = bool(
            score.get("usable_selection")
            and candidate_membership_valid
            and (validation_status == "ready" or validation_review_pending)
        )
        return {
            "case_id": case.case_id,
            "db_id": case.db_id,
            "split": case.split,
            "gold_slots_available": _gold_available(case, catalog),
            "gold_shortlisted": gold_shortlisted,
            "candidate_release_mode": release_mode,
            "policy_assisted_public_scope": release_mode != "none",
            "gold_influenced_tool_schema": release_mode == "gold",
            "generalization_score_eligible": release_mode != "gold",
            "released_dimension_ids": released_dimension_ids,
            "prompt_bytes": len(prompt.encode("utf-8")),
            "tool_schema_bytes": tool_schema_bytes,
            "model_called": True,
            "latency_ms": round((time.perf_counter() - started) * 1000, 3),
            **score,
        }
    except Exception as exc:
        return {
            "case_id": case.case_id,
            "db_id": case.db_id,
            "split": case.split,
            "status": "model_error",
            "error_type": type(exc).__name__,
            "error_message": str(exc)[:300],
            "slot_exact": False,
            "usable_selection": False,
            "latency_ms": round((time.perf_counter() - started) * 1000, 3),
        }
    finally:
        if explorer._engine is not None:
            explorer._engine.dispose()


def _summary(results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    blockers = Counter(
        str(result.get("production_draft_blocker"))
        for result in results
        if result.get("production_draft_blocker")
    )
    return {
        "case_count": len(results),
        "gold_slots_available": sum(
            result.get("gold_slots_available") is True for result in results
        ),
        "gold_shortlisted": sum(
            result.get("gold_shortlisted") is True for result in results
        ),
        "semantic_call_count": sum(
            result.get("status") == "semantic_call" for result in results
        ),
        "slot_exact_count": sum(result.get("slot_exact") is True for result in results),
        "usable_selection_count": sum(
            result.get("usable_selection") is True for result in results
        ),
        "model_error_count": sum(
            result.get("status") == "model_error" for result in results
        ),
        "production_draft_blockers": dict(sorted(blockers.items())),
        "by_split": {
            split: {
                "cases": sum(result["split"] == split for result in results),
                "slot_exact": sum(
                    result["split"] == split and result.get("slot_exact") is True
                    for result in results
                ),
                "usable": sum(
                    result["split"] == split and result.get("usable_selection") is True
                    for result in results
                ),
            }
            for split in ("dev", "holdout")
        },
    }


async def run_suite(
    manifest: Mapping[str, Any],
    cases: Sequence[EvalCase],
    cache_root: Path,
    *,
    model: str,
    base_url: str,
    timeout: float,
    concurrency: int,
    release_mode: str = "none",
) -> dict[str, Any]:
    paths = cross_domain_baseline._materialized_paths(manifest, cache_root)
    hashes = cross_domain_baseline._lock_hashes(cache_root)
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def bounded(case: EvalCase) -> dict[str, Any]:
        if hashes.get(case.dataset_id) != case.source_sha256:
            raise ValueError(f"source checksum drift for {case.case_id}")
        async with semaphore:
            return await evaluate_case(
                case,
                paths[case.db_id],
                model=model,
                base_url=base_url,
                timeout=timeout,
                release_mode=release_mode,
            )

    results = await asyncio.gather(*(bounded(case) for case in cases))
    return {
        "report_version": 2,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model": model,
        "base_url": base_url,
        "evaluation_boundary": {
            "dialects": ["sqlite"],
            "supported_cases_only": True,
            "first_tool_selection_only": True,
            "production_prepare_query_checked": True,
            "full_discord_loop": False,
            "gold_fed_to_model": False,
            "gold_influenced_tool_schema": release_mode == "gold",
            "generalization_score_eligible": release_mode != "gold",
            "sql_generated_or_executed": False,
            "raw_database_values_read": False,
            "candidate_release_mode": release_mode,
            "usable_selection_definition": (
                "gold slot exact, phrases copied from the question, empty obligations, "
                "and production prepare_query returned ready or an exact persisted "
                "semantic-review pending state with no blocker"
            ),
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
    parser.add_argument("--model", default="gemma4:26b")
    parser.add_argument(
        "--base-url", default="http://127.0.0.1:11434/v1/chat/completions"
    )
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument("--concurrency", type=int, default=4)
    release_group = parser.add_mutually_exclusive_group()
    release_group.add_argument(
        "--release-gold-candidates",
        action="store_true",
        help=(
            "Leakage-sensitivity diagnostic only: release dimensions selected "
            "from the frozen gold plan, which changes the model's tool schema."
        ),
    )
    release_group.add_argument(
        "--release-all-candidates",
        action="store_true",
        help=(
            "Gold-independent sensitivity run: simulate a steward releasing "
            "every review-required dimension before the question is seen."
        ),
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    cases = [
        case for case in load_cases(args.cases) if case.expected_state != "blocked"
    ]
    release_mode = (
        "gold"
        if args.release_gold_candidates
        else "all" if args.release_all_candidates else "none"
    )
    report = asyncio.run(
        run_suite(
            dataset_cache.load_manifest(args.manifest),
            cases,
            args.cache_root,
            model=args.model,
            base_url=args.base_url,
            timeout=args.timeout,
            concurrency=args.concurrency,
            release_mode=release_mode,
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
