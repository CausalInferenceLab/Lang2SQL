"""Local-model scoring is deterministic and keeps gold out of prompts."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
import sqlite3
import sys

from lang2sql.core.types import Completion, ToolCall, ToolSpec
from lang2sql.adapters.llm.openai_ import _decode_completion


_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "bench"))
from eval_contract import EvalCase  # noqa: E402
import local_model_semantic_eval as evaluator  # noqa: E402


def _case(**overrides) -> EvalCase:
    raw = {
            "case_id": "fixture.count",
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
            "oracle_sql": "SELECT kind, COUNT(*) FROM events GROUP BY kind",
        }
    raw.update(overrides)
    return EvalCase.from_mapping(raw)


def test_exact_typed_selection_scores_without_assistant_prose():
    completion = Completion(
        tool_calls=[
            ToolCall(
                id="call-1",
                name="semantic_query",
                arguments={
                    "metric_id": "metric:events.source_record_count",
                    "metric_phrase": "rows",
                    "aggregate": "count",
                    "dimensions": [
                        {"dimension_id": "dimension:events.kind", "phrase": "kind"}
                    ],
                    "unresolved_obligations": [],
                },
            )
        ]
    )

    score = evaluator.score_completion(_case(), completion)

    assert score["slot_exact"] is True
    assert score["usable_selection"] is True
    assert score["assistant_content_present"] is False


def test_missing_grouping_or_duplicate_obligation_is_not_usable():
    completion = Completion(
        tool_calls=[
            ToolCall(
                id="call-1",
                name="semantic_query",
                arguments={
                    "metric_id": "metric:events.source_record_count",
                    "metric_phrase": "rows",
                    "aggregate": "count",
                    "dimensions": [],
                    "unresolved_obligations": ["group by kind"],
                },
            )
        ]
    )

    score = evaluator.score_completion(_case(), completion)

    assert score["dimensions_match"] is False
    assert score["obligations_empty"] is False
    assert score["usable_selection"] is False


def test_invalid_dimension_sibling_cannot_score_as_exact_or_usable():
    completion = Completion(
        tool_calls=[
            ToolCall(
                id="call-1",
                name="semantic_query",
                arguments={
                    "metric_id": "metric:events.source_record_count",
                    "metric_phrase": "rows",
                    "aggregate": "count",
                    "dimensions": [
                        {"dimension_id": "dimension:events.kind", "phrase": "kind"},
                        "invalid-sibling",
                    ],
                    "unresolved_obligations": [],
                },
            )
        ]
    )

    score = evaluator.score_completion(_case(), completion)

    assert score["dimensions_shape_valid"] is False
    assert score["slot_exact"] is False
    assert score["usable_selection"] is False


def test_non_object_tool_arguments_are_a_case_level_invalid_selection():
    completion = _decode_completion(
        {
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {
                                "id": "call-1",
                                "function": {
                                    "name": "semantic_query",
                                    "arguments": "[]",
                                },
                            }
                        ]
                    }
                }
            ]
        }
    )

    score = evaluator.score_completion(_case(), completion)

    assert completion.tool_calls[0].arguments == {
        "__invalid_argument_shape__": "list"
    }
    assert score["status"] == "invalid_argument_shape"
    assert score["slot_exact"] is False
    assert score["usable_selection"] is False


def test_production_blocker_is_not_counted_as_usable_selection(
    tmp_path, monkeypatch
):
    database = tmp_path / "blocked-draft.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE events (kind TEXT)")

    async def exact_completion(*_args, **_kwargs):
        return Completion(
            tool_calls=[
                ToolCall(
                    id="call-1",
                    name="semantic_query",
                    arguments={
                        "metric_id": "metric:events.source_record_count",
                        "metric_phrase": "rows",
                        "aggregate": "count",
                        "dimensions": [
                            {
                                "dimension_id": "dimension:events.kind",
                                "phrase": "kind",
                            }
                        ],
                        "unresolved_obligations": [],
                    },
                )
            ]
        )

    monkeypatch.setattr(evaluator, "_complete", exact_completion)
    result = asyncio.run(
        evaluator.evaluate_case(
            _case(question="How many rows by kind in Boston?"),
            database,
            model="fake",
            base_url="http://127.0.0.1:1/v1/chat/completions",
            timeout=1,
            release_mode="all",
        )
    )

    assert result["slot_exact"] is True
    assert result["production_draft_status"] == "clarification"
    assert result["production_draft_blocker"] == "unresolved_question_terms"
    assert result["production_review_pending"] is False
    assert result["usable_selection"] is False


def test_exact_persisted_review_pending_is_usable(tmp_path, monkeypatch):
    database = tmp_path / "reviewable-draft.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE events (kind TEXT, amount REAL)")

    case = _case(
        question="What is total amount by kind?",
        gold_operators=["sum", "group_by"],
        expected_state="review_then_ready",
        gold_semantic_plan={
            "metric": {
                "table_id": "events",
                "column": "amount",
                "aggregate": "sum",
            },
            "dimensions": [{"table_id": "events", "column": "kind"}],
        },
        oracle_sql="SELECT kind, SUM(amount) FROM events GROUP BY kind",
    )

    async def exact_completion(*_args, **_kwargs):
        return Completion(
            tool_calls=[
                ToolCall(
                    id="call-1",
                    name="semantic_query",
                    arguments={
                        "metric_id": "metric:events.amount",
                        "metric_phrase": "amount",
                        "aggregate": "sum",
                        "dimensions": [
                            {
                                "dimension_id": "dimension:events.kind",
                                "phrase": "kind",
                            }
                        ],
                        "unresolved_obligations": [],
                    },
                )
            ]
        )

    monkeypatch.setattr(evaluator, "_complete", exact_completion)
    result = asyncio.run(
        evaluator.evaluate_case(
            case,
            database,
            model="fake",
            base_url="http://127.0.0.1:1/v1/chat/completions",
            timeout=1,
            release_mode="all",
        )
    )

    assert result["slot_exact"] is True
    assert result["production_draft_status"] == "clarification"
    assert result["production_draft_blocker"] == ""
    assert result["production_review_pending"] is True
    assert result["usable_selection"] is True


def test_exact_hallucinated_id_outside_attention_is_not_usable(
    tmp_path, monkeypatch
):
    database = tmp_path / "outside-attention.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE events (kind TEXT)")

    original_attention = evaluator.build_attention_envelope

    def without_gold_metric(catalog, question):
        attention = original_attention(catalog, question)
        return replace(
            attention,
            metric_ids=tuple(
                item
                for item in attention.metric_ids
                if item != "metric:events.source_record_count"
            ),
        )

    async def exact_completion(*_args, **_kwargs):
        return Completion(
            tool_calls=[
                ToolCall(
                    id="call-1",
                    name="semantic_query",
                    arguments={
                        "metric_id": "metric:events.source_record_count",
                        "metric_phrase": "rows",
                        "aggregate": "count",
                        "dimensions": [
                            {
                                "dimension_id": "dimension:events.kind",
                                "phrase": "kind",
                            }
                        ],
                        "unresolved_obligations": [],
                    },
                )
            ]
        )

    monkeypatch.setattr(evaluator, "build_attention_envelope", without_gold_metric)
    monkeypatch.setattr(evaluator, "_complete", exact_completion)
    result = asyncio.run(
        evaluator.evaluate_case(
            _case(),
            database,
            model="fake",
            base_url="http://127.0.0.1:1/v1/chat/completions",
            timeout=1,
            release_mode="all",
        )
    )

    assert result["gold_slots_available"] is True
    assert result["gold_shortlisted"] is False
    assert result["slot_exact"] is True
    assert result["candidate_membership_valid"] is False
    assert result["production_draft_blocker"] == "candidate_not_shortlisted"
    assert result["usable_selection"] is False


def test_model_messages_contain_question_but_not_gold_sql_or_plan():
    captured = {}

    class RecordingLLM:
        async def complete(self, messages, tools=()):
            captured["messages"] = messages
            captured["tools"] = tools
            return Completion()

    tool = ToolSpec(name="semantic_query", description="typed", parameters={})
    asyncio.run(
        evaluator._complete(
            RecordingLLM(),
            _case().question,
            ["events"],
            tool,
        )
    )
    text = "\n".join(message.content or "" for message in captured["messages"])

    assert _case().question in text
    assert _case().oracle_sql not in text
    assert "metric:events.source_record_count" not in text
