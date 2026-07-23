from __future__ import annotations

import asyncio

from sqlalchemy import create_engine, text

from lang2sql import (
    AggregateKind,
    CallContext,
    CandidateRequest,
    CandidateSet,
    Capability,
    Connected,
    ConnectRequest,
    ConnectionInput,
    FeedbackRequest,
    Lang2SQLRuntime,
    PlanRequest,
    QueryDraft,
    ReviewRequired,
)
from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.core.types import Completion
from lang2sql.tenancy.concierge import ContextConcierge


def _seed(path: str) -> None:
    with create_engine(f"sqlite:///{path}").begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE orders ("
                "order_id INTEGER PRIMARY KEY, amount NUMERIC NOT NULL, "
                "status TEXT NOT NULL)"
            )
        )
        connection.execute(
            text("INSERT INTO orders VALUES (1, 10, 'paid'), (2, 20, 'pending')")
        )


def _context() -> CallContext:
    return CallContext(
        scope="workspace",
        actor_id="owner",
        conversation_id="conversation",
        capabilities=frozenset(
            {Capability.CONNECT, Capability.QUERY, Capability.REVIEW_ANY}
        ),
    )


class _AliasLLM:
    def __init__(self, content: str) -> None:
        self.content = content
        self.prompts: list[str] = []

    async def complete(self, messages, tools=()):
        self.prompts.extend(message.content for message in messages)
        return Completion(content=self.content, finish_reason="stop")


def test_first_connect_metadata_llm_improves_search_but_does_not_approve(
    tmp_path,
    monkeypatch,
) -> None:
    database = tmp_path / "orders.sqlite"
    _seed(str(database))
    monkeypatch.setenv("LANG2SQL_AUTO_METADATA_ENRICH", "llm")
    llm = _AliasLLM(
        '{"suggestions":[{"object_id":"metric:orders.amount","aliases":["순매출"]}]}'
    )
    concierge = ContextConcierge(
        store=SqliteStore(str(tmp_path / "runtime.sqlite")),
        llm=llm,
    )
    runtime = Lang2SQLRuntime(concierge)
    context = _context()

    connected = asyncio.run(
        runtime.connect(
            ConnectRequest(context, ConnectionInput(f"sqlite:///{database}"))
        )
    )
    assert isinstance(connected, Connected)
    assert connected.scan.enrichment_status == "llm_ready"
    assert connected.scan.enriched_object_count == 1
    # The metadata-only pass must not receive the seeded row values.
    prompt = "\n".join(llm.prompts)
    assert "paid" not in prompt
    assert "pending" not in prompt

    question = "순매출 합계"
    candidates = asyncio.run(runtime.candidates(CandidateRequest(context, question)))
    assert isinstance(candidates, CandidateSet)
    metric = next(
        item for item in candidates.metrics if item.metric_id == "metric:orders.amount"
    )
    assert metric.grounded_phrase == "순매출"

    planned = asyncio.run(
        runtime.plan(
            PlanRequest(
                context,
                QueryDraft(
                    question=question,
                    source=candidates.source,
                    candidate_token=candidates.candidate_token,
                    metric_id=metric.metric_id,
                    metric_phrase=metric.grounded_phrase,
                    aggregate=AggregateKind.SUM,
                ),
            )
        )
    )
    assert isinstance(planned, ReviewRequired)
    assert planned.review.kind == "metric"
    applied = asyncio.run(
        runtime.feedback(
            FeedbackRequest(context, planned.review.review_id, AggregateKind.SUM.value)
        )
    )
    assert applied.applied is True
    stored = concierge.semantic.load(context.scope)
    assert stored is not None
    approved_metric = stored.metric("metric:orders.amount")
    assert approved_metric is not None
    assert "순매출" in approved_metric.aliases
    assert "순매출" not in approved_metric.suggested_aliases
    runtime.close()


def test_first_connect_reports_llm_failure_and_keeps_metadata_candidates(
    tmp_path,
    monkeypatch,
) -> None:
    database = tmp_path / "orders.sqlite"
    _seed(str(database))
    monkeypatch.setenv("LANG2SQL_AUTO_METADATA_ENRICH", "llm")
    runtime = Lang2SQLRuntime(
        ContextConcierge(
            store=SqliteStore(str(tmp_path / "runtime.sqlite")),
            llm=_AliasLLM("not-json"),
        )
    )
    context = _context()

    connected = asyncio.run(
        runtime.connect(
            ConnectRequest(context, ConnectionInput(f"sqlite:///{database}"))
        )
    )
    assert isinstance(connected, Connected)
    assert connected.scan.enrichment_status == "llm_degraded"
    assert connected.scan.enrichment_reason == "invalid_output"

    candidates = asyncio.run(
        runtime.candidates(CandidateRequest(context, "total amount"))
    )
    assert isinstance(candidates, CandidateSet)
    assert any(item.metric_id == "metric:orders.amount" for item in candidates.metrics)
    runtime.close()
