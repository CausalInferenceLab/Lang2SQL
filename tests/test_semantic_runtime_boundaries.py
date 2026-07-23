"""Runtime boundaries that must hold independently of any benchmark domain."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from pathlib import Path
import threading
import time

import pytest
from sqlalchemy import create_engine, text

from lang2sql.adapters.db.factory import canonicalize_connection
from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer
from lang2sql.adapters.llm.fake import FakeLLM
from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.core.identity import Identity
from lang2sql.core.ports.explorer import QueryTimedOutError
from lang2sql.core.types import Message, Role
from lang2sql.frontends.discord.commands import CommandHandlers
from lang2sql.harness.context import HarnessContext
from lang2sql.harness.loop import agent_loop
from lang2sql.harness.session import Session
from lang2sql.harness.tool_registry import ToolRegistry
from lang2sql.safety.pipeline import SafetyPipeline
from lang2sql.semantic.service import SemanticService
from lang2sql.semantic.shortlist import build_attention_envelope
from lang2sql.semantic.onboarding import build_catalog
from lang2sql.tenancy.concierge import ContextConcierge
from lang2sql.tools.semantic_query import SemanticQuery


def _seed_events(path: Path, row_count: int) -> SqlAlchemyExplorer:
    with create_engine(f"sqlite:///{path}").begin() as connection:
        connection.execute(text("CREATE TABLE events (amount NUMERIC NOT NULL)"))
        connection.execute(
            text("INSERT INTO events (amount) VALUES (:amount)"),
            [{"amount": index + 1} for index in range(row_count)],
        )
    return SqlAlchemyExplorer(f"sqlite:///{path}")


def _source_count_args() -> dict[str, object]:
    return {
        "metric_id": "metric:events.source_record_count",
        "metric_phrase": "events source record count",
        "aggregate": "count",
        "dimensions": [],
        "unresolved_obligations": [],
        "limit": 100,
    }


def test_physical_fingerprint_is_independent_of_adapter_enumeration_order(tmp_path):
    database = tmp_path / "order-stable.sqlite"
    with create_engine(f"sqlite:///{database}").begin() as connection:
        connection.execute(
            text("CREATE TABLE parent (id INTEGER PRIMARY KEY, label TEXT)")
        )
        connection.execute(
            text(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER "
                "REFERENCES parent(id), amount NUMERIC)"
            )
        )
    explorer = SqlAlchemyExplorer(f"sqlite:///{database}")

    class ReorderedExplorer:
        def __init__(self, wrapped):
            self.wrapped = wrapped

        async def list_tables(self):
            return list(reversed(await self.wrapped.list_tables()))

        async def describe_table(self, name):
            table = deepcopy(await self.wrapped.describe_table(name))
            table.columns.reverse()
            return table

        async def catalog_metadata(self):
            metadata = deepcopy(await self.wrapped.catalog_metadata())
            metadata["tables"] = dict(reversed(list(metadata["tables"].items())))
            for table in metadata["tables"].values():
                table["foreign_keys"] = list(reversed(table.get("foreign_keys", [])))
            return metadata

        async def sample_rows(self, name, limit=5):
            raise AssertionError("fingerprinting must not sample rows")

        async def execute(self, sql, limit=1000, *, timeout_seconds=30.0):
            raise AssertionError("fingerprinting must not execute SQL")

        def quote_identifier(self, name):
            return self.wrapped.quote_identifier(name)

    normal = asyncio.run(build_catalog(explorer)).catalog
    reordered = asyncio.run(build_catalog(ReorderedExplorer(explorer))).catalog
    assert normal.fingerprint == reordered.fingerprint
    assert {join.id for join in normal.joins} == {join.id for join in reordered.joins}


def test_direct_library_reonboard_blocks_identical_schema_stale_context(tmp_path):
    first = _seed_events(tmp_path / "first.sqlite", 1)
    second = _seed_events(tmp_path / "second.sqlite", 3)
    store = SqliteStore()
    service = SemanticService(store)
    first_summary = asyncio.run(service.onboard("g1", first))
    question = "events source record count"
    identity = Identity(user_id="u1", guild_id="g1", channel_id="c1")
    session = Session(identity=identity)
    session.add(Message(role=Role.USER, content=question))
    tool = SemanticQuery(
        service,
        first_summary.catalog,
        build_attention_envelope(first_summary.catalog, question),
    )
    stale_context = HarnessContext(
        identity=identity,
        llm=FakeLLM(),
        tools=ToolRegistry([tool]),
        session=session,
        explorer=first,
        safety=SafetyPipeline(),
        store=store,
    )

    second_summary = asyncio.run(service.onboard("g1", second))
    assert first_summary.catalog.fingerprint == second_summary.catalog.fingerprint
    assert first_summary.catalog.source_id != second_summary.catalog.source_id
    assert second_summary.catalog.connection_generation == 2

    result = asyncio.run(tool.run(_source_count_args(), stale_context))
    assert result.is_error is True
    assert "connection_stale_pre_execute" in result.content
    assert stale_context.semantic_result_rows == []


def test_managed_identical_schema_switch_invalidates_old_context(tmp_path):
    first = tmp_path / "managed-first.sqlite"
    second = tmp_path / "managed-second.sqlite"
    _seed_events(first, 1)
    _seed_events(second, 3)
    concierge = ContextConcierge(llm=FakeLLM())
    handlers = CommandHandlers(concierge)
    identity = Identity(user_id="admin", guild_id="g1", channel_id="c1", is_admin=True)
    first_dsn = f"sqlite:///{first}"
    second_dsn = f"sqlite:///{second}"

    assert "연결 완료" in asyncio.run(handlers.connect(identity, first_dsn)).text
    first_binding = concierge.connection_binding("g1")
    old_context = asyncio.run(
        concierge.build_context(identity, user_text="events source record count")
    )
    old_context.trusted_reviewed_question = "events source record count"
    assert old_context.semantic_query is not None

    assert "연결 완료" in asyncio.run(handlers.connect(identity, second_dsn)).text
    second_binding = concierge.connection_binding("g1")
    assert first_binding is not None and second_binding is not None
    assert first_binding.source_id != second_binding.source_id
    assert second_binding.generation == first_binding.generation + 1

    result = asyncio.run(
        old_context.semantic_query.run(_source_count_args(), old_context)
    )
    assert result.is_error is True
    assert "stale" in result.content


def test_same_dsn_reactivation_carries_review_but_rotates_generation(tmp_path):
    database = tmp_path / "same.sqlite"
    explorer = _seed_events(database, 2)
    concierge = ContextConcierge(llm=FakeLLM())
    handlers = CommandHandlers(concierge)
    identity = Identity(user_id="admin", guild_id="g1", channel_id="c1", is_admin=True)
    dsn = explorer.url
    assert "연결 완료" in asyncio.run(handlers.connect(identity, dsn)).text
    first_binding = concierge.connection_binding("g1")
    outcome = concierge.semantic.prepare_query(
        scope="g1",
        review_scope="review:u1",
        requester_id="u1",
        explorer=explorer,
        question="total amount",
        metric_id="metric:events.amount",
        metric_phrase="amount",
        aggregate="sum",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    assert outcome.status == "clarification"
    assert (
        concierge.semantic.confirm_pending(
            "g1", "review:u1", "sum", reviewer_id="u1"
        ).status
        == "confirmed"
    )

    assert "연결 완료" in asyncio.run(handlers.connect(identity, dsn)).text
    second_binding = concierge.connection_binding("g1")
    catalog = concierge.semantic.load("g1")
    assert first_binding is not None and second_binding is not None and catalog
    assert first_binding.source_id == second_binding.source_id
    assert second_binding.generation == first_binding.generation + 1
    assert catalog.metric("metric:events.amount").reviewed_bindings == {
        "amount": ["sum"]
    }


def test_generation_change_clears_db_derived_session_history(tmp_path):
    database = tmp_path / "session.sqlite"
    _seed_events(database, 2)
    concierge = ContextConcierge(llm=FakeLLM())
    handlers = CommandHandlers(concierge)
    identity = Identity(user_id="admin", guild_id="g1", channel_id="c1", is_admin=True)
    dsn = f"sqlite:///{database}"
    asyncio.run(handlers.connect(identity, dsn))
    context = asyncio.run(concierge.build_context(identity, user_text="amount"))
    context.session.add(Message(role=Role.ASSISTANT, content="SECRET_OLD_ROW"))
    asyncio.run(concierge.store.save(identity.session_key(), context.session))

    asyncio.run(handlers.connect(identity, dsn))
    refreshed = asyncio.run(concierge.build_context(identity, user_text="amount"))
    assert all(
        "SECRET_OLD_ROW" not in item.content for item in refreshed.session.history()
    )


def test_stale_catalog_cannot_resurrect_after_managed_activation(tmp_path):
    first = tmp_path / "resurrection-a.sqlite"
    second = tmp_path / "resurrection-b.sqlite"
    _seed_events(first, 1)
    _seed_events(second, 3)
    concierge = ContextConcierge(llm=FakeLLM())
    handlers = CommandHandlers(concierge)
    identity = Identity(user_id="admin", guild_id="g1", channel_id="c1", is_admin=True)
    asyncio.run(handlers.connect(identity, f"sqlite:///{first}"))
    stale = concierge.semantic.load("g1")
    assert stale is not None
    asyncio.run(handlers.connect(identity, f"sqlite:///{second}"))
    active = concierge.connection_binding("g1")
    with pytest.raises(RuntimeError, match="connection changed"):
        concierge.semantic.save(
            "g1", stale, expected_review_revision=stale.review_revision
        )
    current = concierge.semantic.load("g1")
    assert active is not None and current is not None
    assert current.source_id == active.source_id
    assert current.connection_generation == active.generation


def test_bound_catalog_revision_cas_rejects_lost_update(tmp_path):
    explorer = _seed_events(tmp_path / "lost-update.sqlite", 2)
    service = SemanticService(SqliteStore())
    asyncio.run(service.onboard("g1", explorer))
    first = service.load("g1")
    second = service.load("g1")
    assert first is not None and second is not None
    prior_revision = first.review_revision
    first.metric("metric:events.amount").aliases.append("alpha amount")
    first.review_revision += 1
    service.save("g1", first, expected_review_revision=prior_revision)
    second.metric("metric:events.amount").aliases.append("beta amount")
    second.review_revision += 1
    with pytest.raises(RuntimeError, match="catalog changed"):
        service.save("g1", second, expected_review_revision=prior_revision)
    final = service.load("g1")
    assert final is not None
    aliases = final.metric("metric:events.amount").aliases
    assert "alpha amount" in aliases
    assert "beta amount" not in aliases


def test_all_store_writes_share_activation_lock():
    store = SqliteStore()
    entered = threading.Event()
    completed = threading.Event()

    def concurrent_write() -> None:
        entered.set()
        store.kv_set("g1", "other", "value")
        completed.set()

    with store._lock:
        store._conn.execute("BEGIN IMMEDIATE")
        thread = threading.Thread(target=concurrent_write)
        thread.start()
        assert entered.wait(timeout=1)
        time.sleep(0.02)
        assert completed.is_set() is False
        assert store._conn.in_transaction is True
        store._conn.rollback()
    thread.join(timeout=1)
    assert completed.is_set() is True


def test_relative_file_connection_is_frozen_to_absolute_path(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    canonical = canonicalize_connection("sqlite:///relative.sqlite")
    assert canonical.startswith("sqlite:////")
    assert str(tmp_path / "relative.sqlite") in canonical


def test_sqlite_timeout_interrupts_and_connection_is_reusable(tmp_path):
    explorer = _seed_events(tmp_path / "timeout.sqlite", 1)
    long_sql = (
        "WITH RECURSIVE counter(x) AS (SELECT 1 UNION ALL "
        "SELECT x + 1 FROM counter WHERE x < 100000000) SELECT SUM(x) FROM counter"
    )
    with pytest.raises(QueryTimedOutError):
        asyncio.run(explorer.execute(long_sql, timeout_seconds=0.01))
    assert asyncio.run(explorer.execute("SELECT 1 AS ok", timeout_seconds=1)) == [
        {"ok": 1}
    ]


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf")])
def test_sqlite_timeout_rejects_non_positive_or_non_finite_values(tmp_path, timeout):
    explorer = _seed_events(tmp_path / f"timeout-{str(timeout)}.sqlite", 1)
    with pytest.raises(ValueError, match="finite positive"):
        asyncio.run(explorer.execute("SELECT 1", timeout_seconds=timeout))


def test_cancelled_sqlite_query_leaves_no_worker_error_and_is_reusable(tmp_path):
    explorer = _seed_events(tmp_path / "cancel.sqlite", 1)
    long_sql = (
        "WITH RECURSIVE counter(x) AS (SELECT 1 UNION ALL "
        "SELECT x + 1 FROM counter WHERE x < 100000000) SELECT SUM(x) FROM counter"
    )

    async def scenario() -> tuple[list[dict], list[dict[str, object]]]:
        loop = asyncio.get_running_loop()
        errors: list[dict[str, object]] = []
        loop.set_exception_handler(lambda _loop, context: errors.append(context))
        task = asyncio.create_task(explorer.execute(long_sql, timeout_seconds=30))
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        rows = await explorer.execute("SELECT 1 AS ok", timeout_seconds=1)
        await asyncio.sleep(0)
        return rows, errors

    rows, errors = asyncio.run(scenario())
    assert rows == [{"ok": 1}]
    assert errors == []


def test_agent_loop_clears_stale_structured_payload_before_early_clarification():
    identity = Identity(user_id="u1", guild_id="g1", channel_id="c1")
    context = HarnessContext(
        identity=identity,
        llm=FakeLLM(),
        tools=ToolRegistry(),
        session=Session(identity=identity),
        semantic_attention_state="clarify_metric",
        semantic_attention_message="metric required",
        semantic_result_ready=True,
        semantic_result_message="old",
        semantic_result_headers=("old",),
        semantic_result_rows=[("SECRET_OLD_ROW",)],
    )
    answer = asyncio.run(agent_loop(context, "new question"))
    assert answer.startswith("NEEDS CLARIFICATION")
    assert context.semantic_result_ready is False
    assert context.semantic_result_rows == []
    assert context.semantic_result_stamp == ()
