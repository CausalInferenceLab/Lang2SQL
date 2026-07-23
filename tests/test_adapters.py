"""Adapter tests — SqliteStore round trips, OpenAILLM offline behaviour, stub.

No network: OpenAI is only checked for construction + the no-key RuntimeError.
Async port methods are driven with ``asyncio.run``.
"""

from __future__ import annotations

import asyncio
import os

import pytest

from lang2sql.adapters.db.postgres_explorer import PostgresExplorer
from lang2sql.adapters.llm.openai_ import OpenAILLM
from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.core.identity import Identity
from lang2sql.core.ports.audit import AuditEvent
from lang2sql.core.ports.explorer import (
    ExplorerPort,
    QueryTimeoutUnsupportedError,
)
from lang2sql.core.types import Message, Role, ToolCall
from lang2sql.harness.session import Session


def test_audit_record_then_query() -> None:
    store = SqliteStore()
    asyncio.run(
        store.record(
            AuditEvent(
                actor="u1", action="run_sql", scope="s", detail={"q": "SELECT 1"}
            )
        )
    )
    asyncio.run(
        store.record(
            AuditEvent(actor="u1", action="define_metric", scope="s", detail={})
        )
    )
    asyncio.run(store.record(AuditEvent(actor="other", action="run_sql", scope="s")))

    events = asyncio.run(store.query("u1"))
    assert len(events) == 2
    # Newest first.
    assert events[0].action == "define_metric"
    assert events[1].action == "run_sql"
    assert events[1].detail == {"q": "SELECT 1"}
    assert events[0].ts > 0  # store fills the timestamp


def test_session_save_then_load_reconstructs_transcript() -> None:
    store = SqliteStore()
    identity = Identity(
        user_id="u1", guild_id="g", channel_id="c", thread_id="t", is_admin=True
    )
    session = Session(identity=identity)
    session.add(Message(role=Role.USER, content="hi"))
    session.add(
        Message(
            role=Role.ASSISTANT,
            content="",
            tool_calls=[
                ToolCall(id="call_1", name="run_sql", arguments={"q": "SELECT 1"})
            ],
        )
    )
    session.add(
        Message(role=Role.TOOL, content="ok", tool_call_id="call_1", name="run_sql")
    )

    key = identity.session_key()
    asyncio.run(store.save(key, session))
    loaded = asyncio.run(store.load(key))

    assert loaded is not None
    assert loaded.identity == identity
    assert len(loaded.transcript) == 3
    assert loaded.transcript[0].role == Role.USER
    tc = loaded.transcript[1].tool_calls[0]
    assert tc.id == "call_1"
    assert tc.name == "run_sql"
    assert tc.arguments == {"q": "SELECT 1"}
    assert loaded.transcript[2].tool_call_id == "call_1"
    assert loaded.transcript[2].name == "run_sql"


def test_session_load_missing_returns_none() -> None:
    store = SqliteStore()
    assert asyncio.run(store.load("nope")) is None


def test_kv_set_get_delete() -> None:
    store = SqliteStore()
    assert store.kv_get("scope", "k") is None
    store.kv_set("scope", "k", "v1")
    assert store.kv_get("scope", "k") == "v1"
    store.kv_set("scope", "k", "v2")  # upsert
    assert store.kv_get("scope", "k") == "v2"
    store.kv_delete("scope", "k")
    assert store.kv_get("scope", "k") is None


def test_postgres_explorer_canned_data() -> None:
    explorer = PostgresExplorer("postgresql://ignored")
    tables = asyncio.run(explorer.list_tables())
    names = {t.qualified for t in tables}
    assert "public.orders" in names
    assert "public.users" in names

    orders = asyncio.run(explorer.describe_table("orders"))
    assert {c.name for c in orders.columns} == {"id", "amount", "status", "created_at"}

    rows = asyncio.run(explorer.sample_rows("public.orders", limit=1))
    assert len(rows) == 1
    assert "status" in rows[0]


def test_postgres_explorer_satisfies_protocol() -> None:
    explorer = PostgresExplorer("postgresql://ignored")
    assert isinstance(explorer, ExplorerPort)


def test_postgres_metadata_stub_refuses_unbounded_execute() -> None:
    explorer = PostgresExplorer("postgresql://ignored")
    with pytest.raises(QueryTimeoutUnsupportedError, match="statement cancellation"):
        asyncio.run(explorer.execute("SELECT * FROM orders WHERE status='paid'"))


def test_openai_constructs_offline() -> None:
    # No network, no key required to construct.
    llm = OpenAILLM(model="gpt-4.1-mini", api_key=None, base_url="http://localhost:0")
    assert llm.model == "gpt-4.1-mini"


def test_openai_complete_without_key_raises() -> None:
    saved = os.environ.pop("OPENAI_API_KEY", None)
    try:
        llm = OpenAILLM(api_key=None)
        raised = False
        try:
            asyncio.run(llm.complete([Message(role=Role.USER, content="hi")]))
        except RuntimeError as exc:
            raised = True
            assert "OPENAI_API_KEY not set" in str(exc)
        assert raised
    finally:
        if saved is not None:
            os.environ["OPENAI_API_KEY"] = saved
