"""Week-1 smoke test: the agent loop completes a full tool cycle.

Locks in the walking-skeleton contract — user turn → tool call → tool result →
final answer — so later refactors of loop/registry/context don't silently break
the dispatch path.
"""

from __future__ import annotations

import asyncio

from lang2sql.adapters.db.postgres_explorer import PostgresExplorer
from lang2sql.adapters.llm.fake import FakeLLM
from lang2sql.core.identity import Identity
from lang2sql.core.types import Completion, Role, ToolCall
from lang2sql.harness.context import HarnessContext
from lang2sql.harness.loop import agent_loop
from lang2sql.harness.session import Session
from lang2sql.harness.tool_registry import ToolRegistry
from lang2sql.tools.explore_schema import ExploreSchema
from lang2sql.tools.ping import Ping


def _ctx() -> HarnessContext:
    identity = Identity(user_id="tester")
    return HarnessContext(
        identity=identity,
        llm=FakeLLM(),
        tools=ToolRegistry([Ping()]),
        session=Session(identity=identity),
    )


def test_loop_runs_tool_then_answers():
    ctx = _ctx()
    answer = asyncio.run(agent_loop(ctx, "hello"))
    assert "pong" in answer  # tool ran and its result reached the final answer


def test_transcript_shape():
    ctx = _ctx()
    asyncio.run(agent_loop(ctx, "hello"))
    roles = [m.role for m in ctx.session.history()]
    # user → assistant(tool_call) → tool → assistant(final)
    assert roles == [Role.USER, Role.ASSISTANT, Role.TOOL, Role.ASSISTANT]


def test_tool_call_id_is_stamped():
    ctx = _ctx()
    asyncio.run(agent_loop(ctx, "hello"))
    tool_msgs = [m for m in ctx.session.history() if m.role == Role.TOOL]
    assert tool_msgs and tool_msgs[0].tool_call_id


def test_schema_batch_uses_the_catalog_from_the_system_prompt():
    class CountingExplorer(PostgresExplorer):
        catalog_reads = 0

        async def list_tables(self):
            self.catalog_reads += 1
            return await super().list_tables()

    class SchemaLLM:
        async def complete(self, messages, tools=()):
            if messages[-1].role == Role.TOOL:
                assert "- amount: numeric" in messages[-1].content
                assert "- email: text" in messages[-1].content
                return Completion(content="Found both tables.")
            assert "## Known tables\npublic.orders, public.users" in messages[0].content
            return Completion(
                tool_calls=[
                    ToolCall(
                        id="schema",
                        name="explore_schema",
                        arguments={"tables": ["public.orders", "public.users"]},
                    )
                ]
            )

    ctx = _ctx()
    explorer = CountingExplorer("postgresql://ignored")
    ctx.explorer = explorer
    ctx.llm = SchemaLLM()
    ctx.tools = ToolRegistry([ExploreSchema()])

    assert asyncio.run(agent_loop(ctx, "Show the schema for orders and users.")) == (
        "Found both tables."
    )
    assert explorer.catalog_reads == 1


def test_scope_chain_orders_narrow_to_wide():
    ident = Identity(user_id="u", guild_id="g", channel_id="c", thread_id="t")
    levels = [s.level.value for s in ident.scope_chain()]
    assert levels == ["thread", "team", "org", "builtin"]
