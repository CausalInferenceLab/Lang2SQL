"""Week-1 smoke test: the agent loop completes a full tool cycle.

Locks in the walking-skeleton contract — user turn → tool call → tool result →
final answer — so later refactors of loop/registry/context don't silently break
the dispatch path.
"""

from __future__ import annotations

import asyncio

from lang2sql.adapters.llm.fake import FakeLLM
from lang2sql.core.identity import Identity
from lang2sql.core.types import Completion, Role, ToolCall
from lang2sql.harness.context import HarnessContext
from lang2sql.harness.loop import agent_loop
from lang2sql.harness.session import Session
from lang2sql.harness.tool_registry import ToolRegistry
from lang2sql.tools.ping import Ping
from lang2sql.tools.ask_user import AskUser


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


class _AskThenActLLM:
    def __init__(self, *, sibling: bool = False) -> None:
        self.calls = 0
        self.sibling = sibling

    async def complete(self, messages, tools=()):
        self.calls += 1
        if self.calls == 1:
            calls = [
                ToolCall(
                    id="ask-1",
                    name="ask_user",
                    arguments={"question": "Which amount?"},
                )
            ]
            if self.sibling:
                calls.append(ToolCall(id="ping-1", name="ping", arguments={}))
            return Completion(tool_calls=calls)
        return Completion(
            tool_calls=[ToolCall(id="ping-2", name="ping", arguments={})]
        )


def _ask_ctx(llm: _AskThenActLLM) -> HarnessContext:
    identity = Identity(user_id="tester")
    return HarnessContext(
        identity=identity,
        llm=llm,
        tools=ToolRegistry([AskUser(), Ping()]),
        session=Session(identity=identity),
    )


def test_ask_user_suspends_until_the_next_real_user_turn():
    llm = _AskThenActLLM()
    ctx = _ask_ctx(llm)

    answer = asyncio.run(agent_loop(ctx, "ambiguous request"))

    assert answer == "❓ Awaiting user: Which amount?"
    assert llm.calls == 1
    assert [message.role for message in ctx.session.history()] == [
        Role.USER,
        Role.ASSISTANT,
        Role.TOOL,
    ]


def test_ask_user_with_a_sibling_blocks_every_tool_without_dispatch():
    llm = _AskThenActLLM(sibling=True)
    ctx = _ask_ctx(llm)

    answer = asyncio.run(agent_loop(ctx, "ambiguous request"))

    assert answer.startswith("BLOCKED (ask_user_must_be_single_call)")
    assert llm.calls == 1
    tool_messages = [
        message for message in ctx.session.history() if message.role == Role.TOOL
    ]
    assert [message.name for message in tool_messages] == ["ask_user", "ping"]
    assert all(message.content == answer for message in tool_messages)


def test_scope_chain_orders_narrow_to_wide():
    ident = Identity(user_id="u", guild_id="g", channel_id="c", thread_id="t")
    levels = [s.level.value for s in ident.scope_chain()]
    assert levels == ["thread", "channel", "guild", "builtin"]
