"""Unit tests for the Discord frontend — no live bot, no token, no network.

Covers the three pure modules (session_router mapping, render thresholds,
CommandHandlers against a real in-memory ContextConcierge) plus the import-
safety contract that ``bot.py`` loads with no ``DISCORD_BOT_TOKEN`` set.

Async handlers are driven with :func:`asyncio.run` to match the convention in
the rest of the suite (no pytest-asyncio marker plumbing).
"""

from __future__ import annotations

import asyncio
import os

from lang2sql.core.identity import ScopeLevel
from lang2sql.frontends.discord import (
    CommandHandlers,
    InteractionContext,
    is_channel,
    is_dm,
    is_thread,
    render_answer,
    to_identity,
)
from lang2sql.frontends.discord.render import MAX_INLINE_ROWS
from lang2sql.tenancy.concierge import ContextConcierge

# -- session_router -------------------------------------------------------


def test_dm_identity_has_no_guild() -> None:
    ident = to_identity(InteractionContext(user_id="u1"))
    assert is_dm(ident)
    assert not is_channel(ident)
    assert not is_thread(ident)
    assert ident.session_key() == "dm:u1"


def test_channel_identity_scopes_to_channel() -> None:
    ident = to_identity(
        InteractionContext(user_id="u1", guild_id="g1", channel_id="c1")
    )
    assert is_channel(ident)
    assert not is_dm(ident)
    assert not is_thread(ident)
    assert ident.session_key() == "channel:c1"
    # Federation chain runs narrow→wide: channel, guild, builtin.
    levels = [s.level for s in ident.scope_chain()]
    assert levels == [ScopeLevel.CHANNEL, ScopeLevel.GUILD, ScopeLevel.BUILTIN]


def test_thread_identity_is_narrowest() -> None:
    ident = to_identity(
        InteractionContext(user_id="u1", guild_id="g1", channel_id="c1", thread_id="t1")
    )
    assert is_thread(ident)
    assert ident.session_key() == "thread:t1"
    assert ident.scope_chain()[0].level is ScopeLevel.THREAD


def test_admin_flag_propagates() -> None:
    ident = to_identity(InteractionContext(user_id="u1", guild_id="g1", is_admin=True))
    assert ident.is_admin is True


# -- render ---------------------------------------------------------------


def test_render_small_text_is_plain() -> None:
    msg = render_answer("just a short answer")
    assert msg.text == "just a short answer"
    assert msg.file_bytes is None
    assert msg.file_name is None


def test_render_large_rows_attaches_csv() -> None:
    rows = [[i, f"name{i}"] for i in range(MAX_INLINE_ROWS + 5)]
    msg = render_answer("Top users:", rows, header=["id", "name"])
    assert msg.file_bytes is not None
    assert msg.file_name == "result.csv"
    assert "55 rows" in msg.text
    assert "Top users:" in msg.text
    decoded = msg.file_bytes.decode("utf-8")
    assert decoded.startswith("id,name")
    assert "name54" in decoded


def test_render_small_rows_inlined() -> None:
    rows = [[1, "a"], [2, "b"]]
    msg = render_answer("", rows, header=["id", "name"])
    assert msg.file_bytes is None
    assert "id,name" in msg.text
    assert "1,a" in msg.text


def test_render_many_text_lines_attaches() -> None:
    text = "\n".join(f"line {i}" for i in range(MAX_INLINE_ROWS + 1))
    msg = render_answer(text)
    assert msg.file_bytes is not None
    assert "lines" in msg.text


# -- CommandHandlers (real in-memory concierge) ---------------------------


def test_term_custom_then_list() -> None:
    handlers = CommandHandlers(ContextConcierge())
    ident = to_identity(
        InteractionContext(user_id="u1", guild_id="g1", channel_id="c1")
    )

    async def scenario() -> tuple[str, str]:
        defined = await handlers.term_custom(
            ident,
            term="active_user",
            definition="logged in within 30 days",
            layer="channel",
        )
        shown = await handlers.term_custom(ident, list_all=True)
        return defined.text, shown.text

    defined_text, shown_text = asyncio.run(scenario())
    assert "active_user" in defined_text
    assert "active_user" in shown_text
    assert "logged in within 30 days" in shown_text


def test_term_custom_list_empty_scope() -> None:
    handlers = CommandHandlers(ContextConcierge())
    ident = to_identity(
        InteractionContext(user_id="solo", guild_id="g9", channel_id="c9")
    )
    shown = asyncio.run(handlers.term_custom(ident, list_all=True))
    assert shown.text  # empty scope returns some message


def test_term_custom_is_scope_isolated() -> None:
    """A channel definition must not leak into a different channel (federation)."""
    handlers = CommandHandlers(ContextConcierge())
    marketing = to_identity(
        InteractionContext(user_id="u1", guild_id="g1", channel_id="mkt")
    )
    product = to_identity(
        InteractionContext(user_id="u1", guild_id="g1", channel_id="prd")
    )

    async def scenario() -> str:
        await handlers.term_custom(
            marketing, term="active_user", definition="30d login", layer="channel"
        )
        return (await handlers.term_custom(product, list_all=True)).text

    assert "active_user" not in asyncio.run(scenario())


def test_remember_and_audit_me() -> None:
    handlers = CommandHandlers(ContextConcierge())
    ident = to_identity(
        InteractionContext(user_id="u2", guild_id="g1", channel_id="c1")
    )

    async def scenario() -> tuple[str, str]:
        remembered = await handlers.remember(ident, "prefers ISO dates")
        audit = await handlers.audit_me(ident)
        return remembered.text, audit.text

    remembered_text, audit_text = asyncio.run(scenario())
    assert "prefers ISO dates" in remembered_text
    assert "remember" in audit_text


def test_audit_me_empty() -> None:
    handlers = CommandHandlers(ContextConcierge())
    ident = to_identity(
        InteractionContext(user_id="never-acted", guild_id="g1", channel_id="c1")
    )
    audit = asyncio.run(handlers.audit_me(ident))
    assert "No audited activity" in audit.text


def test_query_returns_outbound_message() -> None:
    """With the default FakeLLM (no OPENAI key), a query still returns text."""
    handlers = CommandHandlers(ContextConcierge())
    ident = to_identity(
        InteractionContext(user_id="u3", guild_id="g1", channel_id="c1")
    )
    out = asyncio.run(handlers.query(ident, "how many users signed up?"))
    assert isinstance(out.text, str)
    assert out.text  # non-empty


def test_query_persists_session() -> None:
    concierge = ContextConcierge()
    handlers = CommandHandlers(concierge)
    ident = to_identity(
        InteractionContext(user_id="u4", guild_id="g1", channel_id="c1")
    )

    async def scenario():
        await handlers.query(ident, "first question")
        return await concierge.store.load(ident.session_key())

    saved = asyncio.run(scenario())
    assert saved is not None
    assert any(m.content == "first question" for m in saved.transcript)


def test_connect_stub_acknowledges() -> None:
    concierge = ContextConcierge()
    handlers = CommandHandlers(concierge)
    ident = to_identity(
        InteractionContext(user_id="u5", guild_id="g1", channel_id="c1")
    )
    out = asyncio.run(handlers.connect(ident, "postgresql://localhost/db"))
    assert "saved" in out.text.lower()
    assert concierge.store.kv_get("g1", "dsn") == "postgresql://localhost/db"


def test_ingest_lists_or_reports() -> None:
    handlers = CommandHandlers(ContextConcierge())
    ident = to_identity(
        InteractionContext(user_id="u6", guild_id="g1", channel_id="c1")
    )
    out = asyncio.run(
        handlers.ingest(ident, content="total_revenue is the sum of order amounts")
    )
    assert isinstance(out.text, str)
    assert out.text


# -- import-safety contract -----------------------------------------------


def test_bot_imports_without_token() -> None:
    """Importing bot.py must not require a token or a network connection."""
    os.environ.pop("DISCORD_BOT_TOKEN", None)
    import lang2sql.frontends.discord.bot as bot  # noqa: F401

    assert hasattr(bot, "run")
