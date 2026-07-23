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
import re

from lang2sql.core.identity import ScopeLevel
from lang2sql.adapters.llm.fake import FakeLLM
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
from lang2sql.semantic.catalog import MetricSpec, SemanticCatalog
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
    assert "id | name" in msg.text
    assert "1 | a" in msg.text


def test_render_rows_neutralizes_discord_and_spreadsheet_injection() -> None:
    small = render_answer(
        "",
        [["@everyone", "**bold**", "[link](https://example.invalid)"]],
        header=["mention", "format", "link"],
    )
    assert "@\u200beveryone" in small.text
    assert "\\*\\*bold\\*\\*" in small.text
    assert "\\[link\\]\\(https://example\\.invalid\\)" in small.text

    large_rows = [["=2+2", "+cmd", "-1", "@SUM(A1)"]] * 51
    large = render_answer(
        "formula-safe",
        large_rows,
        header=["a", "b", "c", "d"],
    )
    assert large.file_bytes is not None
    csv_text = large.file_bytes.decode("utf-8")
    assert "'=2+2" in csv_text
    assert "'+cmd" in csv_text
    assert "'-1" in csv_text
    assert "'@SUM(A1)" in csv_text


def test_render_many_text_lines_attaches() -> None:
    text = "\n".join(f"line {i}" for i in range(MAX_INLINE_ROWS + 1))
    msg = render_answer(text)
    assert msg.file_bytes is not None
    assert "lines" in msg.text


def test_render_few_very_wide_rows_attach_instead_of_overflowing() -> None:
    msg = render_answer("", [["x" * 3000]], header=["wide"])
    assert msg.file_bytes is not None
    assert len(msg.text) < 1900


def test_metric_candidate_metadata_is_sanitized_without_mutating_lookup_id() -> None:
    raw_id = (
        "metric:table.bad`name\n@everyone<@123>[x](https://bad.invalid)\u202e"
        + "z" * 4200
    )
    concierge = ContextConcierge()
    concierge.semantic.save(
        "g1",
        SemanticCatalog(
            fingerprint="malicious-metadata",
            metrics=[
                MetricSpec(
                    id=raw_id,
                    label=raw_id,
                    table_id="table",
                    column="bad",
                    data_type="REAL",
                )
            ],
        ),
    )
    handlers = CommandHandlers(concierge)
    admin = to_identity(
        InteractionContext(
            user_id="admin", guild_id="g1", channel_id="c1", is_admin=True
        )
    )

    shown = asyncio.run(handlers.semantic_metric_candidates(admin))
    assert "@everyone" not in shown.text
    assert "@\u200beveryone" in shown.text
    assert "\u202e" not in shown.text
    assert "<@123>" not in shown.text
    assert len(shown.text) < 1900 or shown.file_bytes is not None
    token_match = re.search(r"candidate_token: ([A-Za-z0-9_-]+)", shown.text)
    assert token_match is not None

    warning = asyncio.run(
        handlers.semantic_metric_map(
            admin, token_match.group(1), "safe business metric", confirm=False
        )
    )
    assert "표현에 묶였습니다" in warning.text

    mapped = asyncio.run(
        handlers.semantic_metric_map(
            admin, token_match.group(1), "safe business metric", confirm=True
        )
    )
    assert mapped.text.startswith("✅")
    stored = concierge.semantic.load("g1")
    assert stored is not None
    metric = stored.metric(raw_id)
    assert metric is not None
    assert metric.id == raw_id
    assert "safe business metric" in metric.aliases


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
    handlers = CommandHandlers(
        ContextConcierge(llm=FakeLLM()), query_channel_ids={"c1"}
    )
    ident = to_identity(
        InteractionContext(user_id="u3", guild_id="g1", channel_id="c1")
    )
    out = asyncio.run(handlers.query(ident, "how many users signed up?"))
    assert isinstance(out.text, str)
    assert out.text  # non-empty


def test_query_persists_session() -> None:
    concierge = ContextConcierge(llm=FakeLLM())
    handlers = CommandHandlers(concierge, query_channel_ids={"c1"})
    ident = to_identity(
        InteractionContext(user_id="u4", guild_id="g1", channel_id="c1")
    )

    async def scenario():
        await handlers.query(ident, "first question")
        return await concierge.store.load(ident.session_key())

    saved = asyncio.run(scenario())
    assert saved is not None
    assert any(m.content == "first question" for m in saved.transcript)


def test_help_keeps_first_connect_recovery_inline() -> None:
    handlers = CommandHandlers(ContextConcierge(llm=FakeLLM()))

    help_text = asyncio.run(handlers.help()).text

    assert len(help_text) <= 1900
    assert "/semantic_dimension_candidates" in help_text
    assert "/semantic_dimension_map" in help_text
    assert "LANG2SQL_DISCORD_QUERY_CHANNEL_IDS" in help_text


def test_guild_member_query_is_fail_closed_without_explicit_opt_in() -> None:
    class ContextMustNotBeBuilt(ContextConcierge):
        build_calls = 0

        async def build_context(self, *args, **kwargs):
            self.build_calls += 1
            raise AssertionError("authorization must run before context construction")

    concierge = ContextMustNotBeBuilt(llm=FakeLLM())
    handlers = CommandHandlers(concierge)
    member = to_identity(
        InteractionContext(user_id="member", guild_id="g1", channel_id="c1")
    )

    blocked = asyncio.run(handlers.query(member, "how many users signed up?"))

    assert blocked.text.startswith("BLOCKED (guild_query_access)")
    assert concierge.build_calls == 0
    assert asyncio.run(concierge.store.load(member.session_key())) is None


def test_guild_member_query_requires_explicit_channel_allowlist() -> None:
    handlers = CommandHandlers(
        ContextConcierge(llm=FakeLLM()), query_channel_ids={"c1"}
    )
    member = to_identity(
        InteractionContext(user_id="member", guild_id="g1", channel_id="c1")
    )

    allowed = asyncio.run(handlers.query(member, "how many users signed up?"))

    assert not allowed.text.startswith("BLOCKED (guild_query_access)")


def test_admin_and_dm_queries_do_not_require_channel_allowlist() -> None:
    handlers = CommandHandlers(ContextConcierge(llm=FakeLLM()))
    admin = to_identity(
        InteractionContext(
            user_id="admin", guild_id="g1", channel_id="unlisted", is_admin=True
        )
    )
    dm_user = to_identity(InteractionContext(user_id="dm-user"))

    admin_result = asyncio.run(handlers.query(admin, "how many users?"))
    dm_result = asyncio.run(handlers.query(dm_user, "how many users?"))

    assert not admin_result.text.startswith("BLOCKED (guild_query_access)")
    assert not dm_result.text.startswith("BLOCKED (guild_query_access)")


def test_thread_query_uses_parent_channel_allowlist() -> None:
    handlers = CommandHandlers(
        ContextConcierge(llm=FakeLLM()), query_channel_ids={"parent"}
    )
    thread_member = to_identity(
        InteractionContext(
            user_id="member",
            guild_id="g1",
            channel_id="parent",
            thread_id="thread-1",
        )
    )

    allowed = asyncio.run(handlers.query(thread_member, "how many users?"))

    assert not allowed.text.startswith("BLOCKED (guild_query_access)")


def test_discord_query_channel_parser_is_strict_and_fail_closed() -> None:
    from lang2sql.frontends.discord.bot import _parse_query_channel_ids

    assert _parse_query_channel_ids("") == frozenset()
    assert _parse_query_channel_ids(" 123,456 ") == frozenset({"123", "456"})
    for malformed in (",", "123,", "all", "-1", "0", "123,abc", "１２３"):
        try:
            _parse_query_channel_ids(malformed)
        except ValueError:
            continue
        raise AssertionError(f"malformed channel allowlist was accepted: {malformed}")


def test_connect_requires_admin_for_guild() -> None:
    concierge = ContextConcierge()
    handlers = CommandHandlers(concierge)
    ident = to_identity(
        InteractionContext(user_id="u5", guild_id="g1", channel_id="c1")
    )
    out = asyncio.run(handlers.connect(ident, "postgresql://localhost/db"))
    assert "관리자만" in out.text
    assert concierge.store.kv_get("g1", "dsn") is None


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


def test_governance_reply_path_is_ephemeral_for_defer_and_followup() -> None:
    from types import SimpleNamespace

    from lang2sql.core.ports.frontend import OutboundMessage
    from lang2sql.frontends.discord.bot import Lang2SQLBot

    calls: dict[str, dict] = {}

    class Response:
        async def defer(self, **kwargs):
            calls["defer"] = kwargs

    class Followup:
        async def send(self, **kwargs):
            calls["send"] = kwargs

    async def handler():
        return OutboundMessage(text="private schema metadata")

    interaction = SimpleNamespace(response=Response(), followup=Followup())
    asyncio.run(
        Lang2SQLBot._run(
            object(), interaction, handler(), ephemeral=True  # type: ignore[arg-type]
        )
    )
    assert calls["defer"] == {"thinking": True, "ephemeral": True}
    assert calls["send"]["ephemeral"] is True
    assert calls["send"]["content"] == "private schema metadata"


def test_every_semantic_governance_command_selects_ephemeral_run() -> None:
    import inspect

    from lang2sql.frontends.discord.bot import Lang2SQLBot

    source = inspect.getsource(Lang2SQLBot._register_commands)
    names = (
        "semantic_status",
        "semantic_candidates",
        "semantic_dimension_candidates",
        "semantic_dimension_map",
        "semantic_metric_candidates",
        "semantic_metric_map",
        "semantic_reviews",
        "semantic_release",
        "semantic_revoke",
        "semantic_public_data",
        "semantic_reset",
        "semantic_review",
    )
    for index, name in enumerate(names):
        start = source.index(f'name="{name}"')
        end = (
            source.index(f'name="{names[index + 1]}"', start)
            if index + 1 < len(names)
            else source.index('name="audit_me"', start)
        )
        assert "ephemeral=True" in source[start:end], name


def test_message_gate_requires_the_bot_user_id_not_mention_everyone() -> None:
    from types import SimpleNamespace

    from lang2sql.frontends.discord.bot import _is_direct_user_mention

    everyone_only = SimpleNamespace(raw_mentions=[], mention_everyone=True)
    direct = SimpleNamespace(raw_mentions=[42], mention_everyone=False)
    nickname_direct = SimpleNamespace(raw_mentions=[42], mention_everyone=False)

    assert not _is_direct_user_mention(everyone_only, 42)
    assert _is_direct_user_mention(direct, 42)
    assert _is_direct_user_mention(nickname_direct, 42)
    assert not _is_direct_user_mention(direct, None)
