"""Edge-case tests — exercises real production code, not inline re-implementations."""

from __future__ import annotations

import asyncio
import json

from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.frontends.discord.render import OutboundMessage
from lang2sql.tools.semantic_federation import FedEntry, _kv_key, _render_effective

# -- FedEntry synonyms: string stored in KV (pre-fix data or JSON null) -------


def test_fed_entry_from_json_coerces_string_synonyms() -> None:
    """If old KV data has synonyms as a JSON string, from_json must produce a list."""
    raw_json = json.dumps(
        {
            "term": "active_user",
            "layer": "guild",
            "entity": "",
            "definition": "30d login",
            "synonyms": "활성유저, active",
            "inferred": False,
        }
    )
    entry = FedEntry.from_json(raw_json)
    assert isinstance(entry.synonyms, list), "synonyms must be a list after from_json"
    assert "활성유저" in entry.synonyms
    assert "active user" not in entry.synonyms or "active" in entry.synonyms


def test_fed_entry_from_json_handles_null_synonyms() -> None:
    """If KV data has synonyms=null, from_json must produce an empty list."""
    raw_json = json.dumps(
        {
            "term": "revenue",
            "layer": "guild",
            "entity": "",
            "definition": "gross revenue",
            "synonyms": None,
            "inferred": False,
        }
    )
    entry = FedEntry.from_json(raw_json)
    assert entry.synonyms == []


def test_render_effective_string_synonyms_in_kv_does_not_character_join() -> None:
    """Old KV entry with string synonyms renders words, not individual characters."""
    store = SqliteStore()
    scope = "g1"
    # Simulate a KV entry written by old code (synonyms as JSON string)
    bad_json = json.dumps(
        {
            "term": "active_user",
            "layer": "guild",
            "entity": "",
            "definition": "30d login",
            "synonyms": "활성유저, active",
            "inferred": False,
        }
    )
    store.kv_set(scope, _kv_key("active_user", "guild", ""), bad_json)
    rendered = _render_effective(store, scope, "", "u1")
    assert "active_user" in rendered
    assert "활, 성" not in rendered  # character-join would produce this


# -- bot.py _build_send_kwargs -- tests real helper, not inline re-impl --------


def test_build_send_kwargs_omits_file_for_text_only() -> None:
    """_build_send_kwargs must not include 'file' key when response has no attachment."""
    from lang2sql.frontends.discord.bot import _build_send_kwargs

    out = OutboundMessage(text="42 users", file_bytes=None)
    kwargs = _build_send_kwargs(out)
    assert "file" not in kwargs
    assert kwargs["content"] == "42 users"
    assert kwargs["allowed_mentions"].everyone is False
    assert kwargs["allowed_mentions"].users is False
    assert kwargs["allowed_mentions"].roles is False


def test_build_send_kwargs_includes_file_for_csv() -> None:
    """_build_send_kwargs must include 'file' key when response has CSV bytes."""
    from lang2sql.frontends.discord.bot import _build_send_kwargs

    csv_bytes = b"id,name\n1,alice"
    out = OutboundMessage(text="Results:", file_bytes=csv_bytes, file_name="result.csv")
    kwargs = _build_send_kwargs(out)
    assert "file" in kwargs
    assert kwargs["file"] is not None


# -- term_custom audit events --------------------------------------------------


def test_term_custom_remove_emits_audit_event() -> None:
    """Deleting a term must be recorded in the audit log."""
    from lang2sql.core.identity import Identity
    from lang2sql.tenancy.concierge import ContextConcierge
    from lang2sql.tools.semantic_federation import SemanticFederationTool

    concierge = ContextConcierge()
    ident = Identity(user_id="u1", guild_id="g1", channel_id="c1", is_admin=True)
    ctx = asyncio.run(concierge.build_context(ident))

    # Write then remove
    asyncio.run(
        SemanticFederationTool().run(
            {"term": "active_user", "definition": "30d login", "layer": "guild"}, ctx
        )
    )
    asyncio.run(
        SemanticFederationTool().run({"term": "active_user", "remove": True}, ctx)
    )

    events = asyncio.run(ctx.audit.query(ident.user_id))
    assert any(
        e.action == "term_custom_remove" and e.detail.get("term") == "active_user"
        for e in events
    )


# -- guild layer admin guard ---------------------------------------------------


def test_guild_write_requires_admin() -> None:
    """Non-admin must be blocked from writing guild-layer terms via term_custom."""
    from lang2sql.core.identity import Identity
    from lang2sql.tenancy.concierge import ContextConcierge
    from lang2sql.tools.semantic_federation import SemanticFederationTool

    concierge = ContextConcierge()
    ident = Identity(user_id="u1", guild_id="g1", channel_id="c1", is_admin=False)
    ctx = asyncio.run(concierge.build_context(ident))

    result = asyncio.run(
        SemanticFederationTool().run(
            {"term": "revenue", "definition": "gross revenue", "layer": "guild"}, ctx
        )
    )
    assert result.is_error
    assert "관리자" in result.content


def test_guild_remove_non_admin_skips_guild_keeps_own_entry() -> None:
    """Non-admin remove skips guild-layer entry but deletes their own member entry."""
    from lang2sql.core.identity import Identity
    from lang2sql.tenancy.concierge import ContextConcierge
    from lang2sql.tools.semantic_federation import SemanticFederationTool, _kv_key

    concierge = ContextConcierge()

    # Admin registers the guild-layer term
    admin_ctx = asyncio.run(
        concierge.build_context(
            Identity(user_id="admin", guild_id="g1", channel_id="c1", is_admin=True)
        )
    )
    asyncio.run(
        SemanticFederationTool().run(
            {"term": "revenue", "definition": "gross revenue", "layer": "guild"},
            admin_ctx,
        )
    )

    # Non-admin adds their own member-layer override
    member_ctx = asyncio.run(
        concierge.build_context(
            Identity(user_id="u1", guild_id="g1", channel_id="c1", is_admin=False)
        )
    )
    asyncio.run(
        SemanticFederationTool().run(
            {"term": "revenue", "definition": "my override", "layer": "member"},
            member_ctx,
        )
    )

    # Non-admin removes — must keep guild entry, delete own member entry
    asyncio.run(
        SemanticFederationTool().run({"term": "revenue", "remove": True}, member_ctx)
    )

    scope = "g1"
    assert member_ctx.store.kv_get(scope, _kv_key("revenue", "guild", "")) is not None
    assert member_ctx.store.kv_get(scope, _kv_key("revenue", "member", "u1")) is None


def test_parse_synonyms_strips_list_items() -> None:
    """_parse_synonyms must strip whitespace from list items (covers org_setup LLM list path)."""
    from lang2sql.tools.semantic_federation import _parse_synonyms

    result = _parse_synonyms([" active_user ", " 활성화 ", None, ""])
    assert result == ["active_user", "활성화"]


# -- effective_channel_id: channel-layer terms visible from thread context -----


def test_channel_layer_term_visible_from_thread_context() -> None:
    """Term defined at channel layer must be visible to users in threads of that channel."""
    from lang2sql.core.identity import Identity

    channel_ident = Identity(user_id="u1", guild_id="g1", channel_id="c1")
    thread_ident = Identity(
        user_id="u2", guild_id="g1", channel_id="c1", thread_id="t1"
    )

    # Both identities must resolve to the same channel entity
    assert (
        channel_ident.effective_channel_id == thread_ident.effective_channel_id == "c1"
    )

    store = SqliteStore()
    scope = "g1"
    store.kv_set(
        scope,
        _kv_key("active_user", "channel", "c1"),
        FedEntry(
            term="active_user", layer="channel", entity="c1", definition="30d login"
        ).to_json(),
    )

    # Term visible from channel context
    assert "active_user" in _render_effective(
        store, scope, channel_ident.effective_channel_id, "u1"
    )
    # Term also visible from thread context (inherits parent channel)
    assert "active_user" in _render_effective(
        store, scope, thread_ident.effective_channel_id, "u2"
    )
