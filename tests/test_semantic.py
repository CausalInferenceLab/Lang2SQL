"""Tests for KV-backed semantic federation (★④).

Verifies that the KV store correctly implements narrow→wide scope resolution,
matching the original ScopeResolver contract.
"""

from __future__ import annotations

from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.tools.semantic_federation import (
    FedEntry,
    _kv_key,
    _render_effective,
    build_prompt_section,
)


def _store_with_entries(entries: list[tuple[str, str, str, str]]) -> SqliteStore:
    """Helper: create an in-memory store and populate KV with FedEntry items.

    Each tuple is (scope, term, layer, entity, definition) — scope is the guild id.
    """
    store = SqliteStore()
    for scope, term, layer, entity, definition in entries:  # type: ignore[misc]
        entry = FedEntry(term=term, layer=layer, entity=entity, definition=definition)
        store.kv_set(scope, _kv_key(term, layer, entity), entry.to_json())
    return store


def test_channel_overrides_guild() -> None:
    store = SqliteStore()
    scope = "g1"
    store.kv_set(scope, _kv_key("active_user", "guild", ""), FedEntry("active_user", "guild", "", "30d login").to_json())
    store.kv_set(scope, _kv_key("active_user", "channel", "c1"), FedEntry("active_user", "channel", "c1", "7d core action").to_json())

    rendered = _render_effective(store, scope, "c1", "u1")
    assert "7d core action" in rendered
    assert "30d login" not in rendered


def test_guild_fills_gap_when_channel_missing() -> None:
    store = SqliteStore()
    scope = "g1"
    store.kv_set(scope, _kv_key("revenue", "guild", ""), FedEntry("revenue", "guild", "", "net revenue").to_json())

    rendered = _render_effective(store, scope, "c1", "u1")
    assert "net revenue" in rendered


def test_member_overrides_channel_and_guild() -> None:
    store = SqliteStore()
    scope = "g1"
    store.kv_set(scope, _kv_key("active_user", "guild", ""), FedEntry("active_user", "guild", "", "guild def").to_json())
    store.kv_set(scope, _kv_key("active_user", "channel", "c1"), FedEntry("active_user", "channel", "c1", "channel def").to_json())
    store.kv_set(scope, _kv_key("active_user", "member", "u1"), FedEntry("active_user", "member", "u1", "member def").to_json())

    rendered = _render_effective(store, scope, "c1", "u1")
    assert "member def" in rendered
    assert "channel def" not in rendered
    assert "guild def" not in rendered


def test_two_channels_isolated() -> None:
    store = SqliteStore()
    scope = "g1"
    store.kv_set(scope, _kv_key("active_user", "channel", "mkt"), FedEntry("active_user", "channel", "mkt", "30d login").to_json())
    store.kv_set(scope, _kv_key("active_user", "channel", "fin"), FedEntry("active_user", "channel", "fin", "paid subscriber").to_json())

    mkt = _render_effective(store, scope, "mkt", "u1")
    fin = _render_effective(store, scope, "fin", "u2")
    assert "30d login" in mkt
    assert "paid subscriber" not in mkt
    assert "paid subscriber" in fin
    assert "30d login" not in fin


def test_empty_store_returns_no_terms() -> None:
    store = SqliteStore()
    rendered = _render_effective(store, "g1", "c1", "u1")
    assert "등록된 용어가 없습니다" in rendered


def test_build_prompt_section_includes_ambiguous_term_policy() -> None:
    store = SqliteStore()
    section = build_prompt_section(store, "g1", "c1", "u1")
    assert "Ambiguous Term Policy" in section


def test_fed_entry_kind_applies_to_tags_roundtrip() -> None:
    entry = FedEntry(
        term="활성고객", layer="guild", entity="",
        definition="30일 내 로그인한 users",
        kind="metric", applies_to="users", tags=["growth", "retention"],
    )
    restored = FedEntry.from_json(entry.to_json())
    assert restored.kind == "metric"
    assert restored.applies_to == "users"
    assert restored.tags == ["growth", "retention"]


def test_fed_entry_backward_compat_missing_new_fields() -> None:
    # kind/applies_to/tags 없는 기존 JSON도 파싱 가능해야 함
    import json
    old_json = json.dumps({
        "term": "revenue", "layer": "guild", "entity": "",
        "definition": "net revenue", "synonyms": [], "inferred": False,
    })
    entry = FedEntry.from_json(old_json)
    assert entry.kind == ""
    assert entry.applies_to == ""
    assert entry.tags == []


def test_fmt_entry_shows_kind_badge() -> None:
    from lang2sql.tools.semantic_federation import _fmt_entry
    entry = FedEntry(
        term="활성고객", layer="guild", entity="",
        definition="30일 내 로그인", kind="metric",
    )
    rendered = _fmt_entry(entry, "전사")
    assert "`metric`" in rendered
