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
    assert "7d core action" in rendered          # channel wins (effective)
    # guild base shown for transparency (override does NOT hide the guild def)
    assert "전사 기본: 30d login" in rendered


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
    assert "member def" in rendered          # member wins (effective)
    assert "channel def" not in rendered      # channel is overridden by member
    # guild base is shown for transparency (override does NOT hide the guild def)
    assert "전사 기본: guild def" in rendered


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


def test_channel_override_keeps_guild_visible() -> None:
    """채널 override가 전사 정의를 데이터·표시 양쪽에서 가리지 않는다 (federation 회귀)."""
    from lang2sql.tools.semantic_federation import _render_layers, _kv_key as _k
    store = SqliteStore()
    scope = "g1"
    store.kv_set(scope, _k("invoice", "guild", ""),
                 FedEntry("Invoice", "guild", "", "판매 거래 식별자").to_json())
    store.kv_set(scope, _k("invoice", "channel", "mkt"),
                 FedEntry("Invoice", "channel", "mkt", "영수증").to_json())

    # 마케팅 채널: override가 이기되 전사 기본이 함께 보인다
    eff_mkt = _render_effective(store, scope, "mkt", "u1")
    assert "영수증" in eff_mkt and "전사 기본: 판매 거래 식별자" in eff_mkt

    # 다른 채널: override 없으니 전사 정의 그대로
    eff_other = _render_effective(store, scope, "fin", "u1")
    assert "판매 거래 식별자" in eff_other and "영수증" not in eff_other

    # 레이어별 보기: 전사 Invoice와 채널 Invoice가 둘 다 노출
    layers = _render_layers(store, scope, "mkt", "u1")
    assert "판매 거래 식별자" in layers and "영수증" in layers and "재정의됨" in layers
