"""OkfBundle — export/import round-trip 및 파일 구조 테스트."""

from __future__ import annotations

import tempfile
from pathlib import Path

from lang2sql.adapters.storage.okf_bundle import OkfBundle, _entry_to_md, _md_to_entry
from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.tools.semantic_federation import FedEntry, _kv_key


def _populate(store: SqliteStore, scope: str, entries: list[FedEntry]) -> None:
    for e in entries:
        store.kv_set(scope, _kv_key(e.term, e.layer, e.entity), e.to_json())


# ------------------------------------------------------------------
# 직렬화 단위 테스트
# ------------------------------------------------------------------


def test_entry_to_md_contains_required_okf_fields() -> None:
    entry = FedEntry(
        term="활성고객",
        layer="guild",
        entity="",
        definition="30일 내 로그인한 users",
        kind="metric",
        applies_to="users",
        tags=["growth"],
    )
    md = _entry_to_md(entry)
    assert "type: Metric" in md
    assert "title: 활성고객" in md
    assert "description:" in md
    assert "layer: guild" in md


def test_md_to_entry_roundtrip() -> None:
    entry = FedEntry(
        term="순매출",
        layer="channel",
        entity="mkt-123",
        definition="환불 제외 매출",
        synonyms=["net revenue"],
        kind="metric",
        applies_to="orders",
        tags=["finance"],
        inferred=True,
    )
    with tempfile.NamedTemporaryFile(
        suffix=".md", mode="w", delete=False, encoding="utf-8"
    ) as f:
        f.write(_entry_to_md(entry))
        tmp = Path(f.name)

    restored = _md_to_entry(tmp)
    assert restored is not None
    assert restored.term == "순매출"
    assert restored.kind == "metric"
    assert restored.layer == "channel"
    assert restored.entity == "mkt-123"
    assert restored.applies_to == "orders"
    assert restored.tags == ["finance"]
    assert restored.inferred is True
    tmp.unlink()


def test_md_to_entry_unknown_type_becomes_empty_kind() -> None:
    md = "---\ntype: Playbook\ntitle: foo\ndescription: bar\nlayer: guild\nentity: ''\ninferred: false\n---\n\nbar\n"
    with tempfile.NamedTemporaryFile(
        suffix=".md", mode="w", delete=False, encoding="utf-8"
    ) as f:
        f.write(md)
        tmp = Path(f.name)
    entry = _md_to_entry(tmp)
    assert entry is not None
    assert entry.kind == ""
    tmp.unlink()


def test_md_to_entry_no_frontmatter_returns_none() -> None:
    with tempfile.NamedTemporaryFile(
        suffix=".md", mode="w", delete=False, encoding="utf-8"
    ) as f:
        f.write("no frontmatter here")
        tmp = Path(f.name)
    assert _md_to_entry(tmp) is None
    tmp.unlink()


# ------------------------------------------------------------------
# export / import 통합 테스트
# ------------------------------------------------------------------


def test_export_creates_kind_based_folders() -> None:
    store = SqliteStore()
    scope = "g1"
    entries = [
        FedEntry("활성고객", "guild", "", "30일 로그인", kind="metric"),
        FedEntry("orders", "guild", "", "주문 테이블", kind="table"),
        FedEntry("환불제외", "guild", "", "status != refunded", kind="rule"),
        FedEntry("고객등급", "guild", "", "users.tier", kind="dimension"),
        FedEntry("기타용어", "guild", "", "정의 없음", kind=""),
    ]
    _populate(store, scope, entries)

    with tempfile.TemporaryDirectory() as tmp:
        bundle = OkfBundle(tmp)
        count = bundle.export(store, scope)

        assert count == 5
        assert (Path(tmp) / "guild" / "metrics" / "활성고객.md").exists()
        assert (Path(tmp) / "guild" / "tables" / "orders.md").exists()
        assert (Path(tmp) / "guild" / "rules" / "환불제외.md").exists()
        assert (Path(tmp) / "guild" / "dimensions" / "고객등급.md").exists()
        assert (Path(tmp) / "guild" / "misc" / "기타용어.md").exists()


def test_export_separates_scopes() -> None:
    store = SqliteStore()
    scope = "g1"
    _populate(
        store,
        scope,
        [
            FedEntry("활성고객", "guild", "", "30일 로그인", kind="metric"),
            FedEntry("활성고객", "channel", "mkt", "7일 구매", kind="metric"),
        ],
    )

    with tempfile.TemporaryDirectory() as tmp:
        bundle = OkfBundle(tmp)
        bundle.export(store, scope)

        assert (Path(tmp) / "guild" / "metrics" / "활성고객.md").exists()
        assert (Path(tmp) / "channel:mkt" / "metrics" / "활성고객.md").exists()


def test_import_restores_kv_from_files() -> None:
    store = SqliteStore()
    scope = "g1"
    original = FedEntry(
        "순매출",
        "guild",
        "",
        "환불 제외 매출",
        kind="metric",
        applies_to="orders",
        tags=["finance"],
    )
    _populate(store, scope, [original])

    with tempfile.TemporaryDirectory() as tmp:
        bundle = OkfBundle(tmp)
        bundle.export(store, scope)

        # KV 비우고 import
        empty_store = SqliteStore()
        count = bundle.import_(empty_store, scope)

        assert count == 1
        key = _kv_key("순매출", "guild", "")
        raw = empty_store.kv_get(scope, key)
        assert raw is not None
        restored = FedEntry.from_json(raw)
        assert restored.term == "순매출"
        assert restored.kind == "metric"
        assert restored.applies_to == "orders"


def test_import_skips_reserved_files() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        guild_dir = Path(tmp) / "guild"
        guild_dir.mkdir()
        (guild_dir / "index.md").write_text("# index", encoding="utf-8")
        (guild_dir / "log.md").write_text("# log", encoding="utf-8")

        store = SqliteStore()
        bundle = OkfBundle(tmp)
        count = bundle.import_(store, "g1")
        assert count == 0


def test_full_roundtrip_preserves_all_fields() -> None:
    store = SqliteStore()
    scope = "g1"
    original = FedEntry(
        term="월매출",
        layer="member",
        entity="user-99",
        definition="당월 발생 매출 합계",
        synonyms=["monthly revenue"],
        inferred=False,
        kind="metric",
        applies_to="orders.amount",
        tags=["finance", "monthly"],
    )
    _populate(store, scope, [original])

    with tempfile.TemporaryDirectory() as tmp:
        bundle = OkfBundle(tmp)
        bundle.export(store, scope)
        restored_store = SqliteStore()
        bundle.import_(restored_store, scope)

    key = _kv_key("월매출", "member", "user-99")
    raw = restored_store.kv_get(scope, key)
    assert raw is not None
    restored = FedEntry.from_json(raw)

    assert restored.term == "월매출"
    assert restored.layer == "member"
    assert restored.entity == "user-99"
    assert restored.kind == "metric"
    assert restored.applies_to == "orders.amount"
    assert set(restored.tags) == {"finance", "monthly"}
    assert restored.synonyms == ["monthly revenue"]
