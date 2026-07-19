"""OkfBundle — OKF(Open Knowledge Format) 기반 지식 번들 어댑터.

KV 캐시(SqliteStore)와 양방향 sync:
- export: KV → 스코프별 .md 파일 (Git 영속, 사람이 읽을 수 있는 형태)
- import_: .md 파일 → KV (번들에서 런타임 캐시 복원)

디렉토리 구조 (OKF SPEC §3):
    <base_dir>/
    ├── guild/
    │   ├── index.md
    │   ├── metrics/active_user.md
    │   ├── tables/orders.md
    │   ├── rules/exclude_cancelled.md
    │   ├── dimensions/customer_tier.md
    │   └── misc/<term>.md       # kind 미지정
    └── channel:<ch_id>/
        ├── index.md
        └── metrics/active_user.md

각 .md 파일 형식 (OKF SPEC §4):
    ---
    type: Metric
    title: active_user
    description: "30일 내 로그인한 users"
    tags: [growth, retention]
    applies_to: users
    synonyms: [활성화고객]
    layer: guild
    entity: ""
    inferred: false
    timestamp: 2026-07-18T...
    ---

    (markdown body — definition 반복 또는 추가 설명)
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from ...tools.semantic_federation import (
    FedEntry,
    _KV_PREFIX,
    _kv_key,
)

if TYPE_CHECKING:
    from .sqlite_store import SqliteStore

_KIND_FOLDER: dict[str, str] = {
    "metric": "metrics",
    "table": "tables",
    "rule": "rules",
    "dimension": "dimensions",
}
_RESERVED = {"index.md", "log.md"}


class OkfBundle:
    """KV ↔ OKF .md 파일 양방향 sync 어댑터."""

    def __init__(self, base_dir: str) -> None:
        self.base_dir = Path(base_dir)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def export(self, store: "SqliteStore", kv_scope: str) -> int:
        """KV에서 모든 FedEntry를 읽어 .md 파일로 저장. 저장된 파일 수 반환."""
        raw = store.kv_list_prefix(kv_scope, _KV_PREFIX + ":")
        count = 0
        for _key, val in raw:
            try:
                entry = FedEntry.from_json(val)
            except (ValueError, KeyError):
                continue
            path = self._concept_path(entry)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(_entry_to_md(entry), encoding="utf-8")
            count += 1
        return count

    def import_(self, store: "SqliteStore", kv_scope: str) -> int:
        """번들의 .md 파일을 읽어 KV로 복원. 로드된 항목 수 반환."""
        count = 0
        for md_file in self.base_dir.rglob("*.md"):
            if md_file.name in _RESERVED:
                continue
            entry = _md_to_entry(md_file)
            if entry is None:
                continue
            key = _kv_key(entry.term, entry.layer, entry.entity)
            store.kv_set(kv_scope, key, entry.to_json())
            count += 1
        return count

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _scope_dir(self, entry: FedEntry) -> Path:
        label = "guild" if entry.layer == "guild" else f"{entry.layer}:{entry.entity}"
        return self.base_dir / label

    def _concept_path(self, entry: FedEntry) -> Path:
        folder = _KIND_FOLDER.get(entry.kind, "misc")
        slug = re.sub(r'[/\\]', '-', entry.term.strip()).replace(" ", "_").replace(":", "-")
        path = self._scope_dir(entry) / folder / f"{slug}.md"
        if not path.resolve().is_relative_to(self.base_dir.resolve()):
            raise ValueError(f"unsafe path derived from term: {entry.term!r}")
        return path


# ------------------------------------------------------------------
# Serialization helpers (module-level for testability)
# ------------------------------------------------------------------


def _entry_to_md(entry: FedEntry) -> str:
    """FedEntry → OKF .md 문자열 (SPEC §4.1)."""
    fm: dict = {
        "type": entry.kind.capitalize() if entry.kind else "Concept",
        "title": entry.term,
        "description": entry.definition,
    }
    if entry.tags:
        fm["tags"] = entry.tags
    if entry.applies_to:
        fm["applies_to"] = entry.applies_to
    if entry.synonyms:
        fm["synonyms"] = entry.synonyms
    fm["layer"] = entry.layer
    fm["entity"] = entry.entity
    fm["inferred"] = entry.inferred
    fm["timestamp"] = datetime.now(timezone.utc).isoformat()

    yaml_block = yaml.dump(
        fm, allow_unicode=True, default_flow_style=False, sort_keys=False
    )
    return f"---\n{yaml_block}---\n\n{entry.definition}\n"


def _md_to_entry(path: Path) -> FedEntry | None:
    """OKF .md 파일 → FedEntry. 파싱 실패 시 None 반환."""
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        return None
    rest = text[4:]  # skip opening "---\n"
    parts = rest.split("\n---\n", 1)
    if len(parts) < 2:
        return None
    try:
        fm = yaml.safe_load(parts[0])
    except yaml.YAMLError:
        return None
    if not isinstance(fm, dict):
        return None

    raw_kind = str(fm.get("type", "")).lower()
    kind = raw_kind if raw_kind in _KIND_FOLDER else ""

    return FedEntry(
        term=str(fm.get("title", path.stem)),
        layer=str(fm.get("layer", "guild")),
        entity=str(fm.get("entity", "")),
        definition=str(fm.get("description", "")),
        synonyms=fm.get("synonyms") or [],
        inferred=bool(fm.get("inferred", False)),
        kind=kind,
        applies_to=str(fm.get("applies_to", "")),
        tags=fm.get("tags") or [],
    )
