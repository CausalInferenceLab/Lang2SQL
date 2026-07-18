"""SemanticFederation — 채널(팀)/전사(guild)/개인(member) 계층 비즈니스 용어 사전.

계층 우선순위 (narrow → wide): member > channel > guild
- guild  : 전사 공통 정의 (회사 전체, /org_setup이 자동 채움)
- channel: 이 채널/팀 전용 정의 (다른 채널과 충돌 없음 — 채널이 격리 경계)
- member : 개인 오버라이드 (조용히 상위 정의를 덮어씀)

KV 키 구조 (모두 guild scope에 저장):
  cterm:{term_lower}:guild              → 전사 공통
  cterm:{term_lower}:channel:{ch_id}   → 채널(팀) 전용
  cterm:{term_lower}:member:{user_id}  → 개인
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ..core.ports.audit import AuditEvent

from ..core.ports.tool import ToolPort, ToolResult, ToolSpec

if TYPE_CHECKING:
    from ..harness.context import HarnessContext

_KV_PREFIX = "cterm"
_LAYERS = ("guild", "channel", "member")

from ..tools.enrich_schema import (
    _KV_PREFIX as _ENRICH_PREFIX,
    _KV_RELATIONSHIPS as _ENRICH_RELATIONSHIPS,
)

_AMBIGUITY_SIGNALS: dict[str, str] = {
    r"(^|_)(created|registered|joined|signup)(_at|_date)?$": "신규/최초 가입 기준 용어",
    r"(^|_)(last|latest|recent)_(login|visit|active|seen|access)(_at|_date)?$": "활성화 기준 용어",
    r"(^|_)(first|initial)_(order|purchase|buy)(_at|_date)?$": "첫 구매 기준 용어",
    r"(^|_)(status|state|type|category|tier|grade|rank|segment)$": "상태/분류 기반 용어",
    r"(^|_)(score|point|level|rating)$": "점수/등급 기반 용어",
    r"(^|_)(is_|has_|can_).+": "boolean 조건 기반 용어",
}


def _kv_key(term: str, layer: str, entity: str) -> str:
    base = f"{_KV_PREFIX}:{term.strip().lower()}:{layer}"
    if layer == "guild":
        return base
    return f"{base}:{entity.strip().lower()}"


def _parse_synonyms(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return [s.strip() for s in (str(x) for x in raw if x is not None) if s.strip()]
    return [s.strip() for s in str(raw or "").split(",") if s.strip()]


@dataclass
class FedEntry:
    term: str
    layer: str  # guild | channel | member
    entity: str  # channel_id (channel layer), user_id (member layer), "" (guild layer)
    definition: str
    synonyms: list[str] = field(default_factory=list)
    inferred: bool = False
    kind: str = ""  # metric | table | rule | dimension
    applies_to: str = ""  # 관련 테이블/컬럼 (예: users, orders.amount)
    tags: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not isinstance(self.synonyms, list):
            self.synonyms = _parse_synonyms(self.synonyms)
        if not isinstance(self.tags, list):
            self.tags = [t.strip() for t in str(self.tags).split(",") if t.strip()]

    def to_json(self) -> str:
        return json.dumps(
            {
                "term": self.term,
                "layer": self.layer,
                "entity": self.entity,
                "definition": self.definition,
                "synonyms": self.synonyms,
                "inferred": self.inferred,
                "kind": self.kind,
                "applies_to": self.applies_to,
                "tags": self.tags,
            },
            ensure_ascii=False,
        )

    @staticmethod
    def from_json(raw: str) -> "FedEntry":
        d = json.loads(raw)
        return FedEntry(
            term=d["term"],
            layer=d["layer"],
            entity=d.get("entity", ""),
            definition=d["definition"],
            synonyms=d.get("synonyms", []),
            inferred=d.get("inferred", False),
            kind=d.get("kind", ""),
            applies_to=d.get("applies_to", ""),
            tags=d.get("tags", []),
        )


class SemanticFederationTool(ToolPort):
    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="term_custom",
            description=(
                "비즈니스 용어 사전 관리. "
                "layer=guild(전사)/channel(이 채널·팀)/member(개인). "
                "lookup은 narrow→wide: member > channel > guild. "
                "list=true로 전체 조회. remove=true로 삭제."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "term": {
                        "type": "string",
                        "description": "정식 용어명 (예: 활성고객)",
                    },
                    "definition": {
                        "type": "string",
                        "description": "DB 컨텍스트에서의 정의 (예: 최근 30일 로그인한 users)",
                    },
                    "layer": {
                        "type": "string",
                        "enum": ["guild", "channel", "member"],
                        "description": "등록 범위. guild=전사 공통, channel=이 채널(팀), member=개인(기본값)",
                    },
                    "synonyms": {
                        "type": "string",
                        "description": "쉼표 구분 동의어 (예: active_user,활성화고객)",
                    },
                    "kind": {
                        "type": "string",
                        "enum": ["metric", "table", "rule", "dimension"],
                        "description": "용어 종류. metric=지표, table=테이블/엔티티, rule=비즈니스 규칙, dimension=분류 기준.",
                    },
                    "applies_to": {
                        "type": "string",
                        "description": "관련 테이블 또는 컬럼 (예: users, orders.amount).",
                    },
                    "tags": {
                        "type": "string",
                        "description": "쉼표 구분 태그 (예: growth,retention).",
                    },
                    "inferred": {
                        "type": "boolean",
                        "description": "true 시 LLM 추론 임시 정의로 표시. 사용자 확인 후 재등록 권장.",
                    },
                    "scan": {
                        "type": "boolean",
                        "description": "true 시 enriched schema에서 모호 용어 후보 탐색.",
                    },
                    "remove": {
                        "type": "boolean",
                        "description": "true 시 해당 term+layer 삭제",
                    },
                    "list": {
                        "type": "boolean",
                        "description": "true 시 현재 채널 기준 유효 용어 목록 반환",
                    },
                },
            },
        )

    async def run(self, args: dict[str, Any], ctx: "HarnessContext") -> ToolResult:
        if ctx.store is None:
            return ToolResult(call_id="", content="❌ KV store 미설정", is_error=True)

        scope = ctx.identity.kv_scope
        user_id = ctx.identity.user_id or "unknown"
        channel_id = ctx.identity.effective_channel_id

        if args.get("list"):
            return ToolResult(
                call_id="",
                content=_render_effective(ctx.store, scope, channel_id, user_id),
            )

        if args.get("scan"):
            return ToolResult(call_id="", content=_scan_schema(ctx.store, scope))

        term = str(args.get("term", "")).strip()
        if not term:
            return ToolResult(
                call_id="", content="❌ term 파라미터가 필요합니다.", is_error=True
            )
        if ":" in term:
            return ToolResult(
                call_id="", content="❌ term에 ':'를 사용할 수 없습니다.", is_error=True
            )

        if args.get("remove"):
            # 존재하는 항목 모두 삭제 — guild layer는 admin만 삭제 가능
            deleted_tags: list[str] = []
            for lyr, ent in [
                ("guild", ""),
                ("channel", channel_id),
                ("member", user_id),
            ]:
                if lyr == "guild" and not ctx.identity.is_admin:
                    continue
                k = _kv_key(term, lyr, ent)
                if ctx.store.kv_get(scope, k) is not None:
                    ctx.store.kv_delete(scope, k)
                    deleted_tags.append(_layer_tag(lyr, ent, user_id, channel_id))
            if not deleted_tags:
                if not ctx.identity.is_admin:
                    guild_k = _kv_key(term, "guild", "")
                    if ctx.store.kv_get(scope, guild_k) is not None:
                        return ToolResult(
                            call_id="",
                            content=f"⚠️ **{term}** — 전사(guild) 항목이 존재하지만 관리자만 삭제할 수 있습니다.",
                            is_error=True,
                        )
                return ToolResult(
                    call_id="", content=f"⚠️ **{term}** — 등록된 정의가 없습니다."
                )
            if ctx.audit is not None:
                await ctx.audit.record(
                    AuditEvent(
                        actor=user_id,
                        action="term_custom_remove",
                        scope=scope,
                        detail={"term": term, "layers": deleted_tags},
                    )
                )
            return ToolResult(
                call_id="", content=f"🗑️ **{term}** [{', '.join(deleted_tags)}] 삭제"
            )

        layer = str(args.get("layer", "member")).strip().lower()
        if layer not in _LAYERS:
            return ToolResult(
                call_id="",
                content=f"❌ layer는 {list(_LAYERS)} 중 하나여야 합니다.",
                is_error=True,
            )

        if layer == "guild" and not ctx.identity.is_admin:
            return ToolResult(
                call_id="",
                content="❌ guild 용어 등록·수정은 관리자만 가능합니다.",
                is_error=True,
            )

        if layer == "channel" and not channel_id:
            return ToolResult(
                call_id="",
                content="❌ 채널 컨텍스트 없이 channel 레이어에 등록할 수 없습니다.",
                is_error=True,
            )

        entity = (
            "" if layer == "guild" else (user_id if layer == "member" else channel_id)
        )
        key = _kv_key(term, layer, entity)

        definition = str(args.get("definition", "")).strip()
        if not definition:
            return ToolResult(
                call_id="",
                content="❌ definition 파라미터가 필요합니다.",
                is_error=True,
            )

        synonyms = _parse_synonyms(args.get("synonyms"))
        inferred = bool(args.get("inferred", False))
        kind = str(args.get("kind", "")).strip().lower()
        applies_to = str(args.get("applies_to", "")).strip()
        tags = [t.strip() for t in str(args.get("tags", "")).split(",") if t.strip()]

        entry = FedEntry(
            term=term,
            layer=layer,
            entity=entity,
            definition=definition,
            synonyms=synonyms,
            inferred=inferred,
            kind=kind,
            applies_to=applies_to,
            tags=tags,
        )
        ctx.store.kv_set(scope, key, entry.to_json())
        if ctx.audit is not None:
            await ctx.audit.record(
                AuditEvent(
                    actor=user_id,
                    action="term_custom",
                    scope=scope,
                    detail={"term": term, "layer": layer},
                )
            )

        tag = _layer_tag(layer, entity, user_id, channel_id)
        syn_str = f" (= {', '.join(synonyms)})" if synonyms else ""
        inferred_badge = " 🤖추론" if inferred else ""
        return ToolResult(
            call_id="",
            content=f"✅ **{term}** [{tag}]{syn_str}{inferred_badge}: {definition}",
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _layer_tag(layer: str, entity: str, user_id: str, channel_id: str) -> str:
    if layer == "guild":
        return "전사"
    if layer == "channel":
        return f"채널:{channel_id}"
    return f"개인:{user_id}"


# ---------------------------------------------------------------------------
# Schema scan
# ---------------------------------------------------------------------------


def _scan_schema(store: Any, scope: str) -> str:
    col_entries = store.kv_list_prefix(scope, _ENRICH_PREFIX + ":")
    if not col_entries:
        return "⚠️ enriched schema가 없습니다. 먼저 `/enrich`를 실행해 스키마를 보강하세요."

    col_map: dict[str, str] = {}
    for key, desc in col_entries:
        parts = key.split(":", 2)
        if len(parts) == 3:
            col_map[f"{parts[1]}.{parts[2]}"] = desc

    raw_rels = store.kv_get(scope, _ENRICH_RELATIONSHIPS)
    relationships: list[str] = json.loads(raw_rels) if raw_rels else []

    candidates: dict[str, list[tuple[str, str, str]]] = {}
    for col_key, desc in col_map.items():
        table, col = col_key.split(".", 1)
        for pattern, signal_type in _AMBIGUITY_SIGNALS.items():
            if re.search(pattern, col, re.IGNORECASE):
                candidates.setdefault(signal_type, []).append((table, col, desc))
                break

    if not candidates:
        return f"스키마에서 모호 용어를 암시하는 컬럼을 찾지 못했습니다. (스캔한 컬럼 수: {len(col_map)}개)"

    lines = [
        "## Business Terminology — 스키마 스캔 결과\n",
        "다음 컬럼들이 모호한 비즈니스 용어를 암시합니다.",
        "각 항목에 대해 term_custom 등록 여부를 사용자에게 확인하세요.\n",
    ]
    for signal_type, cols in candidates.items():
        lines.append(f"### {signal_type}")
        for table, col, desc in cols:
            desc_str = f" — {desc}" if desc else ""
            lines.append(f"- `{table}.{col}`{desc_str}")
        lines.append("")

    if relationships:
        lines.append("### 테이블 관계 (참고)")
        for rel in relationships:
            lines.append(f"- {rel}")
        lines.append("")

    lines.append(
        "---\n위 컬럼을 바탕으로 모호 용어 정의를 추론하고 `term_custom` 툴로 `inferred=true` 등록하거나, "
        "사용자에게 어느 범위(guild/channel/member)로 등록할지 확인하세요."
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# System-prompt helpers
# ---------------------------------------------------------------------------


def _load_all(store: Any, scope: str) -> dict[str, list[FedEntry]]:
    """KV에서 모든 cterm 엔트리를 {term_lower: [FedEntry]} 로 반환."""
    raw = store.kv_list_prefix(scope, _KV_PREFIX + ":")
    by_term: dict[str, list[FedEntry]] = {}
    for key, val in raw:
        # cterm:{term}:guild  or  cterm:{term}:channel:{id}  or  cterm:{term}:member:{id}
        parts = key.split(":", 3)
        if len(parts) < 3:
            continue
        layer = parts[2]
        if layer not in _LAYERS:
            continue
        try:
            entry = FedEntry.from_json(val)
        except (ValueError, KeyError):
            continue
        by_term.setdefault(parts[1], []).append(entry)
    return by_term


def build_prompt_section(store: Any, scope: str, channel_id: str, user_id: str) -> str:
    """현재 채널 기준 narrow→wide lookup 용어 섹션 + 모호 용어 지침 반환."""
    by_term = _load_all(store, scope)

    if not by_term:
        return _AMBIGUOUS_TERM_POLICY

    lines: list[str] = []
    for term_lower in sorted(by_term):
        line = _resolve_term(by_term[term_lower], channel_id, user_id)
        if line:
            lines.append(line)

    header = "## Business Terminology\n" "(lookup 우선순위: 개인 > 채널(팀) > 전사)\n"
    body = "\n".join(lines) if lines else "(없음)"
    return header + body + "\n\n" + _AMBIGUOUS_TERM_POLICY


_AMBIGUOUS_TERM_POLICY = """\
## Ambiguous Term Policy
사전에 없는 주관적/모호한 표현(예: 활성화고객, 신규고객, 우량고객)을 발견하면:
1. 현재 DB 스키마 컨텍스트에서 가장 합리적인 해석으로 SQL을 작성하고 실행한다.
2. 쿼리 후 사용한 해석을 명시하고, term_custom 등록 여부와 범위(guild/channel/member)를 사용자에게 묻는다.
   예: "'신규고객'을 'users.created_at >= NOW()-30일'로 해석했습니다. 이 정의를 어느 범위로 등록할까요?"
3. 사용자가 범위를 지정하면 term_custom 툴로 즉시 등록한다 (inferred=true).
4. inferred=true 엔트리가 이미 있으면 해당 정의를 우선 사용하되, 사용자에게 확정 여부를 확인한다.\
"""


def _fmt_entry(e: FedEntry, tag: str) -> str:
    syns = ", ".join(e.synonyms)
    syn_str = f" (= {syns})" if syns else ""
    inferred_badge = " 🤖" if e.inferred else ""
    kind_badge = f" `{e.kind}`" if e.kind else ""
    return (
        f"- **{e.term}**{kind_badge} [{tag}]{syn_str}{inferred_badge}: {e.definition}"
    )


def _resolve_term(entries: list[FedEntry], channel_id: str, user_id: str) -> str:
    """narrow→wide lookup: member > channel > guild."""
    # 1. 개인 오버라이드
    for e in entries:
        if e.layer == "member" and e.entity == user_id:
            return _fmt_entry(e, f"개인:{user_id}")

    # 2. 이 채널 정의
    for e in entries:
        if e.layer == "channel" and e.entity == channel_id:
            return _fmt_entry(e, "채널")

    # 3. 전사 공통
    for e in entries:
        if e.layer == "guild":
            return _fmt_entry(e, "전사")

    return ""


def _render_effective(store: Any, scope: str, channel_id: str, user_id: str) -> str:
    """Discord /term_custom list 응답 — 현재 채널 기준 유효 용어 목록."""
    by_term = _load_all(store, scope)
    if not by_term:
        return "등록된 용어가 없습니다.\n`/term_custom`으로 용어를 추가하세요."

    lines = ["**Business Terminology — 현재 채널 기준 유효 정의**\n"]
    for term_lower in sorted(by_term):
        line = _resolve_term(by_term[term_lower], channel_id, user_id)
        if line:
            lines.append(line)

    if len(lines) == 1:
        lines.append("(이 채널에 적용되는 용어 정의가 없습니다)")
    return "\n".join(lines)
