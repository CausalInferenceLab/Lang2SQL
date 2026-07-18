"""OrgSetupTool — 팀/조직 등록 + 비즈니스 용어 자동 추출.

온보딩 2단계 (/setup으로 DB 연결 후 실행):
1. 접근 가능한 테이블 스캔 (팀의 DB 권한 = 팀이 보는 테이블)
2. LLM이 테이블 구조 + 샘플 데이터 분석 → 팀 도메인 + 핵심 용어 추론
3. 결과를 SemanticFederationTool과 동일한 KV 네임스페이스에 저장
   → build_prompt_section()이 자동으로 읽어 시스템 프롬프트에 주입

KV 저장:
  org:{org_lower}                              → {"name", "domain", "registered_at"}
  team:{team_lower}:{channel_id}               → {"name", "domain", "registered_at"}  (팀 등록 시)
  cterm:{term_lower}:guild                     → FedEntry JSON (org 전용, guild 레이어)
  cterm:{term_lower}:channel:{channel_id}      → FedEntry JSON (team 등록 시, channel 레이어)
"""

from __future__ import annotations

import json
import re
import time
from typing import TYPE_CHECKING, Any

from ..core.ports.tool import ToolPort
from ..core.types import Message, Role, ToolResult, ToolSpec
from .semantic_federation import (
    FedEntry,
    _KV_PREFIX as _SEMFED_PREFIX,
    _kv_key as _semfed_kv_key,
    _parse_synonyms,
)

if TYPE_CHECKING:
    from ..harness.context import HarnessContext

_SAMPLE_LIMIT = 10
_ORG_PREFIX = "org"
_TEAM_PREFIX = "team"


def _build_prompt(org_name: str, schema_block: str) -> str:
    return (
        f'이 DB에 접근 권한이 있는 팀/조직은 **"{org_name}"** 입니다.\n'
        "아래는 이 팀이 접근 가능한 테이블 스키마와 실제 데이터 샘플입니다.\n\n"
        f"{schema_block}\n\n"
        "위 데이터를 분석해서 다음을 추론해줘:\n"
        "1. 이 팀이 담당하는 업무 도메인 (한 줄)\n"
        "2. 이 팀이 자주 사용할 비즈니스 핵심 용어 (최대 10개)\n"
        "   - 각 용어의 DB 기반 정의 (어느 테이블/컬럼에 해당하는지 포함)\n"
        "   - 다른 팀에서 다르게 부를 수 있는 동의어/별칭\n\n"
        "아래 JSON 형식으로만 응답:\n"
        "{\n"
        '  "domain": "이 팀의 업무 도메인 한 줄 설명",\n'
        '  "terms": [\n'
        '    {"term": "용어명", "definition": "DB 기반 정의", "synonyms": ["동의어1", "동의어2"]}\n'
        "  ]\n"
        "}"
    )


def _extract_result(text: str) -> tuple[str, list[dict]]:
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m:
        return "", []
    try:
        data = json.loads(m.group(0))
        domain = data.get("domain", "") if isinstance(data, dict) else ""
        terms = data.get("terms", []) if isinstance(data, dict) else []
        if not isinstance(terms, list):
            terms = []
        return domain, terms
    except (ValueError, TypeError):
        return "", []


class OrgSetupTool(ToolPort):
    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="org_setup",
            description=(
                "조직(전사) 또는 팀(채널) 등록 및 DB 테이블 스캔으로 비즈니스 용어를 자동 추출한다. "
                "org만 지정 시 guild 레이어(전사 공통), team 지정 시 channel 레이어(팀 전용)에 저장. "
                "DB 연결(/setup) 후 실행."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "org": {
                        "type": "string",
                        "description": "전사 조직 이름 (예: ACME). 전사 공통 용어를 guild 레이어에 저장.",
                    },
                    "team": {
                        "type": "string",
                        "description": "팀 이름 (예: 마케팅팀). 현재 채널에 팀 전용 용어를 channel 레이어에 저장. org 없이 단독 사용 가능.",
                    },
                    "clear": {
                        "type": "boolean",
                        "description": "true이면 해당 org/team의 자동 추출 용어 초기화 (수동 등록 용어 보존)",
                    },
                },
            },
        )

    async def run(self, args: dict[str, Any], ctx: "HarnessContext") -> ToolResult:
        if ctx.store is None:
            return ToolResult(call_id="", content="❌ KV store 미설정", is_error=True)

        org_name = str(args.get("org", "")).strip()
        team_name = str(args.get("team", "")).strip()

        if not org_name and not team_name:
            return ToolResult(
                call_id="",
                content="❌ org 또는 team 파라미터가 필요합니다.",
                is_error=True,
            )

        scope = ctx.identity.kv_scope
        channel_id = ctx.identity.effective_channel_id

        # team이 있으면 channel 레이어, org만 있으면 guild 레이어
        use_team = bool(team_name)
        layer = "channel" if use_team else "guild"

        if layer == "guild" and not ctx.identity.is_admin:
            return ToolResult(
                call_id="",
                content="❌ 전사(guild) 용어 등록·초기화는 관리자만 가능합니다.",
                is_error=True,
            )

        if layer == "channel" and not channel_id:
            return ToolResult(
                call_id="",
                content="❌ 채널 컨텍스트 없이 팀(channel) 레이어에 등록할 수 없습니다.",
                is_error=True,
            )

        entity = channel_id if use_team else ""
        display_name = team_name if use_team else org_name
        meta_key = (
            f"{_TEAM_PREFIX}:{team_name.lower()}:{channel_id}"
            if use_team
            else f"{_ORG_PREFIX}:{org_name.lower()}"
        )

        if args.get("clear"):
            entries = ctx.store.kv_list_prefix(scope, f"{_SEMFED_PREFIX}:")
            deleted = 0
            for key, val in entries:
                try:
                    data = json.loads(val)
                except (ValueError, TypeError):
                    continue
                if not data.get("inferred"):
                    continue
                # 레이어와 entity가 일치하는 항목만 삭제
                entry_layer = data.get("layer", "")
                entry_entity = data.get("entity", "")
                if entry_layer == layer and entry_entity == entity:
                    ctx.store.kv_delete(scope, key)
                    deleted += 1
            ctx.store.kv_delete(scope, meta_key)
            layer_label = "팀(채널)" if use_team else "전사(guild)"
            return ToolResult(
                call_id="",
                content=f"🗑️ **{display_name}** [{layer_label}] 자동 추출 용어 {deleted}개 초기화 완료 (수동 등록 용어 보존)",
            )

        if ctx.explorer is None:
            return ToolResult(
                call_id="",
                content="❌ DB가 연결되지 않았습니다 (/setup 먼저).",
                is_error=True,
            )

        all_tables = await ctx.explorer.list_tables()
        if not all_tables:
            return ToolResult(
                call_id="", content="❌ 접근 가능한 테이블이 없습니다.", is_error=True
            )

        schema_lines: list[str] = []
        for tbl in all_tables:
            try:
                described = await ctx.explorer.describe_table(tbl.name)
            except Exception:
                continue
            schema_lines.append(f"테이블: {tbl.name}")
            for col in described.columns:
                try:
                    sample_sql = (
                        f"SELECT DISTINCT {col.name} FROM {tbl.qualified} "
                        f"WHERE {col.name} IS NOT NULL LIMIT {_SAMPLE_LIMIT}"
                    )
                    rows = await ctx.explorer.execute(sample_sql, _SAMPLE_LIMIT)
                    samples = [
                        str(r.get(col.name, r.get(list(r.keys())[0], ""))) for r in rows
                    ]
                except Exception:
                    samples = []
                sample_str = f" 샘플: {samples}" if samples else ""
                schema_lines.append(f"- {col.name} ({col.type}){sample_str}")
            schema_lines.append("")

        schema_block = "\n".join(schema_lines)
        prompt = _build_prompt(display_name, schema_block)

        completion = await ctx.llm.complete([Message(role=Role.USER, content=prompt)])
        domain, terms = _extract_result(completion.content)

        if not terms:
            return ToolResult(
                call_id="",
                content="LLM이 용어를 추출하지 못했습니다. 다시 시도해주세요.",
                is_error=True,
            )

        ctx.store.kv_set(
            scope,
            meta_key,
            json.dumps(
                {"name": display_name, "domain": domain, "registered_at": time.time()},
                ensure_ascii=False,
            ),
        )

        saved_terms: list[str] = []
        for t in terms:
            term = str(t.get("term", "")).strip()
            definition = str(t.get("definition", "")).strip()
            synonyms = _parse_synonyms(t.get("synonyms"))
            if not term or not definition:
                continue
            if ":" in term:
                continue  # `:` 포함 term은 KV 키 파싱을 깨트리므로 건너뜀
            entry = FedEntry(
                term=term,
                layer=layer,
                entity=entity,
                definition=definition,
                synonyms=synonyms,
                inferred=True,
            )
            kv_key = _semfed_kv_key(term, layer, entity)
            ctx.store.kv_set(scope, kv_key, entry.to_json())
            syn_str = f" (= {', '.join(synonyms)})" if synonyms else ""
            saved_terms.append(f"- **{term}**{syn_str}: {definition} 🤖")

        layer_label = "팀(채널)" if use_team else "전사(guild)"
        domain_line = f"📌 도메인: {domain}\n\n" if domain else ""
        term_block = "\n".join(saved_terms)
        return ToolResult(
            call_id="",
            content=(
                f"✅ **{display_name}** [{layer_label}] 등록 완료 — "
                f"테이블 {len(all_tables)}개 스캔, 용어 {len(saved_terms)}개 추출\n\n"
                f"{domain_line}"
                f"**추출된 용어:**\n{term_block}"
            ),
        )
