"""OrgSetupTool — 팀/조직 등록 + 비즈니스 용어 자동 추출.

온보딩 2단계 (/setup으로 DB 연결 후 실행):
1. 접근 가능한 테이블 스캔 (팀의 DB 권한 = 팀이 보는 테이블)
2. LLM이 테이블 구조 + 샘플 데이터 분석 → 팀 도메인 + 핵심 용어 추론
3. 결과를 SemanticFederationTool과 동일한 KV 네임스페이스에 저장
   → build_prompt_section()이 자동으로 읽어 시스템 프롬프트에 주입

KV 저장:
  org:{org_lower}                        → {"name", "domain", "registered_at"}
  cterm:{term_lower}:team:{org_lower}   → FedEntry JSON (SemanticFederationTool 호환)
"""

from __future__ import annotations

import json
import re
import time
from typing import TYPE_CHECKING, Any

from ..core.ports.tool import ToolPort
from ..core.types import Message, Role, ToolResult, ToolSpec
from .semantic_federation import FedEntry

if TYPE_CHECKING:
    from ..harness.context import HarnessContext

_SAMPLE_LIMIT = 10
_ORG_PREFIX = "org"
_SEMFED_PREFIX = "cterm"  # SemanticFederationTool과 동일한 KV 네임스페이스


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
                "조직/팀 등록 및 접근 가능한 DB 테이블을 스캔해 팀별 비즈니스 용어를 자동 추출한다. "
                "DB 연결(/setup) 후 각 팀이 한 번 실행."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "org": {
                        "type": "string",
                        "description": "팀/조직 이름 (예: 재무팀, 마케팅팀)",
                    },
                    "clear": {
                        "type": "boolean",
                        "description": "true이면 해당 org의 용어 캐시 초기화",
                    },
                },
                "required": ["org"],
            },
        )

    async def run(self, args: dict[str, Any], ctx: "HarnessContext") -> ToolResult:
        if ctx.store is None:
            return ToolResult(call_id="", content="❌ KV store 미설정", is_error=True)

        org_name = str(args.get("org", "")).strip()
        if not org_name:
            return ToolResult(call_id="", content="❌ org 파라미터가 필요합니다.", is_error=True)

        scope = ctx.identity.guild_id or f"dm:{ctx.identity.user_id}"
        org_lower = org_name.lower()

        if args.get("clear"):
            # inferred=True인 guild 레이어 항목만 삭제 (사용자 직접 등록 용어는 보존)
            entries = ctx.store.kv_list_prefix(scope, f"{_SEMFED_PREFIX}:")
            deleted = 0
            for key, val in entries:
                if not key.endswith(":guild"):
                    continue
                try:
                    data = json.loads(val)
                except (ValueError, TypeError):
                    continue
                if data.get("inferred"):
                    ctx.store.kv_delete(scope, key)
                    deleted += 1
            ctx.store.kv_delete(scope, f"{_ORG_PREFIX}:{org_lower}")
            return ToolResult(call_id="", content=f"🗑️ **{org_name}** 자동 추출 용어 {deleted}개 초기화 완료 (수동 등록 용어 보존)")

        if ctx.explorer is None:
            return ToolResult(call_id="", content="❌ DB가 연결되지 않았습니다 (/setup 먼저).", is_error=True)

        # 접근 가능한 테이블 스캔 (팀의 DB 권한 = 팀이 보는 테이블)
        all_tables = await ctx.explorer.list_tables()
        if not all_tables:
            return ToolResult(call_id="", content="❌ 접근 가능한 테이블이 없습니다.", is_error=True)

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
                    samples = [str(r.get(col.name, r.get(list(r.keys())[0], ""))) for r in rows]
                except Exception:
                    samples = []
                sample_str = f" 샘플: {samples}" if samples else ""
                schema_lines.append(f"- {col.name} ({col.type}){sample_str}")
            schema_lines.append("")

        schema_block = "\n".join(schema_lines)
        prompt = _build_prompt(org_name, schema_block)

        completion = await ctx.llm.complete([Message(role=Role.USER, content=prompt)])
        domain, terms = _extract_result(completion.content)

        if not terms:
            return ToolResult(
                call_id="",
                content="LLM이 용어를 추출하지 못했습니다. 다시 시도해주세요.",
                is_error=True,
            )

        # org 등록
        ctx.store.kv_set(
            scope,
            f"{_ORG_PREFIX}:{org_lower}",
            json.dumps({"name": org_name, "domain": domain, "registered_at": time.time()}, ensure_ascii=False),
        )

        # cterm:{term}:guild 형식으로 저장 → 전사 공통 레이어 (narrow→wide 최하단)
        saved_terms: list[str] = []
        for t in terms:
            term = str(t.get("term", "")).strip()
            definition = str(t.get("definition", "")).strip()
            synonyms = t.get("synonyms", [])
            if not term or not definition:
                continue
            entry = FedEntry(
                term=term, layer="guild", entity="",
                definition=definition, synonyms=synonyms, inferred=True,
            )
            ctx.store.kv_set(
                scope,
                f"{_SEMFED_PREFIX}:{term.lower()}:guild",
                entry.to_json(),
            )
            syn_str = f" (= {', '.join(synonyms)})" if synonyms else ""
            saved_terms.append(f"- **{term}**{syn_str}: {definition} 🤖")

        domain_line = f"📌 도메인: {domain}\n\n" if domain else ""
        term_block = "\n".join(saved_terms)
        return ToolResult(
            call_id="",
            content=(
                f"✅ **{org_name}** 조직 등록 완료 — "
                f"테이블 {len(all_tables)}개 스캔, 용어 {len(saved_terms)}개 추출\n\n"
                f"{domain_line}"
                f"**추출된 용어:**\n{term_block}"
            ),
        )
