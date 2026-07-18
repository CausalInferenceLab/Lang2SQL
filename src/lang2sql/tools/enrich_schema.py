"""enrich_schema — LLM-powered column metadata enrichment.

Samples DISTINCT values from each column, sends the full schema + samples to
the LLM in a single call, and stores the inferred descriptions in the KV store.
Subsequent explore_schema calls read from this cache (highest priority).

KV key pattern: enriched_desc:{table}:{column}
"""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any

from ..core.types import Message, Role, ToolResult, ToolSpec

if TYPE_CHECKING:
    from ..harness.context import HarnessContext

_SAMPLE_LIMIT = 10
_KV_PREFIX = "enriched_desc"
_KV_RELATIONSHIPS = "schema_relationships"


def _kv_key(table: str, column: str) -> str:
    return f"{_KV_PREFIX}:{table}:{column}"


def _build_prompt(schema_block: str) -> str:
    return (
        "다음은 DB 테이블들의 스키마와 실제 샘플 데이터야.\n"
        "각 컬럼의 의미와 테이블 간 JOIN 관계를 추론해줘.\n\n"
        f"{schema_block}\n\n"
        "아래 JSON 형식으로만 응답해:\n"
        "{\n"
        '  "columns": {"테이블명.컬럼명": "컬럼 설명 (추론 불확실하면 빈 문자열)"},\n'
        '  "relationships": ["tableA.col = tableB.col", ...]\n'
        "}\n\n"
        "설명 작성 규칙:\n"
        "- 코드값 컬럼(짧은 문자열 샘플): 샘플에서 추론한 각 값의 의미 명시\n"
        "- 계산에 쓰이는 컬럼 쌍: 실제 계산 공식을 설명에 포함\n"
        "- relationships: 샘플값이 겹치거나 의미상 같은 컬럼 쌍을 'A.x = B.y' 형식으로 나열\n"
        "  (FK 선언이 없어도 값이 같으면 포함)"
    )


def _extract_result(text: str) -> tuple[dict[str, str], list[str]]:
    """Extract columns dict and relationships list from LLM response."""
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m:
        return {}, []
    try:
        data = json.loads(m.group(0))
        columns = data.get("columns", {}) if isinstance(data, dict) else {}
        relationships = data.get("relationships", []) if isinstance(data, dict) else []
        return columns, relationships
    except (ValueError, TypeError):
        return {}, []


class EnrichSchema:
    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="enrich_schema",
            description=(
                "DB 컬럼 메타데이터를 실제 샘플 데이터 기반으로 LLM이 자동 보강한다. "
                "테이블 간 FK 관계도 추론한다. /enrich 명령으로 호출."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "table": {
                        "type": "string",
                        "description": "보강할 테이블명 (생략 시 전체 테이블)",
                    },
                    "clear": {
                        "type": "boolean",
                        "description": "true이면 보강 캐시를 초기화",
                    },
                },
            },
        )

    async def run(self, args: dict[str, Any], ctx: "HarnessContext") -> ToolResult:
        if ctx.explorer is None:
            return ToolResult(
                call_id="",
                content="DB가 연결되지 않았습니다 (/connect 먼저).",
                is_error=True,
            )
        if ctx.store is None:
            return ToolResult(
                call_id="", content="KV store를 사용할 수 없습니다.", is_error=True
            )

        scope = ctx.identity.kv_scope

        if args.get("clear"):
            count = ctx.store.kv_delete_prefix(scope, _KV_PREFIX + ":")
            ctx.store.kv_delete(scope, _KV_RELATIONSHIPS)
            return ToolResult(
                call_id="", content=f"🗑️ 보강 캐시 초기화 완료 ({count}개 삭제)"
            )

        target = (args.get("table") or "").strip()
        all_tables = await ctx.explorer.list_tables()
        if target:
            tables = [
                t for t in all_tables if t.name == target or t.qualified == target
            ]
            if not tables:
                return ToolResult(
                    call_id="",
                    content=f"테이블 '{target}'을 찾을 수 없습니다.",
                    is_error=True,
                )
        else:
            tables = all_tables

        # Build schema block with sample values for each column.
        schema_lines: list[str] = []
        for tbl in tables:
            described = await ctx.explorer.describe_table(tbl.name)
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
        prompt = _build_prompt(schema_block)

        # Single LLM call for all tables at once.
        completion = await ctx.llm.complete([Message(role=Role.USER, content=prompt)])
        columns, relationships = _extract_result(completion.content)

        if not columns and not relationships:
            return ToolResult(
                call_id="",
                content="LLM이 JSON을 반환하지 않았습니다. 다시 시도해주세요.",
                is_error=True,
            )

        saved: list[str] = []
        for key, desc in columns.items():
            if not desc:
                continue
            parts = key.split(".", 1)
            if len(parts) != 2:
                continue
            tbl_name, col_name = parts
            ctx.store.kv_set(scope, _kv_key(tbl_name, col_name), desc)
            saved.append(f"- {key}: {desc}")

        rel_lines: list[str] = []
        if relationships:
            ctx.store.kv_set(
                scope, _KV_RELATIONSHIPS, json.dumps(relationships, ensure_ascii=False)
            )
            rel_lines = [f"- {r}" for r in relationships]

        result_parts = []
        if saved:
            result_parts.append("✅ 컬럼 메타데이터 보강 완료:\n" + "\n".join(saved))
        if rel_lines:
            result_parts.append("🔗 테이블 관계 추론:\n" + "\n".join(rel_lines))

        return ToolResult(
            call_id="", content="\n\n".join(result_parts) or "보강된 내용이 없습니다."
        )
