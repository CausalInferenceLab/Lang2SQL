"""
LLM 기반 가이드라인 매칭 로직
"""

import json
from typing import Any, Dict, List, Optional

from openai import OpenAI

from utils.llm.chatbot.types import Guideline


class LLMGuidelineMatcher:
    def __init__(
        self,
        guidelines: List[Guideline],
        model: str,
        client_obj: Optional[OpenAI] = None,
    ):
        self.guidelines = guidelines
        self.model = model
        self.client = client_obj or OpenAI()
        self._id_set = {g.id for g in guidelines}

    def _build_messages(self, message: str) -> List[Dict[str, str]]:
        sys = (
            "You are a strict GuidelineMatcher.\n"
            "Return ONLY a JSON object that matches the provided JSON schema."
        )
        lines = [
            "아래 사용자 메시지에 해당하는 모든 가이드라인 id를 선택하세요.",
            f"[USER MESSAGE]\n{message}\n",
            "[GUIDELINES]",
        ]
        for g in self.guidelines:
            examples = ", ".join(g.example_phrases) if g.example_phrases else "-"
            lines.append(
                f"- id: {g.id}\n  desc: {g.description}\n  examples: {examples}"
            )
        return [
            {"role": "system", "content": sys},
            {"role": "user", "content": "\n".join(lines)},
        ]

    def _json_schema_spec(self) -> Dict[str, Any]:
        return {
            "name": "guideline_matches",
            "schema": {
                "type": "object",
                "properties": {
                    "matches": {
                        "type": "array",
                        "items": {"type": "string", "enum": list(self._id_set)},
                    }
                },
                "required": ["matches"],
                "additionalProperties": False,
            },
            "strict": True,
        }

    def match(self, message: str) -> List[Guideline]:
        ids: List[str] = []
        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                temperature=0,
                messages=self._build_messages(message),
                response_format={
                    "type": "json_schema",
                    "json_schema": self._json_schema_spec(),
                },
            )
            raw = completion.choices[0].message.content
            data = json.loads(raw) if isinstance(raw, str) else raw
            ids = [i for i in (data.get("matches") or []) if i in self._id_set]
        except Exception:
            # LLM 호출 실패 시 빈 리스트 반환 (일반 대화로 처리)
            ids = []

        id_to_g = {g.id: g for g in self.guidelines}
        selected = [id_to_g[i] for i in ids if i in id_to_g]
        selected.sort(key=lambda g: g.priority, reverse=True)
        return selected
