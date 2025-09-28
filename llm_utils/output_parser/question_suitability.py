"""
QuestionSuitability 출력 모델.

LLM 구조화 출력으로부터 SQL 적합성 판단 결과를 표현하는 Pydantic 모델입니다.
"""

from pydantic import BaseModel, Field


class QuestionSuitability(BaseModel):
    """
    SQL 생성 적합성 결과 모델.

    LLM 구조화 출력으로 직렬화 가능한 필드를 정의합니다.
    """

    reason: str = Field(description="보완/설명 사유 요약")
    missing_entities: list[str] = Field(
        default_factory=list, description="질문에서 누락된 핵심 엔터티/기간 등"
    )
    requires_data_science: bool = Field(
        default=False, description="SQL을 넘어 ML/통계 분석이 필요한지 여부"
    )
