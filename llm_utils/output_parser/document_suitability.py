"""
DocumentSuitability 출력 모델.

LLM 구조화 출력으로부터 테이블별 적합성 평가 결과를 표현하는 Pydantic 모델입니다.
최상위는 테이블명(string) -> 평가 객체 매핑을 담는 Root 모델입니다.
"""

from typing import Dict, List
from pydantic import BaseModel, Field


class DocumentSuitability(BaseModel):
    """
    단일 테이블에 대한 적합성 평가 결과.
    """

    table_name: str = Field(description="테이블명")
    score: float = Field(description="0.0~1.0 사이의 적합도 점수")
    reason: str = Field(description="한국어 한두 문장 근거")
    matched_columns: List[str] = Field(
        default_factory=list, description="질문과 직접 연관된 컬럼명 목록"
    )
    missing_entities: List[str] = Field(
        default_factory=list, description="부족한 엔티티/지표/기간 등"
    )


class DocumentSuitabilityList(BaseModel):
    """
    문서 적합성 평가 결과 리스트 래퍼.

    OpenAI Structured Outputs 호환을 위해 명시적 최상위 키(`results`)를 둡니다.
    """

    results: List[DocumentSuitability] = Field(description="평가 결과 목록")
