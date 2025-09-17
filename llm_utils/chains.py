"""
LLM 체인 생성 모듈.

이 모듈은 Lang2SQL에서 사용하는 다양한 LangChain 기반 체인을 정의합니다.
- Query Maker
- Query Enrichment
- Profile Extraction
- Question Gate (SQL 적합성 분류)
"""

import os
from langchain_core.prompts import (
    ChatPromptTemplate,
    SystemMessagePromptTemplate,
)
from pydantic import BaseModel, Field
from llm_utils.output_parser.question_suitability import QuestionSuitability
from llm_utils.output_parser.document_suitability import (
    DocumentSuitabilityList,
)

from llm_utils.llm import get_llm

from prompt.template_loader import get_prompt_template


llm = get_llm()


class QuestionProfile(BaseModel):
    """
    자연어 질문의 특징을 구조화해 표현하는 프로파일 모델.

    이 프로파일은 이후 컨텍스트 보강 및 SQL 생성 시 힌트로 사용됩니다.
    """

    is_timeseries: bool = Field(description="시계열 분석 필요 여부")
    is_aggregation: bool = Field(description="집계 함수 필요 여부")
    has_filter: bool = Field(description="조건 필터 필요 여부")
    is_grouped: bool = Field(description="그룹화 필요 여부")
    has_ranking: bool = Field(description="정렬/순위 필요 여부")
    has_temporal_comparison: bool = Field(description="기간 비교 포함 여부")
    intent_type: str = Field(description="질문의 주요 의도 유형")


# QueryMakerChain
def create_query_maker_chain(llm):
    """
    SQL 쿼리 생성을 위한 체인을 생성합니다.

    Args:
        llm: LangChain 호환 LLM 인스턴스

    Returns:
        Runnable: 입력 프롬프트를 받아 SQL을 생성하는 체인
    """
    prompt = get_prompt_template("query_maker_prompt")
    query_maker_prompt = ChatPromptTemplate.from_messages(
        [
            SystemMessagePromptTemplate.from_template(prompt),
        ]
    )
    return query_maker_prompt | llm


def create_query_enrichment_chain(llm):
    """
    사용자 질문을 메타데이터로 보강하기 위한 체인을 생성합니다.

    Args:
        llm: LangChain 호환 LLM 인스턴스

    Returns:
        Runnable: 보강된 질문 텍스트를 반환하는 체인
    """
    prompt = get_prompt_template("query_enrichment_prompt")

    enrichment_prompt = ChatPromptTemplate.from_messages(
        [
            SystemMessagePromptTemplate.from_template(prompt),
        ]
    )

    chain = enrichment_prompt | llm
    return chain


def create_profile_extraction_chain(llm):
    """
    질문으로부터 `QuestionProfile`을 추출하는 체인을 생성합니다.

    Args:
        llm: LangChain 호환 LLM 인스턴스

    Returns:
        Runnable: `QuestionProfile` 구조화 출력을 반환하는 체인
    """
    prompt = get_prompt_template("profile_extraction_prompt")

    profile_prompt = ChatPromptTemplate.from_messages(
        [
            SystemMessagePromptTemplate.from_template(prompt),
        ]
    )

    chain = profile_prompt | llm.with_structured_output(QuestionProfile)
    return chain


def create_question_gate_chain(llm):
    """
    질문 적합성(Question Gate) 체인을 생성합니다.

    ChatPromptTemplate(SystemMessage) + LLM 구조화 출력으로
    `QuestionSuitability`를 반환합니다.

    Args:
        llm: LangChain 호환 LLM 인스턴스

    Returns:
        Runnable: invoke({"question": str}) -> QuestionSuitability
    """

    prompt = get_prompt_template("question_gate_prompt")
    gate_prompt = ChatPromptTemplate.from_messages(
        [SystemMessagePromptTemplate.from_template(prompt)]
    )
    return gate_prompt | llm.with_structured_output(QuestionSuitability)


def create_document_suitability_chain(llm):
    """
    문서 적합성 평가 체인을 생성합니다.

    질문(question)과 검색 결과(tables)를 입력으로 받아
    테이블별 적합도 점수를 포함한 JSON 딕셔너리를 반환합니다.

    Returns:
        Runnable: invoke({"question": str, "tables": dict}) -> {"results": DocumentSuitability[]}
    """

    prompt = get_prompt_template("document_suitability_prompt")
    doc_prompt = ChatPromptTemplate.from_messages(
        [SystemMessagePromptTemplate.from_template(prompt)]
    )
    return doc_prompt | llm.with_structured_output(DocumentSuitabilityList)


query_maker_chain = create_query_maker_chain(llm)
profile_extraction_chain = create_profile_extraction_chain(llm)
query_enrichment_chain = create_query_enrichment_chain(llm)
question_gate_chain = create_question_gate_chain(llm)
document_suitability_chain = create_document_suitability_chain(llm)
