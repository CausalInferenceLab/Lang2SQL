"""
ChatBot 가이드라인 및 툴 정의
"""

from typing import Any, Dict, List

from utils.llm.tools import (
    search_database_tables,
    get_glossary_terms,
    get_query_examples,
)
from utils.llm.chatbot.types import Guideline


def search_database_tables_tool(ctx: Dict[str, Any]) -> str:
    query = ctx.get("query") or ctx.get("last_user_message", "")
    return str(search_database_tables.invoke({"query": query}))


def get_glossary_terms_tool(ctx: Dict[str, Any]) -> str:
    query = ctx.get("query") or ctx.get("last_user_message", "")
    return str(get_glossary_terms.invoke({"query": query}))


def get_query_examples_tool(ctx: Dict[str, Any]) -> str:
    query = ctx.get("query") or ctx.get("last_user_message", "")
    return str(get_query_examples.invoke({"query": query}))


GUIDELINES: List[Guideline] = [
    Guideline(
        id="table_schema",
        description="데이터베이스 테이블 정보나 스키마 확인이 필요할 때 사용",
        example_phrases=[
            "테이블 정보 알려줘",
            "어떤 컬럼이 있어?",
            "스키마 보여줘",
            "데이터 구조가 궁금해",
        ],
        tools=[search_database_tables_tool],
        priority=10,
    ),
    Guideline(
        id="glossary",
        description="용어의 정의나 비즈니스 의미 확인이 필요할 때 사용",
        example_phrases=[
            "용어집 보여줘",
            "이 단어 뜻이 뭐야?",
            "비즈니스 용어 설명해줘",
            "KPI 정의가 뭐야?",
        ],
        tools=[get_glossary_terms_tool],
        priority=8,
    ),
    Guideline(
        id="query_examples",
        description="쿼리 예제나 SQL 작성 패턴 확인이 필요할 때 사용",
        example_phrases=[
            "쿼리 예제 보여줘",
            "비슷한 쿼리 있어?",
            "SQL 어떻게 짜야해?",
            "다른 사람들은 어떻게 쿼리했어?",
        ],
        tools=[get_query_examples_tool],
        priority=9,
    ),
]
