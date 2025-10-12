"""
Retrieval Tools

테이블 메타데이터 검색을 위한 도구들
"""

from typing import Annotated
from langchain_core.tools import tool
from langgraph.prebuilt import InjectedState

from utils.llm.retrieval import search_tables


@tool
def search_tables_tool(question: str, state: Annotated[dict, InjectedState]) -> str:
    """
    질문과 관련된 테이블 메타데이터를 검색합니다.

    Args:
        question: 검색할 질문
        state: 현재 상태 (검색 설정 포함)

    Returns:
        str: 검색된 테이블 정보 메시지
    """
    try:
        # 검색 설정 가져오기
        retriever_config = state.get("retriever_config", {})
        retriever_name = retriever_config.get("retriever_name", "기본")
        top_n = retriever_config.get("top_n", 5)
        device = retriever_config.get("device", "cpu")

        # 테이블 검색
        tables = search_tables(
            query=question,
            retriever_name=retriever_name,
            top_n=top_n,
            device=device,
        )

        if not tables:
            return "❌ 관련된 테이블을 찾지 못했습니다. 질문을 다시 확인해주세요."

        # 상태에 검색 결과 저장
        state["searched_tables"] = tables

        # 검색 결과 메시지 생성
        table_names = list(tables.keys())
        table_list_str = "\n".join([f"  - {name}" for name in table_names])

        # 각 테이블의 컬럼 수 계산
        table_details = []
        for table_name, table_info in tables.items():
            # table_description을 제외한 컬럼 수
            column_count = len(
                [k for k in table_info.keys() if k != "table_description"]
            )
            table_details.append(f"  - {table_name} ({column_count}개 컬럼)")

        result_message = [
            f"📊 {len(tables)}개의 관련 테이블을 찾았습니다:",
            "\n".join(table_details),
        ]

        return "\n".join(result_message)

    except Exception as e:
        return f"❌ 테이블 검색 중 오류 발생: {str(e)}"
