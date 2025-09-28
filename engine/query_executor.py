"""
Lang2SQL 쿼리 실행을 위한 공용 모듈입니다.

이 모듈은 CLI와 Streamlit 인터페이스에서 공통으로 사용할 수 있는
쿼리 실행 함수를 제공합니다.
"""

import logging
from typing import Dict, Any, Optional, Union

from langchain_core.messages import HumanMessage

from llm_utils.graph_utils.enriched_graph import builder as enriched_builder
from llm_utils.graph_utils.basic_graph import builder as basic_builder
from llm_utils.llm_response_parser import LLMResponseParser

logger = logging.getLogger(__name__)


def execute_query(
    *,
    query: str,
    database_env: str,
    retriever_name: str = "기본",
    top_n: int = 5,
    device: str = "cpu",
    use_enriched_graph: bool = False,
    session_state: Optional[Union[Dict[str, Any], Any]] = None,
) -> Dict[str, Any]:
    """
    자연어 쿼리를 SQL로 변환하고 실행 결과를 반환하는 공용 함수입니다.

    이 함수는 Lang2SQL 파이프라인(graph)을 사용하여 사용자의 자연어 질문을
    SQL 쿼리로 변환하고 관련 메타데이터와 함께 결과를 반환합니다.
    CLI와 Streamlit 인터페이스에서 공통으로 사용할 수 있습니다.

    Args:
        query (str): 사용자가 입력한 자연어 기반 질문.
        database_env (str): 사용할 데이터베이스 환경 이름 또는 키 (예: "dev", "prod").
        retriever_name (str, optional): 테이블 검색기 이름. 기본값은 "기본".
        top_n (int, optional): 검색된 상위 테이블 수 제한. 기본값은 5.
        device (str, optional): LLM 실행에 사용할 디바이스 ("cpu" 또는 "cuda"). 기본값은 "cpu".
        use_enriched_graph (bool, optional): 확장된 그래프 사용 여부. 기본값은 False.
        session_state (Optional[Union[Dict[str, Any], Any]], optional): Streamlit 세션 상태 (Streamlit에서만 사용).

    Returns:
        Dict[str, Any]: 다음 정보를 포함한 Lang2SQL 실행 결과 딕셔너리:
            - "generated_query": 생성된 SQL 쿼리 (`AIMessage`)
            - "messages": 전체 LLM 응답 메시지 목록
            - "searched_tables": 참조된 테이블 목록 등 추가 정보
    """

    logger.info("Processing query: %s", query)

    # 그래프 선택
    if use_enriched_graph:
        graph_type = "enriched"
        graph_builder = enriched_builder
    else:
        graph_type = "basic"
        graph_builder = basic_builder

    logger.info("Using %s graph", graph_type)

    # 그래프 선택 및 컴파일
    if session_state is not None:
        # Streamlit 환경: 세션 상태에서 그래프 재사용
        graph = session_state.get("graph")
        if graph is None:
            graph = graph_builder.compile()
            session_state["graph"] = graph
    else:
        # CLI 환경: 매번 새로운 그래프 컴파일
        graph = graph_builder.compile()

    # 그래프 실행
    res = graph.invoke(
        input={
            "messages": [HumanMessage(content=query)],
            "user_database_env": database_env,
            "best_practice_query": "",
            "retriever_name": retriever_name,
            "top_n": top_n,
            "device": device,
        }
    )

    return res


def extract_sql_from_result(res: Dict[str, Any]) -> Optional[str]:
    """
    Lang2SQL 실행 결과에서 SQL 쿼리를 추출합니다.

    Args:
        res (Dict[str, Any]): execute_query 함수의 반환 결과

    Returns:
        Optional[str]: 추출된 SQL 쿼리 문자열. 추출 실패 시 None
    """
    generated_query = res.get("generated_query")
    if not generated_query:
        logger.error("생성된 쿼리가 없습니다.")
        return None

    query_text = (
        generated_query.content
        if hasattr(generated_query, "content")
        else str(generated_query)
    )

    try:
        sql = LLMResponseParser.extract_sql(query_text)
        return sql
    except ValueError:
        logger.error("SQL을 추출할 수 없습니다.")
        return None
