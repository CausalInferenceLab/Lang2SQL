"""
Lang2SQL Streamlit 애플리케이션.

자연어로 입력된 질문을 SQL 쿼리로 변환하고,
ClickHouse 데이터베이스에 실행한 결과를 출력합니다.
"""

import streamlit as st
from langchain.chains.sql_database.prompt import SQL_PROMPTS
from langchain_core.messages import HumanMessage

from llm_utils.connect_db import ConnectDB
from llm_utils.graph import builder
from llm_utils.enriched_graph import builder as enriched_builder

DEFAULT_QUERY = "고객 데이터를 기반으로 유니크한 유저 수를 카운트하는 쿼리"
SIDEBAR_OPTIONS = {
    "show_total_token_usage": "Show Total Token Usage",
    "show_result_description": "Show Result Description",
    "show_sql": "Show SQL",
    "show_question_reinterpreted_by_ai": "Show User Question Reinterpreted by AI",
    "show_referenced_tables": "Show List of Referenced Tables",
    "show_table": "Show Table",
    "show_chart": "Show Chart",
}


def summarize_total_tokens(data: list) -> int:
    """
    메시지 데이터에서 총 토큰 사용량을 집계합니다.

    Args:
        data (list): usage_metadata를 포함하는 객체들의 리스트.

    Returns:
        int: 총 토큰 사용량 합계.
    """

    total_tokens = 0
    for item in data:
        token_usage = getattr(item, "usage_metadata", {})
        total_tokens += token_usage.get("total_tokens", 0)
    return total_tokens


def execute_query(
    *,
    query: str,
    database_env: str,
    retriever_name: str = "기본",
    top_n: int = 5,
    device: str = "cpu",
) -> dict:
    """
    Lang2SQL 그래프를 실행하여 자연어 쿼리를 SQL 쿼리로 변환하고 결과를 반환합니다.

    Args:
        query (str): 자연어로 작성된 사용자 쿼리.
        database_env (str): 사용할 데이터베이스 환경 설정 이름.
        retriever_name (str): 사용할 검색기 이름.
        top_n (int): 검색할 테이블 정보의 개수.

    Returns:
        dict: 변환된 SQL 쿼리 및 관련 메타데이터를 포함하는 결과 딕셔너리.
    """
    # 세션 상태에서 그래프 가져오기
    graph = st.session_state.get("graph")
    if graph is None:
        graph_builder = (
            enriched_builder if st.session_state.get("use_enriched") else builder
        )
        graph = graph_builder.compile()
        st.session_state["graph"] = graph

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


def display_result(
    *,
    res: dict,
    database: ConnectDB,
) -> None:
    """
    Lang2SQL 실행 결과를 Streamlit 화면에 출력합니다.

    Args:
        res (dict): Lang2SQL 실행 결과 딕셔너리.
        database (ConnectDB): SQL 쿼리 실행을 위한 데이터베이스 연결 객체.

    출력 항목:
        - 총 토큰 사용량
        - 생성된 SQL 쿼리
        - 결과 설명
        - AI가 재해석한 사용자 질문
        - 참조된 테이블 목록
        - 쿼리 실행 결과 테이블
    """
    total_tokens = summarize_total_tokens(res["messages"])

    if st.session_state.get("show_total_token_usage", True):
        st.write("총 토큰 사용량:", total_tokens)
    if st.session_state.get("show_sql", True):
        st.write("결과:", "\n\n```sql\n" + res["generated_query"].content + "\n```")
    if st.session_state.get("show_result_description", True):
        st.write("결과 설명:\n\n", res["messages"][-1].content)
    if st.session_state.get("show_question_reinterpreted_by_ai", True):
        st.write("AI가 재해석한 사용자 질문:\n", res["refined_input"].content)
    if st.session_state.get("show_referenced_tables", True):
        st.write("참고한 테이블 목록:", res["searched_tables"])
    if st.session_state.get("show_table", True):
        sql = res["generated_query"]
        df = database.run_sql(sql)
        st.dataframe(df.head(10) if len(df) > 10 else df)


db = ConnectDB()

st.title("Lang2SQL")

# 워크플로우 선택(UI)
use_enriched = st.sidebar.checkbox(
    "프로파일 추출 & 컨텍스트 보강 워크플로우 사용", value=False
)

# 세션 상태 초기화
if (
    "graph" not in st.session_state
    or st.session_state.get("use_enriched") != use_enriched
):
    graph_builder = enriched_builder if use_enriched else builder
    st.session_state["graph"] = graph_builder.compile()

    # 프로파일 추출 & 컨텍스트 보강 그래프
    st.session_state["use_enriched"] = use_enriched
    st.info("Lang2SQL이 성공적으로 시작되었습니다.")

# 새로고침 버튼 추가
if st.sidebar.button("Lang2SQL 새로고침"):
    graph_builder = (
        enriched_builder if st.session_state.get("use_enriched") else builder
    )
    st.session_state["graph"] = graph_builder.compile()
    st.sidebar.success("Lang2SQL이 성공적으로 새로고침되었습니다.")

user_query = st.text_area(
    "쿼리를 입력하세요:",
    value=DEFAULT_QUERY,
)
user_database_env = st.selectbox(
    "DB 환경정보를 입력하세요:",
    options=SQL_PROMPTS.keys(),
    index=0,
)

device = st.selectbox(
    "모델 실행 장치를 선택하세요:",
    options=["cpu", "cuda"],
    index=0,
)

retriever_options = {
    "기본": "벡터 검색 (기본)",
    "Reranker": "Reranker 검색 (정확도 향상)",
}

user_retriever = st.selectbox(
    "검색기 유형을 선택하세요:",
    options=list(retriever_options.keys()),
    format_func=lambda x: retriever_options[x],
    index=0,
)

user_top_n = st.slider(
    "검색할 테이블 정보 개수:",
    min_value=1,
    max_value=20,
    value=5,
    step=1,
    help="검색할 테이블 정보의 개수를 설정합니다. 값이 클수록 더 많은 테이블 정보를 검색하지만 처리 시간이 길어질 수 있습니다.",
)

st.sidebar.title("Output Settings")
for key, label in SIDEBAR_OPTIONS.items():
    st.sidebar.checkbox(label, value=True, key=key)

if st.button("쿼리 실행"):
    result = execute_query(
        query=user_query,
        database_env=user_database_env,
        retriever_name=user_retriever,
        top_n=user_top_n,
        device=device,
    )
    display_result(res=result, database=db)
