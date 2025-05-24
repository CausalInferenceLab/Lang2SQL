"""
Lang2SQL Streamlit 애플리케이션.

자연어로 입력된 질문을 SQL 쿼리로 변환하고,
ClickHouse 데이터베이스에 실행한 결과를 출력합니다.
"""

import streamlit as st
from langchain.chains.sql_database.prompt import SQL_PROMPTS
from langchain_core.messages import AIMessage, HumanMessage

from llm_utils.connect_db import ConnectDB
from llm_utils.display_chart import DisplayChart
from llm_utils.enriched_graph import builder as enriched_builder
from llm_utils.graph import builder
from llm_utils.llm_response_parser import LLMResponseParser
from llm_utils.token_utils import TokenUtils

TITLE = "Lang2SQL"
DEFAULT_QUERY = "고객 데이터를 기반으로 유니크한 유저 수를 카운트하는 쿼리"
SIDEBAR_OPTIONS = {
    "show_token_usage": "Show Token Usage",
    "show_result_description": "Show Result Description",
    "show_sql": "Show SQL",
    "show_question_reinterpreted_by_ai": "Show User Question Reinterpreted by AI",
    "show_referenced_tables": "Show List of Referenced Tables",
    "show_table": "Show Table",
    "show_chart": "Show Chart",
}


def execute_query(
    *,
    query: str,
    database_env: str,
    retriever_name: str = "기본",
    top_n: int = 5,
    device: str = "cpu",
) -> dict:
    """
    자연어 쿼리를 SQL로 변환하고 실행 결과를 반환하는 Lang2SQL 그래프 인터페이스 함수입니다.

    이 함수는 Lang2SQL 파이프라인(graph)을 세션 상태에서 가져오거나 새로 컴파일한 뒤,
    사용자의 자연어 질문을 SQL 쿼리로 변환하고 관련 메타데이터와 함께 결과를 반환합니다.
    내부적으로 LangChain의 `graph.invoke` 메서드를 호출합니다.

    Args:
        query (str): 사용자가 입력한 자연어 기반 질문.
        database_env (str): 사용할 데이터베이스 환경 이름 또는 키 (예: "dev", "prod").
        retriever_name (str, optional): 테이블 검색기 이름. 기본값은 "기본".
        top_n (int, optional): 검색된 상위 테이블 수 제한. 기본값은 5.
        device (str, optional): LLM 실행에 사용할 디바이스 ("cpu" 또는 "cuda"). 기본값은 "cpu".

    Returns:
        dict: 다음 정보를 포함한 Lang2SQL 실행 결과 딕셔너리:
            - "generated_query": 생성된 SQL 쿼리 (`AIMessage`)
            - "messages": 전체 LLM 응답 메시지 목록
            - "refined_input": AI가 재구성한 입력 질문
            - "searched_tables": 참조된 테이블 목록 등 추가 정보
    """

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

    def should_show(_key: str) -> bool:
        return st.session_state.get(_key, True)

    if should_show("show_token_usage"):
        st.markdown("---")
        token_summary = TokenUtils.get_token_usage_summary(data=res["messages"])
        st.write("**토큰 사용량:**")
        st.markdown(
            f"""
        - Input tokens: `{token_summary['input_tokens']}`
        - Output tokens: `{token_summary['output_tokens']}`
        - Total tokens: `{token_summary['total_tokens']}`
        """
        )

    if should_show("show_sql"):
        st.markdown("---")
        generated_query = res.get("generated_query")
        query_text = (
            generated_query.content
            if isinstance(generated_query, AIMessage)
            else str(generated_query)
        )

        try:
            sql = LLMResponseParser.extract_sql(query_text)
            st.markdown("**생성된 SQL 쿼리:**")
            st.code(sql, language="sql")
        except ValueError:
            st.warning("SQL 블록을 추출할 수 없습니다.")
            st.text(query_text)

        interpretation = LLMResponseParser.extract_interpretation(query_text)
        if interpretation:
            st.markdown("**결과 해석:**")
            st.code(interpretation)

    if should_show("show_result_description"):
        st.markdown("---")
        st.markdown("**결과 설명:**")
        result_message = res["messages"][-1].content

        try:
            sql = LLMResponseParser.extract_sql(result_message)
            st.code(sql, language="sql")
        except ValueError:
            st.warning("SQL 블록을 추출할 수 없습니다.")
            st.text(result_message)

        interpretation = LLMResponseParser.extract_interpretation(result_message)
        if interpretation:
            st.code(interpretation, language="plaintext")

    if should_show("show_question_reinterpreted_by_ai"):
        st.markdown("---")
        st.markdown("**AI가 재해석한 사용자 질문:**")
        st.code(res["refined_input"].content)

    if should_show("show_referenced_tables"):
        st.markdown("---")
        st.markdown("**참고한 테이블 목록:**")
        st.write(res.get("searched_tables", []))

    if should_show("show_table"):
        st.markdown("---")
        try:
            sql_raw = (
                res["generated_query"].content
                if isinstance(res["generated_query"], AIMessage)
                else str(res["generated_query"])
            )
            sql = LLMResponseParser.extract_sql(sql_raw)
            df = database.run_sql(sql)
            st.dataframe(df.head(10) if len(df) > 10 else df)
        except Exception as e:
            st.error(f"쿼리 실행 중 오류 발생: {e}")

    if should_show("show_chart"):
        st.markdown("---")
        df = database.run_sql(sql)
        st.markdown("**쿼리 결과 시각화:**")
        display_code = DisplayChart(
            question=res["refined_input"].content,
            sql=sql,
            df_metadata=f"Running df.dtypes gives:\n{df.dtypes}",
        )
        # plotly_code 변수도 따로 보관할 필요 없이 바로 그려도 됩니다
        fig = display_code.get_plotly_figure(
            plotly_code=display_code.generate_plotly_code(), df=df
        )
        st.plotly_chart(fig)


db = ConnectDB()

st.title(TITLE)

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
