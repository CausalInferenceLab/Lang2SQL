"""
Lang2SQL Streamlit 애플리케이션.

자연어로 입력된 질문을 SQL 쿼리로 변환하고,
ClickHouse 데이터베이스에 실행한 결과를 출력합니다.
"""

import re

import streamlit as st
from langchain.chains.sql_database.prompt import SQL_PROMPTS
from langchain_core.messages import AIMessage

from db_utils import get_db_connector
from db_utils.base_connector import BaseConnector
from viz.display_chart import DisplayChart
from engine.query_executor import execute_query as execute_query_common
from llm_utils.llm_response_parser import LLMResponseParser
from infra.observability.token_usage import TokenUtils
from llm_utils.graph_utils.enriched_graph import builder as enriched_builder
from llm_utils.graph_utils.basic_graph import builder


TITLE = "Lang2SQL"
DEFAULT_QUERY = "고객 데이터를 기반으로 유니크한 유저 수를 카운트하는 쿼리"
SIDEBAR_OPTIONS = {
    "show_token_usage": "Show Token Usage",
    "show_result_description": "Show Result Description",
    "show_sql": "Show SQL",
    "show_question_reinterpreted_by_ai": "Show User Question Reinterpreted by AI",
    "show_referenced_tables": "Show List of Referenced Tables",
    "show_question_gate_result": "Show Question Gate Result",
    "show_document_suitability": "Show Document Suitability",
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

    이 함수는 공용 execute_query 함수를 호출하여 Lang2SQL 파이프라인을 실행합니다.
    Streamlit 세션 상태를 활용하여 그래프를 재사용합니다.

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
            - "searched_tables": 참조된 테이블 목록 등 추가 정보
    """

    return execute_query_common(
        query=query,
        database_env=database_env,
        retriever_name=retriever_name,
        top_n=top_n,
        device=device,
        use_enriched_graph=st.session_state.get("use_enriched", False),
        session_state=st.session_state,
    )


def display_result(
    *,
    res: dict,
    database: BaseConnector,
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

    has_query = bool(res.get("generated_query"))
    # 섹션 표시 여부를 QUERY_MAKER 출력 유무에 따라 제어
    show_sql_section = has_query and should_show("show_sql")
    show_result_desc = has_query and should_show("show_result_description")
    show_reinterpreted = has_query and should_show("show_question_reinterpreted_by_ai")
    show_gate_result = should_show("show_question_gate_result")
    show_doc_suitability = should_show("show_document_suitability")
    show_table_section = has_query and should_show("show_table")
    show_chart_section = has_query and should_show("show_chart")
    if show_gate_result and ("question_gate_result" in res):
        st.markdown("---")
        st.markdown("**Question Gate 결과:**")
        details = res.get("question_gate_result")
        if details:
            try:
                import json as _json

                st.code(
                    _json.dumps(details, ensure_ascii=False, indent=2), language="json"
                )
            except Exception:
                st.write(details)

    if show_doc_suitability and ("document_suitability" in res):
        st.markdown("---")
        st.markdown("**문서 적합성 평가:**")
        ds = res.get("document_suitability")
        if not isinstance(ds, dict):
            st.write(ds)
        else:

            def _as_float(value):
                try:
                    return float(value)
                except Exception:
                    return -1.0

            rows = [
                {
                    "table": table_name,
                    "score": _as_float(info.get("score", -1)),
                    "matched_columns": ", ".join(info.get("matched_columns", [])),
                    "missing_entities": ", ".join(info.get("missing_entities", [])),
                    "reason": info.get("reason", ""),
                }
                for table_name, info in ds.items()
                if isinstance(info, dict)
            ]

            rows.sort(key=lambda r: r["score"], reverse=True)
            if rows:
                st.dataframe(rows, use_container_width=True)
            else:
                st.info("문서 적합성 평가 결과가 비어 있습니다.")

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

    if show_sql_section:
        st.markdown("---")
        generated_query = res.get("generated_query")
        if generated_query:
            query_text = (
                generated_query.content
                if isinstance(generated_query, AIMessage)
                else str(generated_query)
            )

            # query_text가 문자열인지 확인
            if isinstance(query_text, str):
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
            else:
                st.warning("쿼리 텍스트가 문자열이 아닙니다.")
                st.text(str(query_text))

    if show_result_desc:
        st.markdown("---")
        st.markdown("**결과 설명:**")
        result_message = res["messages"][-1].content

        if isinstance(result_message, str):
            try:
                sql = LLMResponseParser.extract_sql(result_message)
                st.code(sql, language="sql")
            except ValueError:
                st.warning("SQL 블록을 추출할 수 없습니다.")
                st.text(result_message)

            interpretation = LLMResponseParser.extract_interpretation(result_message)
            if interpretation:
                st.code(interpretation, language="plaintext")
        else:
            st.warning("결과 메시지가 문자열이 아닙니다.")
            st.text(str(result_message))

    if show_reinterpreted:
        st.markdown("---")
        st.markdown("**AI가 재해석한 사용자 질문:**")
        try:
            if len(res["messages"]) > 1:
                candidate = res["messages"][-2]
                question_text = (
                    candidate.content
                    if hasattr(candidate, "content")
                    else str(candidate)
                )
            else:
                question_text = res["messages"][0].content
        except Exception:
            question_text = str(res["messages"][0].content)
        st.code(question_text)

    if should_show("show_referenced_tables"):
        st.markdown("---")
        st.markdown("**참고한 테이블 목록:**")
        st.write(res.get("searched_tables", []))

    # QUERY_MAKER가 비활성화된 경우 안내 메시지 출력
    if not has_query:
        st.info("QUERY_MAKER 없이 실행되었습니다. 검색된 테이블 정보만 표시합니다.")

    if show_table_section:
        st.markdown("---")
        try:
            sql_raw = (
                res["generated_query"].content
                if isinstance(res["generated_query"], AIMessage)
                else str(res["generated_query"])
            )
            if isinstance(sql_raw, str):
                sql = LLMResponseParser.extract_sql(sql_raw)
                df = database.run_sql(sql)
                st.dataframe(df.head(10) if len(df) > 10 else df)
            else:
                st.error("SQL 원본이 문자열이 아닙니다.")
        except Exception as e:
            st.error(f"쿼리 실행 중 오류 발생: {e}")

    if show_chart_section:
        st.markdown("---")
        try:
            sql_raw = (
                res["generated_query"].content
                if isinstance(res["generated_query"], AIMessage)
                else str(res["generated_query"])
            )
            if isinstance(sql_raw, str):
                sql = LLMResponseParser.extract_sql(sql_raw)
                df = database.run_sql(sql)
                st.markdown("**쿼리 결과 시각화:**")
                try:
                    if len(res["messages"]) > 1:
                        candidate = res["messages"][-2]
                        chart_question = (
                            candidate.content
                            if hasattr(candidate, "content")
                            else str(candidate)
                        )
                    else:
                        chart_question = res["messages"][0].content
                except Exception:
                    chart_question = str(res["messages"][0].content)

                display_code = DisplayChart(
                    question=chart_question,
                    sql=sql,
                    df_metadata=f"Running df.dtypes gives:\n{df.dtypes}",
                )
                # plotly_code 변수도 따로 보관할 필요 없이 바로 그려도 됩니다
                fig = display_code.get_plotly_figure(
                    plotly_code=display_code.generate_plotly_code(), df=df
                )
                st.plotly_chart(fig)
            else:
                st.error("SQL 원본이 문자열이 아닙니다.")
        except Exception as e:
            st.error(f"차트 생성 중 오류 발생: {e}")


db = get_db_connector()

st.title(TITLE)

# 워크플로우 선택(UI)
st.sidebar.markdown("### 워크플로우 선택")
use_enriched = st.sidebar.checkbox(
    "프로파일 추출 & 컨텍스트 보강 워크플로우 사용", value=False
)

# 세션 상태 초기화
if (
    "graph" not in st.session_state
    or st.session_state.get("use_enriched") != use_enriched
):
    # 그래프 선택 로직
    if use_enriched:
        graph_builder = enriched_builder
        graph_type = "확장된"
    else:
        graph_builder = builder
        graph_type = "기본"

    st.session_state["graph"] = graph_builder.compile()
    st.session_state["use_enriched"] = use_enriched
    st.info(f"Lang2SQL이 성공적으로 시작되었습니다. ({graph_type} 워크플로우)")

# 새로고침 버튼 추가
if st.sidebar.button("Lang2SQL 새로고침"):
    # 그래프 선택 로직
    if st.session_state.get("use_enriched"):
        graph_builder = enriched_builder
        graph_type = "확장된"
    else:
        graph_builder = builder
        graph_type = "기본"

    st.session_state["graph"] = graph_builder.compile()
    st.sidebar.success(
        f"Lang2SQL이 성공적으로 새로고침되었습니다. ({graph_type} 워크플로우)"
    )

user_query = st.text_area(
    "쿼리를 입력하세요:",
    value=DEFAULT_QUERY,
)
user_database_env = st.selectbox(
    "DB 환경정보를 입력하세요:",
    options=SQL_PROMPTS.keys(),
    index=0,
)

_device_options = ["cpu", "cuda"]
_default_device = st.session_state.get("default_device", "cpu")
_device_index = (
    _device_options.index(_default_device) if _default_device in _device_options else 0
)
device = st.selectbox(
    "모델 실행 장치를 선택하세요:",
    options=_device_options,
    index=_device_index,
)

retriever_options = {
    "기본": "벡터 검색 (기본)",
    "Reranker": "Reranker 검색 (정확도 향상)",
}

_retriever_keys = list(retriever_options.keys())
_default_retriever = st.session_state.get("default_retriever_name", "기본")
_retriever_index = (
    _retriever_keys.index(_default_retriever)
    if _default_retriever in _retriever_keys
    else 0
)
user_retriever = st.selectbox(
    "검색기 유형을 선택하세요:",
    options=_retriever_keys,
    format_func=lambda x: retriever_options[x],
    index=_retriever_index,
)

user_top_n = st.slider(
    "검색할 테이블 정보 개수:",
    min_value=1,
    max_value=20,
    value=int(st.session_state.get("default_top_n", 5)),
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
