"""
Lang2SQL Streamlit 애플리케이션.

자연어 질의를 SQL 쿼리로 변환하고 실행 결과를 시각화하는 인터페이스를 제공합니다.
사용자는 데이터베이스 다이얼렉트 선택 및 편집, 검색기(retriever) 방식 지정, 토큰 사용량/결과 설명/시각화 등 다양한 출력 옵션을 설정할 수 있습니다.

주요 기능:
    - 사용자 질의를 SQL 쿼리로 변환 후 실행
    - DB 다이얼렉트(PRESET_DIALECTS) 선택 및 편집 지원
    - 검색기 유형 및 Top-N 테이블 검색 개수 설정
    - 쿼리 실행 결과를 표와 차트로 시각화
    - 토큰 사용량, 문서 적합성 평가, AI 재해석 질의 등 추가 정보 표시
"""

from copy import deepcopy

import streamlit as st

from interface.core.dialects import PRESET_DIALECTS, DialectOption
from interface.core.lang2sql_runner import run_lang2sql
from interface.core.result_renderer import display_result
from interface.core.session_utils import init_graph
from interface.core.config import load_config
from interface.app_pages.sidebar_components import (
    render_sidebar_data_source_selector,
    render_sidebar_llm_selector,
    render_sidebar_embedding_selector,
    render_sidebar_db_selector,
)

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

st.title(TITLE)

config = load_config()

render_sidebar_data_source_selector(config)
st.sidebar.divider()
render_sidebar_llm_selector()
st.sidebar.divider()
render_sidebar_embedding_selector()
st.sidebar.divider()
render_sidebar_db_selector()
st.sidebar.divider()

st.sidebar.title("Output Settings")
for key, label in SIDEBAR_OPTIONS.items():
    st.sidebar.checkbox(label, value=True, key=key)

st.sidebar.markdown("### 워크플로우 선택")
use_enriched = st.sidebar.checkbox(
    "프로파일 추출 & 컨텍스트 보강 워크플로우 사용", value=False
)

if (
    "graph" not in st.session_state
    or st.session_state.get("use_enriched") != use_enriched
):
    GRAPH_TYPE = init_graph(use_enriched)
    st.info(f"Lang2SQL 시작됨. ({GRAPH_TYPE} 워크플로우)")

if st.sidebar.button("Lang2SQL 새로고침"):
    GRAPH_TYPE = init_graph(st.session_state.get("use_enriched", False))
    st.sidebar.success(
        f"Lang2SQL이 성공적으로 새로고침되었습니다. ({GRAPH_TYPE} 워크플로우)"
    )

## moved to component: render_sidebar_llm_selector()

user_query = st.text_area("쿼리를 입력하세요:", value=DEFAULT_QUERY)

if "dialects" not in st.session_state:
    st.session_state["dialects"] = {k: v.to_dict() for k, v in PRESET_DIALECTS.items()}

st.markdown("### DB 선택 및 관리")
cols = st.columns(2)
dialects = st.session_state["dialects"]
keys = list(dialects.keys())
active = st.session_state.get("active_dialect", keys[0])

with cols[0]:
    user_database_env = st.selectbox(
        "사용할 DB를 선택하세요:", options=keys, index=keys.index(active)
    )
    st.session_state["active_dialect"] = user_database_env
    st.session_state["selected_dialect_option"] = dialects[user_database_env]

with cols[1]:
    st.caption("선택된 DB 설정을 편집하거나 새로 추가할 수 있습니다.")

with st.expander("DB 편집"):
    edit_key = st.selectbox(
        "편집할 DB를 선택하세요:",
        options=keys,
        index=keys.index(active),
        key="dialect_edit_selector",
    )
    current = deepcopy(dialects[edit_key])
    _supports_ilike = st.checkbox(
        "ILIKE 지원", value=bool(current.get("supports_ilike", False))
    )
    _hints_text = st.text_area(
        "hints (쉼표로 구분)", value=", ".join(current.get("hints", []))
    )
    if st.button("변경사항 저장", key="btn_save_dialect_edit"):
        st.session_state["dialects"][edit_key] = DialectOption(
            name=edit_key,
            supports_ilike=_supports_ilike,
            hints=[s.strip() for s in _hints_text.split(",") if s.strip()],
        ).to_dict()
        st.success(f"{edit_key} DB가 업데이트되었습니다.")

device = st.selectbox("모델 실행 장치", options=["cpu", "cuda"], index=0)
retriever_options = {
    "기본": "벡터 검색 (기본)",
    "Reranker": "Reranker 검색 (정확도 향상)",
}
user_retriever = st.selectbox(
    "검색기 유형을 선택하세요:",
    options=list(retriever_options.keys()),
    format_func=lambda x: retriever_options[x],
)
user_top_n = st.slider("검색할 테이블 정보 개수:", min_value=1, max_value=20, value=5)

if st.button("쿼리 실행"):
    res = run_lang2sql(
        query=user_query,
        database_env=user_database_env,
        retriever_name=user_retriever,
        top_n=user_top_n,
        device=device,
        use_enriched=use_enriched,
    )
    display_result(res=res)
