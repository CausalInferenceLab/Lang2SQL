import os
import sys

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import streamlit as st
from langchain_core.messages import HumanMessage
from llm_utils.graph import builder
from langchain.chains.sql_database.prompt import SQL_PROMPTS

import agent.query_generator.few_shot as few_shot

# Streamlit 앱 제목
st.title("Lang2SQL")

# 세션 상태 초기화
if "query_executed" not in st.session_state:
    st.session_state.query_executed = False
if "result" not in st.session_state:
    st.session_state.result = None
if "feedback_given" not in st.session_state:
    st.session_state.feedback_given = False

# 사용자 입력 받기
user_query = st.text_area(
    "쿼리를 입력하세요:",
    value="고객 데이터를 기반으로 유니크한 유저 수를 카운트하는 쿼리",
)

user_database_env = st.selectbox(
    "db 환경정보를 입력하세요:",
    options=SQL_PROMPTS.keys(),
    index=0,
)


# Token usage 집계 함수 정의
def summarize_total_tokens(data):
    total_tokens = 0
    for item in data:
        token_usage = getattr(item, "usage_metadata", {})
        total_tokens += token_usage.get("total_tokens", 0)
    return total_tokens


# 만족 피드백 저장 함수
def save_satisfied_feedback():
    if st.session_state.result:
        res = st.session_state.result
        success = few_shot.add_to_vectorstore(
            question=user_query,
            answer=res["messages"][-1].content,
            index_name="few_shot_satisfied",
            meta={},
        )
        st.session_state.feedback_given = True
        if success:
            st.success("만족한 답변이 few-shot 벡터스토어에 저장되었습니다!")
        else:
            st.error("저장 중 오류가 발생했습니다.")


# 불만족 피드백 저장 함수
def save_dissatisfied_feedback():
    if st.session_state.result:
        res = st.session_state.result
        success = few_shot.add_to_vectorstore(
            question=user_query,
            answer=res["messages"][-1].content,
            index_name="few_shot_dissatisfied",
            meta={},
        )
        st.session_state.feedback_given = True
        if success:
            st.success("불만족한 답변이 별도의 벡터스토어에 저장되었습니다!")
        else:
            st.error("저장 중 오류가 발생했습니다.")


# 버튼 클릭 시 실행
if st.button("쿼리 실행"):
    # 세션 상태 초기화
    st.session_state.feedback_given = False

    # 그래프 컴파일 및 쿼리 실행
    graph = builder.compile()

    res = graph.invoke(
        input={
            "messages": [HumanMessage(content=user_query)],
            "user_database_env": user_database_env,
            "best_practice_query": "",
        }
    )

    # 세션 상태에 결과와 실행 상태 저장
    st.session_state.result = res
    st.session_state.query_executed = True

    total_tokens = summarize_total_tokens(res["messages"])

    # 결과 출력
    st.write("총 토큰 사용량:", total_tokens)
    st.write("결과 설명:\n\n", res["messages"][-1].content)
    st.write("AI가 재해석한 사용자 질문:\n", res["refined_input"].content)
    st.write("참고한 테이블 목록:", res["searched_tables"])


# 쿼리가 실행되고 피드백이 아직 제공되지 않은 경우에만 피드백 버튼 표시
if st.session_state.query_executed and not st.session_state.feedback_given:
    col1, col2 = st.columns(2)

    with col1:
        if st.button("이 답변에 만족합니다"):
            save_satisfied_feedback()

    with col2:
        if st.button("이 답변에 불만족합니다"):
            save_dissatisfied_feedback()
