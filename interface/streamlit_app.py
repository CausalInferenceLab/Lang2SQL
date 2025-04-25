import os
import sys
# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import streamlit as st
from langchain_core.messages import HumanMessage
from llm_utils.graph import builder
from RAG.agentic.graph import RAGGraph  
from langchain.chains.sql_database.prompt import SQL_PROMPTS

# Streamlit 앱 제목
st.title("Lang2SQL")

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


# 버튼 클릭 시 실행
def summarize_total_tokens(data):
    total_tokens = 0
    for item in data:
        token_usage = getattr(item, "usage_metadata", {})
        total_tokens += token_usage.get("total_tokens", 0)
    return total_tokens


# 버튼 클릭 시 실행
if st.button("쿼리 실행"):
    # 그래프 컴파일 및 쿼리 실행
    graph = builder.compile()

    res = graph.invoke(
        input={
            "messages": [HumanMessage(content=user_query)],
            "user_database_env": user_database_env,
            "best_practice_query": "",
        }
    )
    total_tokens = summarize_total_tokens(res["messages"])

    # 결과 출력
    st.write("총 토큰 사용량:", total_tokens)
    # st.write("결과:", res["generated_query"].content)
    st.write("결과:", "\n\n```sql\n" + res["generated_query"] + "\n```")
    st.write("결과 설명:\n\n", res["messages"][-1].content)
    st.write("AI가 재해석한 사용자 질문:\n", res["refined_input"].content)
    st.write("참고한 테이블 목록:", res["searched_tables"])