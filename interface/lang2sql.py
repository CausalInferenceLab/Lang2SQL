import os
from typing import Union

import pandas as pd
import streamlit as st
from clickhouse_driver import Client
from dotenv import load_dotenv
from langchain.chains.sql_database.prompt import SQL_PROMPTS
from langchain_core.messages import HumanMessage

from llm_utils.connect_db import ConnectDB
from llm_utils.graph import builder

# Clickhouse 연결
db = ConnectDB()
db.connect_to_clickhouse()

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
st.sidebar.title("Output Settings")
st.sidebar.checkbox("Show Total Token Usage", value=True, key="show_total_token_usage")
st.sidebar.checkbox(
    "Show Result Description", value=True, key="show_result_description"
)
st.sidebar.checkbox("Show SQL", value=True, key="show_sql")
st.sidebar.checkbox(
    "Show User Question Reinterpreted by AI",
    value=True,
    key="show_question_reinterpreted_by_ai",
)
st.sidebar.checkbox(
    "Show List of Referenced Tables", value=True, key="show_referenced_tables"
)
st.sidebar.checkbox("Show Table", value=True, key="show_table")
st.sidebar.checkbox("Show Chart", value=True, key="show_chart")


# Token usage 집계 함수 정의
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
    if st.session_state.get("show_total_token_usage", True):
        st.write("총 토큰 사용량:", total_tokens)
    if st.session_state.get("show_sql", True):
        st.write("결과:", "\n\n```sql\n" + res["generated_query"] + "\n```")
    if st.session_state.get("show_result_description", True):
        st.write("결과 설명:\n\n", res["messages"][-1].content)
    if st.session_state.get("show_question_reinterpreted_by_ai", True):
        st.write("AI가 재해석한 사용자 질문:\n", res["refined_input"].content)
    if st.session_state.get("show_referenced_tables", True):
        st.write("참고한 테이블 목록:", res["searched_tables"])
    if st.session_state.get("show_table", True):
        sql = res["generated_query"]
        df = db.run_sql(sql)
        if len(df) > 10:
            st.dataframe(df.head(10))
        else:
            st.dataframe(df)
