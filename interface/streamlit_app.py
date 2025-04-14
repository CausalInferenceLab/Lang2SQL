import os
import sys
# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import streamlit as st
from langchain_core.messages import HumanMessage
from llm_utils.graph import builder
from RAG.agentic.graph import RAGGraph  # RAG 디렉토리를 찾을 수 있어야 함


# Streamlit 앱 제목
st.title("Lang2SQL")

# 사용자 입력 받기
user_query = st.text_area(
    "쿼리를 입력하세요:",
    value="고객 데이터를 기반으로 유니크한 유저 수를 카운트하는 쿼리",
)

user_database_env = st.text_area(
    "db 환경정보를 입력하세요:",
    value="duckdb",
)


# Token usage 집계 함수 정의
def summarize_total_tokens(data):
    total_tokens = 0
    for item in data:
        token_usage = getattr(item, "usage_metadata", {})
        total_tokens += token_usage.get("total_tokens", 0)
    return total_tokens


# 버튼 클릭 시 실행
if st.button("쿼리 실행"):
    # 그래프 컴파일 (RAGGraph 사용)
    if 'rag_graph' not in st.session_state:
        st.session_state.rag_graph = RAGGraph()
    
    graph = st.session_state.rag_graph.get_graph()  # builder.compile() 대신 사용

    # 원래 코드와 유사한 형태로 호출
    # 하지만 RAGGraph의 입력 형식에 맞게 조정
    input_message = HumanMessage(content=user_query)
    
    # RAGGraph는 문자열 형태의 질문만 받으므로 간소화
    result = st.session_state.rag_graph.invoke(user_query)
    
    # 결과 형식을 원래 코드의 예상 형식으로 변환
    res = {
        "messages": [input_message],  # 토큰 계산용
        "generated_query": {"content": result.get("answer", "")},
        "refined_input": {"content": user_query},
        "searched_tables": []  # 실제 테이블 정보가 있다면 여기에 추가
    }
    
    total_tokens = summarize_total_tokens(res["messages"])

    # 결과 출력 (원래 형식 유지)
    st.write("총 토큰 사용량:", total_tokens)
    st.write("결과:", res["generated_query"]["content"])
    st.write("AI가 재해석한 사용자 질문:\n", res["refined_input"]["content"])
    st.write("참고한 테이블 목록:", res["searched_tables"])
    
    # 추가 정보 표시 (선택 사항)
    with st.expander("자세한 정보"):
        if "context" in result:
            st.write("참조 컨텍스트:", result["context"])
