"""
AI ChatBot 페이지
LangGraph와 OpenAI를 활용한 대화형 인터페이스
"""

import os
import streamlit as st

from utils.llm.chatbot import ChatBot
from interface.app_pages.sidebar_components import (
    render_sidebar_data_source_selector,
    render_sidebar_llm_selector,
    render_sidebar_embedding_selector,
    render_sidebar_db_selector,
    render_sidebar_chatbot_session_controller,
)
from interface.core.config import load_config

from interface.app_pages.chatbot_components import render_context_cards


def initialize_session_state():
    """세션 상태 초기화 함수

    Streamlit의 session_state를 사용하여 앱의 상태를 유지합니다.
    LLM 설정을 sidebar의 llm_selector에서 선택한 값으로부터 가져옵니다.
    """
    # 채팅 메시지 기록 저장 (자동으로 시작)
    if "chatbot_messages" not in st.session_state:
        st.session_state.chatbot_messages = []

    # LLM 공급자 확인 (현재 ChatBot은 OpenAI만 지원)
    llm_provider = (
        st.session_state.get("LLM_PROVIDER") or os.getenv("LLM_PROVIDER") or "openai"
    ).lower()

    if llm_provider != "openai":
        st.error(
            f"⚠️ ChatBot은 현재 OpenAI만 지원합니다. 설정 > LLM에서 OpenAI 프로파일을 선택하거나 LLM_PROVIDER를 'openai'로 설정해주세요."
        )
        st.stop()

    # OpenAI API 키 확인
    openai_api_key = st.session_state.get("OPEN_AI_KEY") or os.getenv("OPEN_AI_KEY")

    if not openai_api_key:
        st.error(
            "⚠️ OpenAI API 키가 설정되지 않았습니다. 설정 > LLM에서 OpenAI API 키를 입력하거나, 사이드바에서 LLM 프로파일을 적용해주세요."
        )
        st.stop()

    # 사용할 모델명 가져오기 (llm_selector에서 설정한 값)
    model_name = (
        st.session_state.get("OPEN_AI_LLM_MODEL")
        or os.getenv("OPEN_AI_LLM_MODEL")
        or "gpt-4o-mini"
    )

    # DataHub 서버 URL 가져오기 (config에서 로드)
    config = load_config()
    gms_server = config.datahub_server

    # ChatBot 인스턴스 생성 또는 모델 업데이트
    if "chatbot_instance" not in st.session_state:
        st.session_state.chatbot_instance = ChatBot(
            openai_api_key, model_name=model_name, gms_server=gms_server
        )
    else:
        # 기존 인스턴스가 있는 경우, 모델이나 API 키, gms_server가 변경되었는지 확인
        existing_bot = st.session_state.chatbot_instance
        if (
            existing_bot.model_name != model_name
            or existing_bot.openai_api_key != openai_api_key
            or existing_bot.gms_server != gms_server
        ):
            st.session_state.chatbot_instance = ChatBot(
                openai_api_key, model_name=model_name, gms_server=gms_server
            )


# 세션 상태 초기화 실행
initialize_session_state()

# 페이지 제목
st.title("🤖 AI ChatBot")

st.markdown(
    """
    LangGraph 기반 AI ChatBot과 대화를 나눌 수 있습니다.
    - 데이터베이스 테이블 정보 검색
    - 용어집 조회
    - 쿼리 예제 조회
    - 대화를 통해 질문 구체화
    """
)

# 설정 로드
config = load_config()

# 사이드바 UI 구성 (lang2sql.py와 동일한 구조)
render_sidebar_data_source_selector(config)
st.sidebar.divider()
render_sidebar_llm_selector()
st.sidebar.divider()
render_sidebar_embedding_selector()
st.sidebar.divider()
render_sidebar_db_selector()
st.sidebar.divider()

# ChatBot 전용 설정
with st.sidebar:
    st.markdown("### 🤖 ChatBot 설정")
    st.divider()
    thread_id = render_sidebar_chatbot_session_controller()


# 첫 메시지가 없으면 환영 메시지 추가
if not st.session_state.chatbot_messages:
    hello_message = "안녕하세요! 무엇을 도와드릴까요? 🤖"
    st.session_state.chatbot_messages = [
        {"role": "assistant", "content": hello_message}
    ]

# 저장된 모든 메시지를 순서대로 표시
for message in st.session_state.chatbot_messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        # 컨텍스트 정보가 있으면 렌더링
        if "context" in message:
            render_context_cards(message["context"])

# 사용자 입력 처리
if prompt := st.chat_input("메시지를 입력하세요"):
    # 사용자 메시지를 기록에 추가
    st.session_state.chatbot_messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # AI 응답 생성 및 표시
    with st.chat_message("assistant"):
        try:
            # ChatBot을 통해 응답 생성
            response = st.session_state.chatbot_instance.chat(prompt, thread_id)

            # 응답 내용 추출
            response_content = response["messages"][-1].content

            # 모델 정보 표시
            model_name = st.session_state.chatbot_instance.model_name
            st.caption(f"🤖 모델: {model_name}")

            # 응답 표시
            st.markdown(response_content)

            # 컨텍스트 데이터 추출
            context_data = {}
            if response.get("table_schema_outputs"):
                context_data["table_schema_outputs"] = response["table_schema_outputs"]
            if response.get("glossary_outputs"):
                context_data["glossary_outputs"] = response["glossary_outputs"]
            if response.get("query_example_outputs"):
                context_data["query_example_outputs"] = response[
                    "query_example_outputs"
                ]

            # 컨텍스트 카드가 있으면 렌더링
            if context_data:
                render_context_cards(context_data)

            # AI 응답을 기록에 추가
            msg_data = {"role": "assistant", "content": response_content}
            if context_data:
                msg_data["context"] = context_data

            st.session_state.chatbot_messages.append(msg_data)

        except Exception as e:
            error_msg = f"오류가 발생했습니다: {str(e)}"
            st.error(error_msg)
            st.session_state.chatbot_messages.append(
                {"role": "assistant", "content": error_msg}
            )
