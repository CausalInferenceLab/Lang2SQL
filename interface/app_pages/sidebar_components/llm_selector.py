import os
import streamlit as st

from interface.core.config import (
    update_llm_settings,
    get_llm_registry,
)


def render_sidebar_llm_selector() -> None:
    st.sidebar.markdown("### LLM 선택")

    reg = get_llm_registry()
    if not reg.profiles:
        st.sidebar.info(
            "저장된 LLM 프로파일이 없습니다. 설정 > LLM에서 프로파일을 저장하세요."
        )
        # 기존 방식 fallback
        default_llm = (
            (
                st.session_state.get("LLM_PROVIDER")
                or os.getenv("LLM_PROVIDER")
                or "openai"
            )
        ).lower()
        selected_provider = st.sidebar.selectbox(
            "LLM 공급자",
            options=["openai", "azure", "bedrock", "gemini", "ollama", "huggingface"],
            index=(
                ["openai", "azure", "bedrock", "gemini", "ollama", "huggingface"].index(
                    default_llm
                )
                if default_llm
                in {"openai", "azure", "bedrock", "gemini", "ollama", "huggingface"}
                else 0
            ),
            key="sidebar_llm_provider_fallback",
        )
        if selected_provider != default_llm:
            try:
                update_llm_settings(provider=selected_provider, values={})
                st.sidebar.success(
                    f"LLM 공급자가 '{selected_provider}'로 변경되었습니다."
                )
            except Exception as e:
                st.sidebar.error(f"LLM 공급자 변경 실패: {e}")
        return

    names = [p.name for p in reg.profiles]
    # 기본 선택: 세션의 LLM_PROVIDER와 같은 provider를 가진 첫 프로파일
    current_provider = (
        st.session_state.get("LLM_PROVIDER") or os.getenv("LLM_PROVIDER") or ""
    ).lower()
    default_index = 0
    if current_provider:
        for idx, p in enumerate(reg.profiles):
            if p.provider == current_provider:
                default_index = idx
                break

    sel_name = st.sidebar.selectbox(
        "LLM 프로파일", options=names, index=default_index, key="sidebar_llm_profile"
    )
    selected = next((p for p in reg.profiles if p.name == sel_name), None)
    if selected is None:
        st.sidebar.error("선택한 LLM 프로파일을 찾을 수 없습니다.")
        return

    if st.sidebar.button("적용", key="sidebar_apply_llm_profile"):
        try:
            # provider 설정 + 프로파일의 비민감 필드만 적용
            update_llm_settings(provider=selected.provider, values=selected.fields)
            st.sidebar.success(f"LLM 프로파일 적용됨: {selected.name}")
        except Exception as e:
            st.sidebar.error(f"LLM 프로파일 적용 실패: {e}")

    # Embeddings 관련 UI는 embedding_selector.py에서 처리
