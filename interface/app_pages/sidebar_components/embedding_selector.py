import os
import streamlit as st

from interface.core.config import (
    update_embedding_settings,
    get_embedding_registry,
)


def render_sidebar_embedding_selector() -> None:
    st.sidebar.markdown("### Embeddings 선택")

    e_reg = get_embedding_registry()
    if not e_reg.profiles:
        st.sidebar.info(
            "저장된 Embeddings 프로파일이 없습니다. 설정 > LLM에서 저장하세요."
        )
        # fallback: 간단 공급자 선택 유지
        default_emb = (
            (
                st.session_state.get("EMBEDDING_PROVIDER")
                or os.getenv("EMBEDDING_PROVIDER")
                or "openai"
            )
        ).lower()
        selected = st.sidebar.selectbox(
            "Embeddings 공급자",
            options=["openai", "azure", "bedrock", "gemini", "ollama", "huggingface"],
            index=(
                ["openai", "azure", "bedrock", "gemini", "ollama", "huggingface"].index(
                    default_emb
                )
                if default_emb
                in {"openai", "azure", "bedrock", "gemini", "ollama", "huggingface"}
                else 0
            ),
            key="sidebar_embedding_provider_fallback",
        )
        if selected != default_emb:
            try:
                update_embedding_settings(provider=selected, values={})
                st.sidebar.success(
                    f"Embeddings 공급자가 '{selected}'로 변경되었습니다."
                )
            except Exception as e:
                st.sidebar.error(f"Embeddings 공급자 변경 실패: {e}")
        return

    e_names = [p.name for p in e_reg.profiles]
    current_emb_provider = (
        st.session_state.get("EMBEDDING_PROVIDER")
        or os.getenv("EMBEDDING_PROVIDER")
        or ""
    ).lower()
    e_default_index = 0
    if current_emb_provider:
        for idx, p in enumerate(e_reg.profiles):
            if p.provider == current_emb_provider:
                e_default_index = idx
                break

    e_sel_name = st.sidebar.selectbox(
        "Embeddings 프로파일",
        options=e_names,
        index=e_default_index,
        key="sidebar_embedding_profile",
    )

    e_selected = next((p for p in e_reg.profiles if p.name == e_sel_name), None)
    if e_selected is None:
        st.sidebar.error("선택한 Embeddings 프로파일을 찾을 수 없습니다.")
        return

    if st.sidebar.button("Embeddings 적용", key="sidebar_apply_embedding_profile"):
        try:
            update_embedding_settings(
                provider=e_selected.provider, values=e_selected.fields
            )
            st.sidebar.success(f"Embeddings 프로파일 적용됨: {e_selected.name}")
        except Exception as e:
            st.sidebar.error(f"Embeddings 프로파일 적용 실패: {e}")
