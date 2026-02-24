import os
import streamlit as st

from interface.core.config import (
    update_llm_settings,
    update_embedding_settings,
    Config,
    load_config,
    save_llm_profile,
    get_llm_registry,
    save_embedding_profile,
    get_embedding_registry,
)

LLM_PROVIDERS = [
    "openai",
    "azure",
    "bedrock",
    "gemini",
    "ollama",
    "huggingface",
]


def _llm_fields(provider: str) -> list[tuple[str, str, bool]]:
    """Return list of (label, env_key, is_secret) for LLM provider."""
    p = provider.lower()
    if p == "openai":
        return [
            ("Model", "OPEN_AI_LLM_MODEL", False),
            ("API Key", "OPEN_AI_KEY", True),
        ]
    if p == "azure":
        return [
            ("Endpoint", "AZURE_OPENAI_LLM_ENDPOINT", False),
            ("Deployment(Model)", "AZURE_OPENAI_LLM_MODEL", False),
            ("API Version", "AZURE_OPENAI_LLM_API_VERSION", False),
            ("API Key", "AZURE_OPENAI_LLM_KEY", True),
        ]
    if p == "bedrock":
        return [
            ("Model", "AWS_BEDROCK_LLM_MODEL", False),
            ("Access Key ID", "AWS_BEDROCK_LLM_ACCESS_KEY_ID", True),
            ("Secret Access Key", "AWS_BEDROCK_LLM_SECRET_ACCESS_KEY", True),
            ("Region", "AWS_BEDROCK_LLM_REGION", False),
        ]
    if p == "gemini":
        return [
            ("Model", "GEMINI_LLM_MODEL", False),
            # ChatGoogleGenerativeAI uses GOOGLE_API_KEY at process level, but factory currently reads only model
        ]
    if p == "ollama":
        return [
            ("Model", "OLLAMA_LLM_MODEL", False),
            ("Base URL", "OLLAMA_LLM_BASE_URL", False),
        ]
    if p == "huggingface":
        return [
            ("Endpoint URL", "HUGGING_FACE_LLM_ENDPOINT", False),
            ("Repo ID", "HUGGING_FACE_LLM_REPO_ID", False),
            ("Model", "HUGGING_FACE_LLM_MODEL", False),
            ("API Token", "HUGGING_FACE_LLM_API_TOKEN", True),
        ]
    return []


def _embedding_fields(provider: str) -> list[tuple[str, str, bool]]:
    p = provider.lower()
    if p == "openai":
        return [
            ("Model", "OPEN_AI_EMBEDDING_MODEL", False),
            ("API Key", "OPEN_AI_KEY", True),
        ]
    if p == "azure":
        return [
            ("Endpoint", "AZURE_OPENAI_EMBEDDING_ENDPOINT", False),
            ("Deployment(Model)", "AZURE_OPENAI_EMBEDDING_MODEL", False),
            ("API Version", "AZURE_OPENAI_EMBEDDING_API_VERSION", False),
            ("API Key", "AZURE_OPENAI_EMBEDDING_KEY", True),
        ]
    if p == "bedrock":
        return [
            ("Model", "AWS_BEDROCK_EMBEDDING_MODEL", False),
            ("Access Key ID", "AWS_BEDROCK_EMBEDDING_ACCESS_KEY_ID", True),
            ("Secret Access Key", "AWS_BEDROCK_EMBEDDING_SECRET_ACCESS_KEY", True),
            ("Region", "AWS_BEDROCK_EMBEDDING_REGION", False),
        ]
    if p == "gemini":
        return [
            ("Model", "GEMINI_EMBEDDING_MODEL", False),
            ("API Key", "GEMINI_EMBEDDING_KEY", True),
        ]
    if p == "ollama":
        return [
            ("Model", "OLLAMA_EMBEDDING_MODEL", False),
            ("Base URL", "OLLAMA_EMBEDDING_BASE_URL", False),
        ]
    if p == "huggingface":
        return [
            ("Model", "HUGGING_FACE_EMBEDDING_MODEL", False),
            ("Repo ID", "HUGGING_FACE_EMBEDDING_REPO_ID", False),
            ("API Token", "HUGGING_FACE_EMBEDDING_API_TOKEN", True),
        ]
    return []


def render_llm_section(config: Config | None = None) -> None:
    st.subheader("LLM 설정")

    if config is None:
        try:
            config = load_config()
        except Exception:
            config = None  # UI 일관성을 위한 옵셔널 처리

    llm_col, emb_col = st.columns(2)

    with llm_col:
        st.markdown("**Chat LLM**")
        default_llm_provider = (
            (
                st.session_state.get("LLM_PROVIDER")
                or os.getenv("LLM_PROVIDER")
                or "openai"
            )
        ).lower()
        try:
            default_llm_index = LLM_PROVIDERS.index(default_llm_provider)
        except ValueError:
            default_llm_index = 0
        provider = st.selectbox(
            "공급자",
            options=LLM_PROVIDERS,
            index=default_llm_index,
            key="llm_provider",
        )
        fields = _llm_fields(provider)
        values: dict[str, str | None] = {}
        non_secret_values: dict[str, str | None] = {}
        for label, env_key, is_secret in fields:
            prefill = st.session_state.get(env_key) or os.getenv(env_key) or ""
            if is_secret:
                values[env_key] = st.text_input(
                    label, value=prefill, type="password", key=f"llm_{env_key}"
                )
            else:
                v = st.text_input(label, value=prefill, key=f"llm_{env_key}")
                values[env_key] = v
                non_secret_values[env_key] = v

        # 메시지 영역
        llm_msg = st.empty()

        st.markdown("**프로파일 저장 (비밀키 제외)**")
        with st.form("llm_profile_save_form"):
            prof_cols = st.columns([2, 2])
            with prof_cols[0]:
                profile_name = st.text_input("프로파일 이름", key="llm_profile_name")
            with prof_cols[1]:
                profile_note = st.text_input("메모(선택)", key="llm_profile_note")

            submitted = st.form_submit_button("프로파일 저장")
            if submitted:
                try:
                    if not profile_name:
                        llm_msg.warning("프로파일 이름을 입력하세요.")
                    else:
                        # 1) 환경/세션에 즉시 적용 (저장된 모든 값 사용: 사용자 요청)
                        update_llm_settings(provider=provider, values=values)
                        # 2) 디스크에 프로파일 저장 (비밀키 포함)
                        save_llm_profile(
                            name=profile_name,
                            provider=provider,
                            values=values,
                            note=(profile_note or None),
                        )
                        llm_msg.success("프로파일이 저장 및 적용되었습니다.")
                except Exception as e:
                    llm_msg.error(f"프로파일 저장 실패: {e}")

        # 저장된 프로파일 미리보기
        reg = get_llm_registry()
        if reg.profiles:
            with st.expander("저장된 LLM 프로파일", expanded=False):
                for p in reg.profiles:
                    if p.fields:
                        pairs = [
                            f"{k}={p.fields.get(k, '')}"
                            for k in sorted(p.fields.keys())
                        ]
                        fields_text = ", ".join(pairs)
                    else:
                        fields_text = "-"
                    note_text = f" | note: {p.note}" if getattr(p, "note", None) else ""
                    st.caption(f"- {p.name} ({p.provider}) | {fields_text}{note_text}")

    with emb_col:
        st.markdown("**Embeddings**")
        default_emb_provider = (
            (
                st.session_state.get("EMBEDDING_PROVIDER")
                or os.getenv("EMBEDDING_PROVIDER")
                or "openai"
            )
        ).lower()
        try:
            default_emb_index = LLM_PROVIDERS.index(default_emb_provider)
        except ValueError:
            default_emb_index = 0
        e_provider = st.selectbox(
            "공급자",
            options=LLM_PROVIDERS,
            index=default_emb_index,
            key="embedding_provider",
        )
        e_fields = _embedding_fields(e_provider)
        e_values: dict[str, str | None] = {}
        for label, env_key, is_secret in e_fields:
            prefill = st.session_state.get(env_key) or os.getenv(env_key) or ""
            if is_secret:
                e_values[env_key] = st.text_input(
                    label, value=prefill, type="password", key=f"emb_{env_key}"
                )
            else:
                e_values[env_key] = st.text_input(
                    label, value=prefill, key=f"emb_{env_key}"
                )

        # 메시지 영역: 버튼 컬럼 밖(섹션 폭)
        emb_msg = st.empty()

        st.markdown("**Embeddings 프로파일 저장 (시크릿 포함)**")
        with st.form("embedding_profile_save_form"):
            e_prof_cols = st.columns([2, 2])
            with e_prof_cols[0]:
                e_profile_name = st.text_input(
                    "프로파일 이름", key="embedding_profile_name"
                )
            with e_prof_cols[1]:
                e_profile_note = st.text_input(
                    "메모(선택)", key="embedding_profile_note"
                )

            e_submitted = st.form_submit_button("프로파일 저장")
            if e_submitted:
                try:
                    if not e_profile_name:
                        emb_msg.warning("프로파일 이름을 입력하세요.")
                    else:
                        update_embedding_settings(provider=e_provider, values=e_values)
                        save_embedding_profile(
                            name=e_profile_name,
                            provider=e_provider,
                            values=e_values,
                            note=(e_profile_note or None),
                        )
                        emb_msg.success("Embeddings 프로파일이 저장 및 적용되었습니다.")
                except Exception as e:
                    emb_msg.error(f"프로파일 저장 실패: {e}")
        e_reg = get_embedding_registry()
        if e_reg.profiles:
            with st.expander("저장된 Embeddings 프로파일", expanded=False):
                for p in e_reg.profiles:
                    if p.fields:
                        pairs = [
                            f"{k}={p.fields.get(k, '')}"
                            for k in sorted(p.fields.keys())
                        ]
                        fields_text = ", ".join(pairs)
                    else:
                        fields_text = "-"
                    note_text = f" | note: {p.note}" if getattr(p, "note", None) else ""
                    st.caption(f"- {p.name} ({p.provider}) | {fields_text}{note_text}")
