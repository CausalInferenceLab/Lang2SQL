"""LLM/Embedding 프로파일 레지스트리를 세션+디스크에 관리하는 모듈입니다.
프로파일 저장(upsert)과 Streamlit 세션 연동을 제공합니다.
"""

try:
    import streamlit as st  # type: ignore
except Exception:  # pragma: no cover
    st = None  # type: ignore

from .models import (
    LLMRegistry,
    LLMProfile,
    EmbeddingRegistry,
    EmbeddingProfile,
)
from .persist import (
    load_llm_registry_from_disk,
    save_llm_registry_to_disk,
    load_embedding_registry_from_disk,
    save_embedding_registry_to_disk,
)


def get_llm_registry() -> LLMRegistry:
    if st is not None and "llm_registry" in st.session_state:
        return st.session_state["llm_registry"]
    try:
        registry = load_llm_registry_from_disk()
    except Exception:
        registry = LLMRegistry()
    if st is not None:
        st.session_state["llm_registry"] = registry
    return registry


def _save_llm_registry(registry: LLMRegistry) -> None:
    if st is not None:
        st.session_state["llm_registry"] = registry
    try:
        save_llm_registry_to_disk(registry)
    except Exception:
        pass


def get_embedding_registry() -> EmbeddingRegistry:
    if st is not None and "embedding_registry" in st.session_state:
        return st.session_state["embedding_registry"]
    try:
        registry = load_embedding_registry_from_disk()
    except Exception:
        registry = EmbeddingRegistry()
    if st is not None:
        st.session_state["embedding_registry"] = registry
    return registry


def _save_embedding_registry(registry: EmbeddingRegistry) -> None:
    if st is not None:
        st.session_state["embedding_registry"] = registry
    try:
        save_embedding_registry_to_disk(registry)
    except Exception:
        pass


def save_llm_profile(
    *, name: str, provider: str, values: dict[str, str | None], note: str | None = None
) -> None:
    provider_norm = (provider or "").lower()
    stored_fields: dict[str, str] = {}
    for k, v in (values or {}).items():
        if v is None:
            continue
        stored_fields[k] = str(v)

    reg = get_llm_registry()
    # upsert by name
    for idx, p in enumerate(reg.profiles):
        if p.name == name:
            reg.profiles[idx] = LLMProfile(
                name=name, provider=provider_norm, fields=stored_fields, note=note
            )
            _save_llm_registry(reg)
            return
    reg.profiles.append(
        LLMProfile(name=name, provider=provider_norm, fields=stored_fields, note=note)
    )
    _save_llm_registry(reg)


def save_embedding_profile(
    *, name: str, provider: str, values: dict[str, str | None], note: str | None = None
) -> None:
    provider_norm = (provider or "").lower()
    stored_fields: dict[str, str] = {}
    for k, v in (values or {}).items():
        if v is None:
            continue
        stored_fields[k] = str(v)

    reg = get_embedding_registry()
    for idx, p in enumerate(reg.profiles):
        if p.name == name:
            reg.profiles[idx] = EmbeddingProfile(
                name=name, provider=provider_norm, fields=stored_fields, note=note
            )
            _save_embedding_registry(reg)
            return
    reg.profiles.append(
        EmbeddingProfile(
            name=name, provider=provider_norm, fields=stored_fields, note=note
        )
    )
    _save_embedding_registry(reg)
