"""런타임 설정 로딩/업데이트 및 세션/환경 변수 반영, 입력값 검증 유틸 포함.
DataHub/VectorDB/DB/LLM/Embedding 관련 설정 업데이트를 제공합니다.
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional

try:
    import streamlit as st  # type: ignore
except Exception:  # pragma: no cover - streamlit may not be present in non-UI contexts
    st = None  # type: ignore

from utils.llm.tools import set_gms_server

from .models import Config

DEFAULT_DATAHUB_SERVER = "http://localhost:8080"
DEFAULT_VECTORDB_TYPE = os.getenv("VECTORDB_TYPE", "faiss").lower()
DEFAULT_VECTORDB_LOCATION = os.getenv("VECTORDB_LOCATION", "")


def _get_session_value(key: str) -> str | None:
    if st is None:
        return None
    try:
        if key in st.session_state and st.session_state[key]:
            return str(st.session_state[key])
    except Exception:
        return None
    return None


def load_config() -> Config:
    """Load configuration with priority: session_state > environment > defaults."""
    datahub = _get_session_value("datahub_server") or os.getenv(
        "DATAHUB_SERVER", DEFAULT_DATAHUB_SERVER
    )
    mode = _get_session_value("data_source_mode")

    vectordb_type = _get_session_value("vectordb_type") or os.getenv(
        "VECTORDB_TYPE", DEFAULT_VECTORDB_TYPE
    )
    vectordb_location = _get_session_value("vectordb_location") or os.getenv(
        "VECTORDB_LOCATION", DEFAULT_VECTORDB_LOCATION
    )

    return Config(
        datahub_server=datahub,
        vectordb_type=vectordb_type.lower() if vectordb_type else DEFAULT_VECTORDB_TYPE,
        vectordb_location=vectordb_location,
        data_source_mode=mode,
    )


def update_datahub_server(config: Config, new_url: str) -> None:
    """Update DataHub server URL across runtime config, env-aware clients, and session."""
    if not new_url:
        return
    config.datahub_server = new_url

    # Propagate to underlying tooling/clients
    try:
        set_gms_server(new_url)
    except Exception:
        # Fail-soft: UI should surface errors from callers if needed
        pass

    # Reflect into session state for immediate UI reuse
    if st is not None:
        try:
            st.session_state["datahub_server"] = new_url
        except Exception:
            pass


def update_data_source_mode(config: Config, mode: str | None) -> None:
    """Persist user's data source selection (datahub | vectordb)."""
    config.data_source_mode = mode
    if st is not None:
        try:
            st.session_state["data_source_mode"] = mode
        except Exception:
            pass


def _put_env(key: str, value: str | None) -> None:
    if value is None:
        return
    os.environ[key] = value


def _put_session(key: str, value: str | None) -> None:
    if st is None:
        return
    try:
        st.session_state[key] = value
    except Exception:
        pass


def update_db_settings(
    *,
    db_type: str,
    values: Dict[str, Any] | None,
    secrets: Dict[str, Optional[str]] | None = None,
) -> None:
    """Update DB settings into process env and session.

    Only non-sensitive values should be passed in values and may come from registry.
    Secrets (e.g., PASSWORD, ACCESS_TOKEN) are applied to env/session but never persisted to disk.
    """
    db_type_norm = (db_type or "").lower()
    if not db_type_norm:
        raise ValueError("DB 타입이 비어 있습니다.")

    # Core selector
    _put_env("DB_TYPE", db_type_norm)
    _put_session("DB_TYPE", db_type_norm)

    prefix = db_type_norm.upper()

    base_keys = ["HOST", "PORT", "USER", "DATABASE"]
    for base_key in base_keys:
        vk = base_key.lower()
        v = (values or {}).get(vk)
        if v is None:
            continue
        _put_env(f"{prefix}_{base_key}", str(v))
        _put_session(f"{prefix}_{base_key}", str(v))

    # Extras (non-secret)
    extra = (values or {}).get("extra") or {}
    if isinstance(extra, dict):
        for k, v in extra.items():
            if v is None:
                continue
            _put_env(f"{prefix}_{str(k).upper()}", str(v))
            _put_session(f"{prefix}_{str(k).upper()}", str(v))

    # Secrets (applied to env+session, never persisted)
    for sk, sv in (secrets or {}).items():
        if sv is None:
            continue
        key_up = str(sk).upper()
        _put_env(f"{prefix}_{key_up}", str(sv))
        _put_session(f"{prefix}_{key_up}", str(sv))


def update_vectordb_settings(
    config: Config, *, vectordb_type: str, vectordb_location: str | None
) -> None:
    """Validate and update VectorDB settings into env and session.

    Basic validation rules follow CLI's behavior:
      - vectordb_type must be 'faiss' or 'pgvector' or 'qdrant'
      - if type == 'faiss' and location provided: must be an existing directory
      - if type == 'pgvector' and location provided: must start with 'postgresql://'
      - if type == 'qdrant' and location provided: must start with 'http://'
    """
    vtype = (vectordb_type or "").lower()
    if vtype not in ("faiss", "pgvector", "qdrant"):
        raise ValueError(f"지원하지 않는 VectorDB 타입: {vectordb_type}")

    vloc = vectordb_location or ""
    if vloc:
        if vtype == "faiss":
            path = Path(vloc)
            # 신규 경로 허용: 존재하면 디렉토리인지 확인, 없으면 상위 디렉토리 생성
            if path.exists() and not path.is_dir():
                raise ValueError(
                    f"유효하지 않은 FAISS 디렉토리 경로(파일 경로임): {vloc}"
                )
            if not path.exists():
                try:
                    path.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    raise ValueError(f"FAISS 경로 생성 실패: {vloc} | {e}")
        elif vtype == "pgvector":
            if not vloc.startswith("postgresql://"):
                raise ValueError("pgvector URL은 'postgresql://'로 시작해야 합니다")

    # Persist to runtime config
    config.vectordb_type = vtype
    config.vectordb_location = vloc

    # Reflect to process env for downstream modules
    os.environ["VECTORDB_TYPE"] = vtype
    if vloc:
        os.environ["VECTORDB_LOCATION"] = vloc

    # Reflect to session state for UI
    if st is not None:
        try:
            st.session_state["vectordb_type"] = vtype
            st.session_state["vectordb_location"] = vloc
        except Exception:
            pass


def update_llm_settings(*, provider: str, values: dict[str, str | None]) -> None:
    """Update chat LLM settings from UI into process env and session.

    This function mirrors the environment-variable based configuration consumed by
    utils.llm.core.factory.get_llm(). Only sets provided keys; missing values are left as-is.
    """
    provider_norm = (provider or "").lower()
    if provider_norm not in {
        "openai",
        "azure",
        "bedrock",
        "gemini",
        "ollama",
        "huggingface",
    }:
        raise ValueError(f"지원하지 않는 LLM 공급자: {provider}")

    # Core selector
    _put_env("LLM_PROVIDER", provider_norm)
    _put_session("LLM_PROVIDER", provider_norm)

    # Provider-specific fields (keys exactly as factory expects)
    for k, v in (values or {}).items():
        if v is not None:
            _put_env(k, v)
            _put_session(k, v)


def update_embedding_settings(*, provider: str, values: dict[str, str | None]) -> None:
    """Update Embeddings settings from UI into process env and session.

    Mirrors env vars consumed by utils.llm.core.factory.get_embeddings().
    """
    provider_norm = (provider or "").lower()
    if provider_norm not in {
        "openai",
        "azure",
        "bedrock",
        "gemini",
        "ollama",
        "huggingface",
    }:
        raise ValueError(f"지원하지 않는 Embedding 공급자: {provider}")

    _put_env("EMBEDDING_PROVIDER", provider_norm)
    _put_session("EMBEDDING_PROVIDER", provider_norm)

    for k, v in (values or {}).items():
        if v is not None:
            _put_env(k, v)
            _put_session(k, v)
