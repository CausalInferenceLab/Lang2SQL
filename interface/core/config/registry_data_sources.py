"""DataHub/VectorDB 소스 레지스트리를 세션+디스크에 관리하는 모듈입니다.
get/add/update/delete 연산과 Streamlit 세션 연동을 제공합니다.
"""

from typing import Optional

try:
    import streamlit as st  # type: ignore
except Exception:  # pragma: no cover
    st = None  # type: ignore

from .models import DataSourcesRegistry, DataHubSource, VectorDBSource
from .persist import (
    load_registry_from_disk,
    save_registry_to_disk,
)


def get_data_sources_registry() -> DataSourcesRegistry:
    if st is not None and "data_sources_registry" in st.session_state:
        reg = st.session_state["data_sources_registry"]
        return reg  # stored as DataSourcesRegistry
    # Try load from disk
    try:
        registry = load_registry_from_disk()
    except Exception:
        registry = DataSourcesRegistry()
    if st is not None:
        st.session_state["data_sources_registry"] = registry
    return registry


def _save_registry(registry: DataSourcesRegistry) -> None:
    if st is not None:
        st.session_state["data_sources_registry"] = registry
    try:
        save_registry_to_disk(registry)
    except Exception:
        # fail-soft; UI will still have session copy
        pass


def add_datahub_source(
    *, name: str, url: str, faiss_path: Optional[str] = None, note: Optional[str] = None
) -> None:
    registry = get_data_sources_registry()
    if any(s.name == name for s in registry.datahub):
        raise ValueError(f"이미 존재하는 DataHub 이름입니다: {name}")
    registry.datahub.append(
        DataHubSource(name=name, url=url, faiss_path=faiss_path, note=note)
    )
    _save_registry(registry)


def update_datahub_source(
    *, name: str, url: str, faiss_path: Optional[str], note: Optional[str]
) -> None:
    registry = get_data_sources_registry()
    for idx, s in enumerate(registry.datahub):
        if s.name == name:
            registry.datahub[idx] = DataHubSource(
                name=name, url=url, faiss_path=faiss_path, note=note
            )
            _save_registry(registry)
            return
    raise ValueError(f"존재하지 않는 DataHub 이름입니다: {name}")


def delete_datahub_source(*, name: str) -> None:
    registry = get_data_sources_registry()
    registry.datahub = [s for s in registry.datahub if s.name != name]
    _save_registry(registry)


def add_vectordb_source(
    *,
    name: str,
    vtype: str,
    location: str,
    collection_prefix: Optional[str] = None,
    note: Optional[str] = None,
) -> None:
    vtype = (vtype or "").lower()
    if vtype not in ("faiss", "pgvector", "qdrant"):
        raise ValueError("VectorDB 타입은 'faiss' 또는 'pgvector'여야 합니다")
    registry = get_data_sources_registry()
    if any(s.name == name for s in registry.vectordb):
        raise ValueError(f"이미 존재하는 VectorDB 이름입니다: {name}")
    registry.vectordb.append(
        VectorDBSource(
            name=name,
            type=vtype,
            location=location,
            collection_prefix=collection_prefix,
            note=note,
        )
    )
    _save_registry(registry)


def update_vectordb_source(
    *,
    name: str,
    vtype: str,
    location: str,
    collection_prefix: Optional[str],
    note: Optional[str],
) -> None:
    vtype = (vtype or "").lower()
    if vtype not in ("faiss", "pgvector", "qdrant"):
        raise ValueError("VectorDB 타입은 'faiss' 또는 'pgvector'여야 합니다")
    registry = get_data_sources_registry()
    for idx, s in enumerate(registry.vectordb):
        if s.name == name:
            registry.vectordb[idx] = VectorDBSource(
                name=name,
                type=vtype,
                location=location,
                collection_prefix=collection_prefix,
                note=note,
            )
            _save_registry(registry)
            return
    raise ValueError(f"존재하지 않는 VectorDB 이름입니다: {name}")


def delete_vectordb_source(*, name: str) -> None:
    registry = get_data_sources_registry()
    registry.vectordb = [s for s in registry.vectordb if s.name != name]
    _save_registry(registry)
