"""레지스트리 직렬화/역직렬화와 디스크 저장/로드 로직을 제공합니다."""

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List

from .models import (
    DataSourcesRegistry,
    DataHubSource,
    VectorDBSource,
    DBConnectionsRegistry,
    DBConnectionProfile,
    LLMRegistry,
    LLMProfile,
    EmbeddingRegistry,
    EmbeddingProfile,
)
from .paths import (
    _get_registry_file_path,
    _get_db_registry_file_path,
    _get_llm_registry_file_path,
    _get_embedding_registry_file_path,
    _ensure_parent_dir,
)


def save_registry_to_disk(registry: DataSourcesRegistry) -> None:
    path = _get_registry_file_path()
    _ensure_parent_dir(path)
    payload = asdict(registry)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def save_db_registry_to_disk(registry: DBConnectionsRegistry) -> None:
    path = _get_db_registry_file_path()
    _ensure_parent_dir(path)
    payload = asdict(registry)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def save_llm_registry_to_disk(registry: LLMRegistry) -> None:
    path = _get_llm_registry_file_path()
    _ensure_parent_dir(path)
    payload = asdict(registry)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def save_embedding_registry_to_disk(registry: EmbeddingRegistry) -> None:
    path = _get_embedding_registry_file_path()
    _ensure_parent_dir(path)
    payload = asdict(registry)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _parse_datahub_list(items: List[Dict[str, Any]]) -> List[DataHubSource]:
    parsed: List[DataHubSource] = []
    for item in items or []:
        name = str(item.get("name", "")).strip()
        url = str(item.get("url", "")).strip()
        faiss_path = item.get("faiss_path")
        note = item.get("note")
        if not name or not url:
            continue
        parsed.append(
            DataHubSource(name=name, url=url, faiss_path=faiss_path, note=note)
        )
    return parsed


def _parse_vectordb_list(items: List[Dict[str, Any]]) -> List[VectorDBSource]:
    parsed: List[VectorDBSource] = []
    for item in items or []:
        name = str(item.get("name", "")).strip()
        vtype = str(item.get("type", "")).strip().lower()
        location = str(item.get("location", "")).strip()
        if not name or not vtype or not location:
            continue
        collection_prefix = item.get("collection_prefix")
        note = item.get("note")
        parsed.append(
            VectorDBSource(
                name=name,
                type=vtype,
                location=location,
                collection_prefix=collection_prefix,
                note=note,
            )
        )
    return parsed


def load_registry_from_disk() -> DataSourcesRegistry:
    path = _get_registry_file_path()
    if not path.exists():
        return DataSourcesRegistry()
    with path.open("r", encoding="utf-8") as f:
        data: Dict[str, Any] = json.load(f)
    return DataSourcesRegistry(
        datahub=_parse_datahub_list(data.get("datahub", [])),
        vectordb=_parse_vectordb_list(data.get("vectordb", [])),
    )


def _parse_db_conn_list(items: List[Dict[str, Any]]) -> List[DBConnectionProfile]:
    parsed: List[DBConnectionProfile] = []
    for item in items or []:
        name = str(item.get("name", "")).strip()
        db_type = str(item.get("type", "")).strip().lower()
        if not name or not db_type:
            continue
        host = item.get("host")
        port = item.get("port")
        try:
            port = int(port) if port is not None else None
        except Exception:
            port = None
        user = item.get("user")
        password = item.get("password")
        database = item.get("database")
        extra = item.get("extra") or None
        note = item.get("note") or None
        parsed.append(
            DBConnectionProfile(
                name=name,
                type=db_type,
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                extra=extra,
                note=note,
            )
        )
    return parsed


def load_db_registry_from_disk() -> DBConnectionsRegistry:
    path = _get_db_registry_file_path()
    if not path.exists():
        return DBConnectionsRegistry()
    with path.open("r", encoding="utf-8") as f:
        data: Dict[str, Any] = json.load(f)
    return DBConnectionsRegistry(
        connections=_parse_db_conn_list(data.get("connections", []))
    )


def _parse_llm_profiles(items: List[Dict[str, Any]]) -> List[LLMProfile]:
    parsed: List[LLMProfile] = []
    for item in items or []:
        name = str(item.get("name", "")).strip()
        provider = str(item.get("provider", "")).strip().lower()
        if not name or not provider:
            continue
        fields = item.get("fields") or {}
        note = item.get("note") or None
        if not isinstance(fields, dict):
            fields = {}
        parsed.append(
            LLMProfile(name=name, provider=provider, fields=fields, note=note)
        )
    return parsed


def load_llm_registry_from_disk() -> LLMRegistry:
    path = _get_llm_registry_file_path()
    if not path.exists():
        return LLMRegistry()
    with path.open("r", encoding="utf-8") as f:
        data: Dict[str, Any] = json.load(f)
    return LLMRegistry(profiles=_parse_llm_profiles(data.get("profiles", [])))


def _parse_embedding_profiles(items: List[Dict[str, Any]]) -> List[EmbeddingProfile]:
    parsed: List[EmbeddingProfile] = []
    for item in items or []:
        name = str(item.get("name", "")).strip()
        provider = str(item.get("provider", "")).strip().lower()
        if not name or not provider:
            continue
        fields = item.get("fields") or {}
        note = item.get("note") or None
        if not isinstance(fields, dict):
            fields = {}
        parsed.append(
            EmbeddingProfile(name=name, provider=provider, fields=fields, note=note)
        )
    return parsed


def load_embedding_registry_from_disk() -> EmbeddingRegistry:
    path = _get_embedding_registry_file_path()
    if not path.exists():
        return EmbeddingRegistry()
    with path.open("r", encoding="utf-8") as f:
        data: Dict[str, Any] = json.load(f)
    return EmbeddingRegistry(
        profiles=_parse_embedding_profiles(data.get("profiles", []))
    )
