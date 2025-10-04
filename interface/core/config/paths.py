"""레지스트리 파일 경로 계산 및 상위 디렉토리 생성 유틸리티를 제공합니다."""

import os
from pathlib import Path


def _get_registry_file_path() -> Path:
    # Allow override via env var, else default to ./config/data_sources.json
    override = os.getenv("LANG2SQL_REGISTRY_PATH")
    if override:
        return Path(override).expanduser().resolve()
    return Path(os.getcwd()) / "config" / "data_sources.json"


def _get_db_registry_file_path() -> Path:
    # Allow override via env var, else default to ./config/db_connections.json
    override = os.getenv("LANG2SQL_DB_REGISTRY_PATH")
    if override:
        return Path(override).expanduser().resolve()
    return Path(os.getcwd()) / "config" / "db_connections.json"


def _get_llm_registry_file_path() -> Path:
    override = os.getenv("LANG2SQL_LLM_REGISTRY_PATH")
    if override:
        return Path(override).expanduser().resolve()
    return Path(os.getcwd()) / "config" / "llm_profiles.json"


def _get_embedding_registry_file_path() -> Path:
    override = os.getenv("LANG2SQL_EMBEDDING_REGISTRY_PATH")
    if override:
        return Path(override).expanduser().resolve()
    return Path(os.getcwd()) / "config" / "embedding_profiles.json"


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
