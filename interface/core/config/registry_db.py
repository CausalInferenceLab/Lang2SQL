"""DB 연결 프로파일 레지스트리를 세션+디스크에 관리하는 모듈입니다.
get/add/update/delete 연산과 Streamlit 세션 연동을 제공합니다.
"""

from typing import Any, Dict, Optional

try:
    import streamlit as st  # type: ignore
except Exception:  # pragma: no cover
    st = None  # type: ignore

from .models import DBConnectionsRegistry, DBConnectionProfile
from .persist import load_db_registry_from_disk, save_db_registry_to_disk


def get_db_connections_registry() -> DBConnectionsRegistry:
    if st is not None and "db_connections_registry" in st.session_state:
        reg = st.session_state["db_connections_registry"]
        return reg  # stored as DBConnectionsRegistry
    try:
        registry = load_db_registry_from_disk()
    except Exception:
        registry = DBConnectionsRegistry()
    if st is not None:
        st.session_state["db_connections_registry"] = registry
    return registry


def _save_db_registry(registry: DBConnectionsRegistry) -> None:
    if st is not None:
        st.session_state["db_connections_registry"] = registry
    try:
        save_db_registry_to_disk(registry)
    except Exception:
        # fail-soft; UI will still have session copy
        pass


def add_db_connection(
    *,
    name: str,
    db_type: str,
    host: Optional[str] = None,
    port: Optional[int] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
    note: Optional[str] = None,
) -> None:
    db_type_norm = (db_type or "").lower()
    registry = get_db_connections_registry()
    if any(c.name == name for c in registry.connections):
        raise ValueError(f"이미 존재하는 DB 프로파일 이름입니다: {name}")
    registry.connections.append(
        DBConnectionProfile(
            name=name,
            type=db_type_norm,
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            extra=extra or None,
            note=note or None,
        )
    )
    _save_db_registry(registry)


def update_db_connection(
    *,
    name: str,
    db_type: str,
    host: Optional[str],
    port: Optional[int],
    user: Optional[str],
    password: Optional[str],
    database: Optional[str],
    extra: Optional[Dict[str, Any]],
    note: Optional[str],
) -> None:
    db_type_norm = (db_type or "").lower()
    registry = get_db_connections_registry()
    for idx, c in enumerate(registry.connections):
        if c.name == name:
            registry.connections[idx] = DBConnectionProfile(
                name=name,
                type=db_type_norm,
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                extra=extra or None,
                note=note or None,
            )
            _save_db_registry(registry)
            return
    raise ValueError(f"존재하지 않는 DB 프로파일 이름입니다: {name}")


def delete_db_connection(*, name: str) -> None:
    registry = get_db_connections_registry()
    registry.connections = [c for c in registry.connections if c.name != name]
    _save_db_registry(registry)
