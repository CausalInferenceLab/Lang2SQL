import os
import streamlit as st

from interface.core.config import get_db_connections_registry, update_db_settings


def render_sidebar_db_selector() -> None:
    st.sidebar.markdown("### DB 연결")

    registry = get_db_connections_registry()
    names = [c.name for c in registry.connections]
    if not names:
        st.sidebar.warning("등록된 DB 프로파일이 없습니다. 설정 > DB에서 추가하세요.")
        return

    # 기본 선택: 세션 또는 ENV의 DB_TYPE과 일치하는 첫 프로파일
    current_type = (
        st.session_state.get("DB_TYPE") or os.getenv("DB_TYPE") or ""
    ).lower()
    default_index = 0
    if current_type:
        for idx, c in enumerate(registry.connections):
            if c.type == current_type:
                default_index = idx
                break

    sel_name = st.sidebar.selectbox(
        "프로파일", options=names, index=default_index, key="sidebar_db_profile"
    )
    selected = next((c for c in registry.connections if c.name == sel_name), None)
    if selected is None:
        st.sidebar.error("선택한 프로파일을 찾을 수 없습니다.")
        return

    if st.sidebar.button("적용", key="sidebar_apply_db"):
        try:
            values = {
                "host": selected.host,
                "port": selected.port,
                "user": selected.user,
                "password": selected.password,
                "database": selected.database,
                "extra": selected.extra,
            }
            update_db_settings(db_type=selected.type, values=values, secrets={})
            st.sidebar.success(f"DB 적용됨: {selected.name}")
        except Exception as e:
            st.sidebar.error(f"적용 실패: {e}")
