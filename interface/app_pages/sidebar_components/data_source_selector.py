import streamlit as st

from interface.core.config import (
    load_config,
    get_data_sources_registry,
    update_datahub_server,
    update_vectordb_settings,
    update_data_source_mode,
)


def render_sidebar_data_source_selector(config=None) -> None:
    if config is None:
        config = load_config()

    registry = get_data_sources_registry()

    st.sidebar.markdown("### 데이터 소스")

    mode_index = 0 if (config.data_source_mode or "datahub").lower() == "datahub" else 1
    selected_mode = st.sidebar.radio(
        "소스 종류", options=["DataHub", "VectorDB"], index=mode_index, horizontal=True
    )

    if selected_mode == "DataHub":
        datahub_names = [s.name for s in registry.datahub]
        if not datahub_names:
            st.sidebar.warning(
                "등록된 DataHub가 없습니다. 설정 > 데이터 소스에서 추가하세요."
            )
            return
        dh_name = st.sidebar.selectbox(
            "DataHub 인스턴스", options=datahub_names, key="sidebar_dh_select"
        )
        if st.sidebar.button("소스 적용", key="sidebar_apply_dh"):
            selected = next((s for s in registry.datahub if s.name == dh_name), None)
            if selected is None:
                st.sidebar.error("선택한 DataHub를 찾을 수 없습니다.")
                return
            try:
                update_datahub_server(config, selected.url)
                # DataHub 선택 시, VectorDB 설정이 정의되어 있으면 기본 VectorDB 로케이션으로도 반영
                if selected.vectordb_location:
                    try:
                        update_vectordb_settings(
                            config,
                            vectordb_type=selected.vectordb_type or "faiss",
                            vectordb_location=selected.vectordb_location,
                        )
                    except Exception as e:
                        st.sidebar.warning(f"VectorDB 설정 적용 경고: {e}")
                elif selected.faiss_path:
                    # Backward compatibility
                    try:
                        update_vectordb_settings(
                            config,
                            vectordb_type="faiss",
                            vectordb_location=selected.faiss_path,
                        )
                    except Exception as e:
                        st.sidebar.warning(f"FAISS 경로 적용 경고: {e}")
                update_data_source_mode(config, "datahub")
                st.sidebar.success(f"DataHub 적용됨: {selected.name}")
            except Exception as e:
                st.sidebar.error(f"적용 실패: {e}")
    else:
        vdb_names = [s.name for s in registry.vectordb]
        if not vdb_names:
            st.sidebar.warning(
                "등록된 VectorDB가 없습니다. 설정 > 데이터 소스에서 추가하세요."
            )
            return
        vdb_name = st.sidebar.selectbox(
            "VectorDB 인스턴스", options=vdb_names, key="sidebar_vdb_select"
        )
        if st.sidebar.button("소스 적용", key="sidebar_apply_vdb"):
            selected = next((s for s in registry.vectordb if s.name == vdb_name), None)
            if selected is None:
                st.sidebar.error("선택한 VectorDB를 찾을 수 없습니다.")
                return
            try:
                update_vectordb_settings(
                    config,
                    vectordb_type=selected.type,
                    vectordb_location=selected.location,
                )
                update_data_source_mode(config, "vectordb")
                st.sidebar.success(f"VectorDB 적용됨: {selected.name}")
            except Exception as e:
                st.sidebar.error(f"적용 실패: {e}")
