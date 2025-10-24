import streamlit as st
from interface.core.config import (
    Config,
    load_config,
    update_vectordb_settings,
    update_data_source_mode,
    get_data_sources_registry,
    add_datahub_source,
    update_datahub_source,
    delete_datahub_source,
    add_vectordb_source,
    update_vectordb_source,
    delete_vectordb_source,
)
from infra.monitoring.check_server import CheckServer


def _render_status_banner(config: Config) -> None:
    mode = config.data_source_mode
    ready_msgs = []

    if mode == "datahub":
        last_health = st.session_state.get("datahub_last_health")
        if last_health is True:
            st.success(f"데이터 소스 준비됨: DataHub ({config.datahub_server})")
        elif last_health is False:
            st.warning(
                "DataHub 헬스 체크 실패. URL을 확인하거나 VectorDB로 전환하세요."
            )
        else:
            st.info("DataHub 상태 미검증 – 헬스 체크 버튼으로 확인하세요.")
    elif mode == "vectordb":
        if config.vectordb_type and (
            (config.vectordb_type == "faiss" and config.vectordb_location)
            or (config.vectordb_type == "pgvector" and config.vectordb_location)
        ):
            st.success(
                f"데이터 소스 준비됨: VectorDB ({config.vectordb_type}, {config.vectordb_location or '기본값'})"
            )
        else:
            st.warning("VectorDB 설정이 불완전합니다. 타입/위치를 확인하세요.")
    else:
        st.info(
            "데이터 소스를 선택해주세요: DataHub 또는 VectorDB 중 하나는 필수입니다."
        )


def render_data_source_section(config: Config | None = None) -> None:
    st.subheader("데이터 소스 (필수)")

    if config is None:
        config = load_config()

    _render_status_banner(config)

    # 선택 스위치
    col = st.columns([1, 3])[0]
    with col:
        mode = st.radio(
            "데이터 소스 선택",
            options=["DataHub", "VectorDB"],
            horizontal=True,
            index=(
                0 if (config.data_source_mode or "datahub").lower() == "datahub" else 1
            ),
        )
    selected = mode.lower()
    update_data_source_mode(config, selected)

    st.divider()

    registry = get_data_sources_registry()

    if selected == "datahub":
        with st.container(border=True):
            st.write("등록된 DataHub")
            for source in list(registry.datahub):
                cols = st.columns([2, 4, 2, 1, 1])
                with cols[0]:
                    st.text(source.name)
                with cols[1]:
                    st.text(source.url)
                with cols[2]:
                    note_val = source.note or ""
                    st.caption(note_val)
                with cols[3]:
                    if st.button("편집", key=f"edit_dh_{source.name}"):
                        st.session_state["edit_dh_name"] = source.name
                with cols[4]:
                    if st.button("삭제", type="secondary", key=f"del_dh_{source.name}"):
                        delete_datahub_source(name=source.name)
                        st.rerun()

            # 편집 폼
            edit_dh = st.session_state.get("edit_dh_name")
            if edit_dh:
                st.divider()
                st.write(f"DataHub 편집: {edit_dh}")
                existing = next(
                    (s for s in registry.datahub if s.name == edit_dh), None
                )
                if existing:
                    new_url = st.text_input(
                        "URL", value=existing.url, key="dh_edit_url"
                    )
                    new_faiss = st.text_input(
                        "FAISS 저장 경로(선택)",
                        value=existing.faiss_path or "",
                        key="dh_edit_faiss",
                    )
                    new_note = st.text_input(
                        "메모", value=existing.note or "", key="dh_edit_note"
                    )
                    cols = st.columns([1, 1, 2])
                    with cols[0]:
                        if st.button("헬스 체크", key="dh_edit_health"):
                            ok = CheckServer.is_gms_server_healthy(url=new_url)
                            st.session_state["datahub_last_health"] = bool(ok)
                            if ok:
                                st.success("GMS 서버가 정상입니다.")
                            else:
                                st.error(
                                    "GMS 서버 헬스 체크 실패. URL과 네트워크를 확인하세요."
                                )
                    with cols[1]:
                        if st.button("저장", key="dh_edit_save"):
                            try:
                                update_datahub_source(
                                    name=edit_dh,
                                    url=new_url,
                                    faiss_path=(new_faiss or None),
                                    note=(new_note or None),
                                )
                                st.success("저장되었습니다.")
                                st.session_state.pop("edit_dh_name", None)
                                st.rerun()
                            except Exception as e:
                                st.error(f"저장 실패: {e}")
                    with cols[2]:
                        if st.button("취소", key="dh_edit_cancel"):
                            st.session_state.pop("edit_dh_name", None)
                            st.rerun()

            st.divider()
            st.write("DataHub 추가")
            dh_name = st.text_input("이름", key="dh_name")
            dh_url = st.text_input(
                "URL", key="dh_url", placeholder="http://localhost:8080"
            )
            dh_faiss = st.text_input(
                "FAISS 저장 경로(선택)",
                key="dh_faiss",
                placeholder="예: ./dev/table_info_db",
            )
            dh_note = st.text_input("메모", key="dh_note", placeholder="선택")

            cols = st.columns([1, 1, 2])
            with cols[0]:
                if st.button("헬스 체크", key="dh_health_new"):
                    ok = CheckServer.is_gms_server_healthy(url=dh_url)
                    st.session_state["datahub_last_health"] = bool(ok)
                    if ok:
                        st.success("GMS 서버가 정상입니다.")
                    else:
                        st.error(
                            "GMS 서버 헬스 체크 실패. URL과 네트워크를 확인하세요."
                        )
            with cols[1]:
                if st.button("추가", key="dh_add"):
                    try:
                        if not dh_name or not dh_url:
                            st.warning("이름과 URL을 입력하세요.")
                        else:
                            add_datahub_source(
                                name=dh_name,
                                url=dh_url,
                                faiss_path=(dh_faiss or None),
                                note=dh_note or None,
                            )
                            st.success("추가되었습니다.")
                            st.rerun()
                    except Exception as e:
                        st.error(f"추가 실패: {e}")

    else:  # VectorDB
        # TODO: PLACEHOLDER 다른 곳으로 옮기긴
        PLACEHOLDERS = {
            "faiss": "FAISS 디렉토리 경로 (예: ./dev/table_info_db)",
            "pgvector": "pgvector 연결 문자열 (postgresql://user:pass@host:port/db)",
            "qdrant": "Qdrant 디렉토리 경로 (예: ./dev/table_info_db)"
        }
        with st.container(border=True):
            st.write("등록된 VectorDB")
            for source in list(registry.vectordb):
                cols = st.columns([2, 2, 4, 2, 1, 1])
                with cols[0]:
                    st.text(source.name)
                with cols[1]:
                    st.text(source.type)
                with cols[2]:
                    st.text(source.location)
                with cols[3]:
                    st.caption(source.collection_prefix or "-")
                with cols[4]:
                    if st.button("편집", key=f"edit_vdb_{source.name}"):
                        st.session_state["edit_vdb_name"] = source.name
                with cols[5]:
                    if st.button(
                        "삭제", type="secondary", key=f"del_vdb_{source.name}"
                    ):
                        delete_vectordb_source(name=source.name)
                        st.rerun()

            # 편집 폼
            edit_vdb = st.session_state.get("edit_vdb_name")
            if edit_vdb:
                st.divider()
                st.write(f"VectorDB 편집: {edit_vdb}")
                existing = next(
                    (s for s in registry.vectordb if s.name == edit_vdb), None
                )
                if existing:
                    new_type = st.selectbox(
                        "타입",
                        options=["faiss", "pgvector", "qdrant"],
                        index=["faiss", "pgvector", "qdrant"].index(existing.type),
                        key="vdb_edit_type",
                    )
                    st.write(new_type)
                    new_loc_placeholder = PLACEHOLDERS.get(new_type)
                    new_location = st.text_input(
                        "위치",
                        value=existing.location,
                        key="vdb_edit_location",
                        placeholder=new_loc_placeholder,
                    )
                    new_prefix = st.text_input(
                        "컬렉션 접두사(선택)",
                        value=existing.collection_prefix or "",
                        key="vdb_edit_prefix",
                    )
                    new_note = st.text_input(
                        "메모(선택)", value=existing.note or "", key="vdb_edit_note"
                    )
                    cols = st.columns([1, 1, 2])
                    with cols[0]:
                        if st.button("검증", key="vdb_edit_validate"):
                            try:
                                update_vectordb_settings(
                                    config,
                                    vectordb_type=new_type,
                                    vectordb_location=new_location,
                                )
                                st.success("설정이 유효합니다.")
                            except Exception as e:
                                st.error(f"검증 실패: {e}")
                    with cols[1]:
                        if st.button("저장", key="vdb_edit_save"):
                            try:
                                update_vectordb_source(
                                    name=edit_vdb,
                                    vtype=new_type,
                                    location=new_location,
                                    collection_prefix=(new_prefix or None),
                                    note=(new_note or None),
                                )
                                st.success("저장되었습니다.")
                                st.session_state.pop("edit_vdb_name", None)
                                st.rerun()
                            except Exception as e:
                                st.error(f"저장 실패: {e}")
                    with cols[2]:
                        if st.button("취소", key="vdb_edit_cancel"):
                            st.session_state.pop("edit_vdb_name", None)
                            st.rerun()

            st.divider()
            st.write("VectorDB 추가")
            vdb_name = st.text_input("이름", key="vdb_name")
            vdb_type = st.selectbox(
                "타입", options=["faiss", "pgvector", "qdrant"], key="vdb_type"
            )
            vdb_loc_placeholder = (
                "FAISS 디렉토리 경로 (예: ./dev/table_info_db)"
                if vdb_type == "faiss"
                else "pgvector 연결 문자열 (postgresql://user:pass@host:port/db)"
            )
            vdb_loc_placeholder = PLACEHOLDERS.get(vdb_type)
            vdb_location = st.text_input(
                "위치", key="vdb_location", placeholder=vdb_loc_placeholder
            )
            vdb_prefix = st.text_input(
                "컬렉션 접두사(선택)", key="vdb_prefix", placeholder="예: app1_"
            )
            vdb_note = st.text_input("메모(선택)", key="vdb_note")

            cols = st.columns([1, 1, 2])
            with cols[0]:
                if st.button("검증", key="vdb_validate_new"):
                    try:
                        update_vectordb_settings(
                            config,
                            vectordb_type=vdb_type,
                            vectordb_location=vdb_location,
                        )
                        st.success("설정이 유효합니다.")
                    except Exception as e:
                        st.error(f"검증 실패: {e}")
            with cols[1]:
                if st.button("추가", key="vdb_add"):
                    try:
                        if not vdb_name or not vdb_type or not vdb_location:
                            st.warning("이름/타입/위치를 입력하세요.")
                        else:
                            add_vectordb_source(
                                name=vdb_name,
                                vtype=vdb_type,
                                location=vdb_location,
                                collection_prefix=(vdb_prefix or None),
                                note=(vdb_note or None),
                            )
                            st.success("추가되었습니다.")
                            st.rerun()
                    except Exception as e:
                        st.error(f"추가 실패: {e}")
