import os

import streamlit as st

from interface.core.config import (
    add_db_connection,
    delete_db_connection,
    get_db_connections_registry,
    update_db_connection,
    update_db_settings,
)
from utils.databases import DatabaseFactory
from utils.databases.factory import load_config_from_env

DB_TYPES = [
    "postgresql",
    "mysql",
    "mariadb",
    "oracle",
    "clickhouse",
    "duckdb",
    "sqlite",
    "databricks",
    "snowflake",
    "trino",
]


def _non_secret_fields(db_type: str) -> list[tuple[str, str]]:
    t = (db_type or "").lower()
    # label, key (values dict)
    if t in {"duckdb", "sqlite"}:
        return [("Path", "path")]
    base = [
        ("Host", "host"),
        ("Port", "port"),
        ("User", "user"),
        ("Database", "database"),
    ]
    return base


def _extra_non_secret_fields(db_type: str) -> list[tuple[str, str]]:
    t = (db_type or "").lower()
    if t == "oracle":
        return [("Service Name", "service_name")]
    if t == "databricks":
        return [
            ("HTTP Path", "http_path"),
            ("Catalog(옵션)", "catalog"),
            ("Schema(옵션)", "schema"),
        ]
    if t == "snowflake":
        return [
            ("Account", "account"),
            ("Warehouse(옵션)", "warehouse"),
            ("Schema(옵션)", "schema"),
        ]
    if t == "trino":
        return [
            ("HTTP Scheme(http/https)", "http_scheme"),
            ("Catalog", "catalog"),
            ("Schema", "schema"),
        ]
    return []


def _secret_fields(db_type: str) -> list[tuple[str, str]]:
    t = (db_type or "").lower()
    if t in {"postgresql", "mysql", "mariadb", "oracle", "clickhouse", "trino"}:
        return [("Password", "password")]
    if t == "databricks":
        return [("Access Token", "access_token")]
    if t == "snowflake":
        return [("Password", "password")]
    # duckdb/sqlite는 비밀번호 필요 없음
    return []


def _prefill_from_env(db_type: str, key: str) -> str:
    # Normalize to PREFIX_KEY
    prefix = (db_type or "").upper()
    if key == "path":
        # duckdb/sqlite: PATH 로 통일 저장
        return os.getenv(f"{prefix}_PATH", "")
    return os.getenv(f"{prefix}_{key.upper()}", "")


def render_db_section() -> None:
    st.subheader("DB 연결")

    registry = get_db_connections_registry()

    # 목록 렌더링
    with st.container(border=True):
        st.write("등록된 DB 프로파일")
        for profile in list(registry.connections):
            cols = st.columns([2, 2, 3, 2, 1, 1])
            with cols[0]:
                st.text(profile.name)
            with cols[1]:
                st.text(profile.type)
            with cols[2]:
                host_or_path = profile.host or (profile.extra or {}).get("path") or "-"
                st.caption(host_or_path)
            with cols[3]:
                st.caption(profile.note or "")
            with cols[4]:
                if st.button("편집", key=f"edit_db_{profile.name}"):
                    st.session_state["edit_db_name"] = profile.name
            with cols[5]:
                if st.button("삭제", type="secondary", key=f"del_db_{profile.name}"):
                    delete_db_connection(name=profile.name)
                    st.rerun()

    # 편집 폼
    edit_name = st.session_state.get("edit_db_name")
    if edit_name:
        st.divider()
        st.write(f"DB 프로파일 편집: {edit_name}")
        existing = next((c for c in registry.connections if c.name == edit_name), None)
        if existing:
            new_type = st.selectbox(
                "타입",
                options=DB_TYPES,
                index=max(
                    0, DB_TYPES.index(existing.type) if existing.type in DB_TYPES else 0
                ),
                key="db_edit_type",
            )

            values: dict[str, object] = {}
            # 기본 필드
            for label, k in _non_secret_fields(new_type):
                default_val = (
                    existing.extra.get("path")
                    if k == "path" and existing.extra
                    else getattr(existing, k, "")
                )
                v = st.text_input(
                    label, value=str(default_val or ""), key=f"db_edit_val_{k}"
                )
                if v != "":
                    values[k] = v

            # 추가 필드(Non-secret)
            extra_vals: dict[str, str] = {}
            for label, k in _extra_non_secret_fields(new_type):
                default_val = (existing.extra or {}).get(k, "")
                v = st.text_input(
                    label, value=str(default_val or ""), key=f"db_edit_extra_{k}"
                )
                if v != "":
                    extra_vals[k] = v
            if extra_vals:
                values["extra"] = extra_vals

            # 시크릿 필드 (JSON 저장 허용)
            secrets: dict[str, str] = {}
            for label, k in _secret_fields(new_type):
                # 기존 저장값 우선: existing.password or existing.extra
                default_secret = ""
                if k == "password":
                    default_secret = getattr(
                        existing, "password", None
                    ) or _prefill_from_env(new_type, k)
                else:
                    default_secret = (existing.extra or {}).get(
                        k, ""
                    ) or _prefill_from_env(new_type, k)

                # 비밀키는 값을 미리 채우지 않음 (보안)
                placeholder = (
                    "값 설정됨 (변경하려면 입력)" if default_secret else "값 없음"
                )
                sv = st.text_input(
                    label,
                    value="",
                    type="password",
                    key=f"db_edit_secret_{k}",
                    placeholder=placeholder,
                )
                # 입력값이 없으면 기존 값 유지, 있으면 새 값 사용
                if sv:
                    secrets[k] = sv
                elif default_secret:
                    secrets[k] = default_secret

            cols = st.columns([1, 1, 2])
            with cols[0]:
                if st.button("적용(세션)", key="db_edit_apply"):
                    try:
                        update_db_settings(
                            db_type=new_type, values=values, secrets=secrets
                        )
                        st.success("환경/세션에 적용되었습니다.")
                    except Exception as e:
                        st.error(f"적용 실패: {e}")
            with cols[1]:
                if st.button("저장", key="db_edit_save"):
                    try:
                        update_db_connection(
                            name=edit_name,
                            db_type=new_type,
                            host=str(values.get("host") or "") or None,
                            port=(
                                int(values.get("port"))
                                if str(values.get("port") or "").isdigit()
                                else None
                            ),
                            user=str(values.get("user") or "") or None,
                            password=(secrets.get("password") or None),
                            database=str(values.get("database") or "") or None,
                            extra=values.get("extra"),
                            note=existing.note,
                        )
                        st.success("저장되었습니다.")
                        st.session_state.pop("edit_db_name", None)
                        st.rerun()
                    except Exception as e:
                        st.error(f"저장 실패: {e}")
            with cols[2]:
                if st.button("연결 테스트", key="db_edit_test"):
                    try:
                        # 먼저 적용하여 env에 반영
                        update_db_settings(
                            db_type=new_type, values=values, secrets=secrets
                        )

                        connector = DatabaseFactory.get_connector(db_type=new_type)
                        # 간단한 SELECT 1 테스트 (DB마다 상이할 수 있음)
                        test_sql = (
                            "SELECT 1"
                            if new_type not in {"oracle", "snowflake", "trino"}
                            else {
                                "oracle": "SELECT 1 FROM dual",
                                "snowflake": "SELECT 1",
                                "trino": "SELECT 1",
                            }[new_type]
                        )
                        df = connector.run_sql(test_sql)
                        st.success(f"연결 성공. 결과 행 수: {len(df)}")
                        connector.close()
                    except Exception as e:
                        st.error(f"연결 테스트 실패: {e}")

    st.divider()
    # 추가 폼
    st.write("DB 프로파일 추가")
    name = st.text_input("이름", key="db_new_name")
    db_type = st.selectbox("타입", options=DB_TYPES, key="db_new_type")

    values_new: dict[str, object] = {}
    for label, k in _non_secret_fields(db_type):
        v = st.text_input(label, key=f"db_new_val_{k}")
        if v != "":
            values_new[k] = v

    extra_new: dict[str, str] = {}
    for label, k in _extra_non_secret_fields(db_type):
        v = st.text_input(label, key=f"db_new_extra_{k}")
        if v != "":
            extra_new[k] = v
    if extra_new:
        values_new["extra"] = extra_new

    secrets_new: dict[str, str] = {}
    for label, k in _secret_fields(db_type):
        sv = st.text_input(label, key=f"db_new_secret_{k}", type="password")
        if sv != "":
            secrets_new[k] = sv

    cols2 = st.columns([1, 1, 2])
    with cols2[0]:
        if st.button("검증", key="db_new_validate"):
            try:
                update_db_settings(
                    db_type=db_type, values=values_new, secrets=secrets_new
                )
                # load back config for the type to ensure required fields presence
                _ = load_config_from_env(db_type.upper())
                st.success("형식 검증 완료.")
            except Exception as e:
                st.error(f"검증 실패: {e}")
    with cols2[1]:
        if st.button("추가", key="db_new_add"):
            try:
                if not name:
                    st.warning("이름을 입력하세요.")
                else:
                    add_db_connection(
                        name=name,
                        db_type=db_type,
                        host=str(values_new.get("host") or "") or None,
                        port=(
                            int(values_new.get("port"))
                            if str(values_new.get("port") or "").isdigit()
                            else None
                        ),
                        user=str(values_new.get("user") or "") or None,
                        password=(secrets_new.get("password") or None),
                        database=str(values_new.get("database") or "") or None,
                        extra=values_new.get("extra"),
                        note=None,
                    )
                    st.success("추가되었습니다.")
                    st.rerun()
            except Exception as e:
                st.error(f"추가 실패: {e}")
