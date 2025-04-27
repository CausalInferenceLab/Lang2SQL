import glob
import json
import os

import pandas as pd
import streamlit as st

st.set_page_config(layout="wide", page_title="Lang2SQL 평가 시각화")

# 스타일 적용
st.markdown(
    """
<style>
    .main {
        padding: 2rem;
    }
    .sql-code {
        background-color: #f8f9fa;
        padding: 10px;
        border-radius: 5px;
        font-family: monospace;
        white-space: pre-wrap;
    }
    .persona-card {
        background-color: #f8f9fa;
        padding: 15px;
        border-radius: 5px;
        margin-bottom: 20px;
    }
    .persona-card h4 {
        color: #1f77b4;
        margin-top: 0;
    }
    .persona-card p {
        margin-bottom: 5px;
        color: #333;
    }
    pre {
        white-space: pre-wrap !important;
        word-wrap: break-word !important;
        overflow-wrap: break-word !important;
    }
    code {
        white-space: pre-wrap !important;
        overflow-x: visible !important;
        word-wrap: break-word !important;
    }
    .stCodeBlock {
        max-width: 100% !important;
        overflow-x: visible !important;
    }
    .block-container {
        max-width: 100% !important;
        padding-left: 5% !important;
        padding-right: 5% !important;
    }
    .stTabs [data-baseweb="tab-panel"] {
        overflow-x: visible !important;
    }
    .version-comparison {
        margin-top: 20px;
        margin-bottom: 20px;
        padding: 10px;
        border: 1px solid #eaeaea;
        border-radius: 5px;
    }
    .version-title {
        font-weight: bold;
        color: #1f77b4;
    }
</style>
""",
    unsafe_allow_html=True,
)

# 제목 설정
st.title("Lang2SQL 평가 결과 시각화")
st.markdown("SQL 생성 프로세스와 결과를 검토합니다.")


# 폴더와 버전 정보 로드 함수
def load_version_folders():
    version_folders = glob.glob("data/q_sql/*/")
    version_names = [
        os.path.basename(os.path.dirname(folder)) for folder in version_folders
    ]
    return dict(zip(version_names, version_folders))


# 특정 버전의 JSON 파일 로드 함수
def load_json_files(version_folder):
    json_files = glob.glob(f"{version_folder}/*.json")
    return json_files


# 선택된 파일로부터 데이터 로드
def load_data(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


# 버전 폴더 선택
version_folders = load_version_folders()
if not version_folders:
    st.error("data/q_sql 디렉토리에 버전 폴더가 존재하지 않습니다.")
    st.stop()

# 비교할 버전 선택
selected_versions = st.multiselect(
    "비교할 버전 선택 (최대 2개):",
    options=list(version_folders.keys()),
    default=list(version_folders.keys())[: min(2, len(version_folders.keys()))],
)

if len(selected_versions) == 0:
    st.warning("최소 하나의 버전을 선택해주세요.")
    st.stop()
elif len(selected_versions) > 2:
    st.warning("최대 2개의 버전만 비교할 수 있습니다.")
    selected_versions = selected_versions[:2]

# 각 버전의 첫 번째 JSON 파일 로드
version_data = {}
for version in selected_versions:
    version_path = version_folders[version]
    json_files = load_json_files(version_path)

    if not json_files:
        st.error(f"{version} 버전에 JSON 파일이 존재하지 않습니다.")
        continue

    # 기본적으로 첫 번째 파일 선택
    data = load_data(json_files[0])
    version_data[version] = {
        "files": json_files,
        "data": data,
        "selected_file": json_files[0],
    }

# 버전별 파일 선택 가능하게
col_versions = st.columns(len(selected_versions))
for i, version in enumerate(selected_versions):
    if version in version_data:
        with col_versions[i]:
            selected_file = st.selectbox(
                f"{version} 버전 파일 선택",
                version_data[version]["files"],
                key=f"file_select_{version}",
            )
            version_data[version]["data"] = load_data(selected_file)
            version_data[version]["selected_file"] = selected_file

# 사이드바에 질문 목록 표시
st.sidebar.title("질문 목록")
# 첫 번째 버전 데이터에서 질문 가져오기
if selected_versions and selected_versions[0] in version_data:
    first_version = selected_versions[0]
    questions = version_data[first_version]["data"]["questions"]

    selected_q_index = st.sidebar.radio(
        "질문을 선택하세요:",
        options=range(len(questions)),
        format_func=lambda i: f"Q{i+1}: {questions[i][:50]}...",
    )
else:
    st.error("선택된 버전이 없거나 데이터를 불러올 수 없습니다.")
    st.stop()

# 페르소나 정보 표시
st.header("페르소나 정보")
# 첫 번째 버전의 페르소나 정보 사용
persona = version_data[first_version]["data"].get("persona", {})
col1, col2 = st.columns([1, 2])

with col1:
    st.markdown(
        f"""
    <div class="persona-card">
        <h4>{persona.get('name', '이름 없음')}</h4>
        <p><strong>부서:</strong> {persona.get('department', '정보 없음')}</p>
        <p><strong>역할:</strong> {persona.get('role', '정보 없음')}</p>
    </div>
    """,
        unsafe_allow_html=True,
    )

with col2:
    st.markdown(
        f"""
    <div class='persona-card'>
        <p>{persona.get('background', '배경 정보 없음')}</p>
    </div>
    """,
        unsafe_allow_html=True,
    )

# 선택된 질문 및 답변 표시
st.header("질문 및 답변 세부 정보")

# 선택된 질문 표시
st.subheader("🔍 원본 질문")
st.markdown(f"**{questions[selected_q_index]}**")

# 각 버전별 비교 탭 생성
tab_names = ["SQL 결과", "질문 구체화", "검색된 테이블", "전체 SQL 생성 과정"]
tabs = st.tabs(tab_names)

# SQL 결과 탭
with tabs[0]:
    cols = st.columns(len(selected_versions))
    for i, version in enumerate(selected_versions):
        if version in version_data:
            with cols[i]:
                st.markdown(f"### {version} 버전의 SQL 쿼리")
                sql_query = version_data[version]["data"]["answers"][
                    selected_q_index
                ].get("answer_SQL", "SQL 쿼리가 없습니다.")
                st.code(sql_query, language="sql")

                st.markdown(f"### {version} 버전의 SQL 설명")
                st.markdown(
                    version_data[version]["data"]["answers"][selected_q_index].get(
                        "answer_explanation", "설명이 없습니다."
                    )
                )

                st.markdown(f"### {version} 버전의 데이터베이스 환경")
                st.code(
                    version_data[version]["data"]["answers"][selected_q_index].get(
                        "user_database_env", "정보 없음"
                    )
                )

# 질문 구체화 탭
with tabs[1]:
    cols = st.columns(len(selected_versions))
    for i, version in enumerate(selected_versions):
        if version in version_data:
            with cols[i]:
                answer = version_data[version]["data"]["answers"][selected_q_index]
                st.markdown(f"### {version} 버전의 질문 구체화")

                refined_question = answer.get(
                    "question_refined", "질문 구체화 정보가 없습니다."
                )
                # 구체화된 질문이 리스트인 경우 각각 표시
                if isinstance(refined_question, list):
                    for idx, q in enumerate(refined_question):
                        st.markdown(f"{idx+1}. {q}")
                else:
                    st.markdown(refined_question)

# 검색된 테이블 탭
with tabs[2]:
    cols = st.columns(len(selected_versions))
    for i, version in enumerate(selected_versions):
        if version in version_data:
            with cols[i]:
                answer = version_data[version]["data"]["answers"][selected_q_index]
                st.markdown(f"### {version} 버전의 검색된 테이블")

                searched_tables = answer.get("searched_tables", {})

                if searched_tables:
                    for table_name, table_info in searched_tables.items():
                        with st.expander(f"테이블: {table_name}"):
                            st.markdown(
                                f"**설명:** {table_info.get('table_description', '설명 없음')}"
                            )

                            # 테이블 컬럼 정보를 DataFrame으로 변환하여 표시
                            columns_data = []
                            for col_name, col_desc in table_info.items():
                                if col_name != "table_description":
                                    columns_data.append(
                                        {"컬럼명": col_name, "설명": col_desc}
                                    )

                            if columns_data:
                                st.table(pd.DataFrame(columns_data))
                            else:
                                st.info("컬럼 정보가 없습니다.")
                else:
                    st.info("검색된 테이블 정보가 없습니다.")

# 전체 SQL 생성 과정 탭
with tabs[3]:
    cols = st.columns(len(selected_versions))
    for i, version in enumerate(selected_versions):
        if version in version_data:
            with cols[i]:
                answer = version_data[version]["data"]["answers"][selected_q_index]
                st.markdown(f"### {version} 버전의 SQL 생성 과정")

                st.markdown("#### 1. 원본 질문")
                st.markdown(f"> {questions[selected_q_index]}")

                st.markdown("#### 2. 질문 구체화")
                refined_question = answer.get(
                    "question_refined", "질문 구체화 정보가 없습니다."
                )
                if isinstance(refined_question, list):
                    for idx, q in enumerate(refined_question):
                        st.markdown(f"{idx+1}. {q}")
                else:
                    st.markdown(refined_question)

                st.markdown("#### 3. 검색된 테이블")
                table_names = list(answer.get("searched_tables", {}).keys())
                st.markdown(
                    ", ".join(table_names) if table_names else "테이블 정보 없음"
                )

                st.markdown("#### 4. 생성된 SQL")
                st.code(
                    answer.get("answer_SQL", "SQL 쿼리가 없습니다."), language="sql"
                )

                st.markdown("#### 5. SQL 설명")
                st.markdown(answer.get("answer_explanation", "설명이 없습니다."))
