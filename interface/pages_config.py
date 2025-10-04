"""
Streamlit 애플리케이션 페이지 설정 모듈.

각 페이지의 경로와 제목을 정의하여 내비게이션에 사용합니다.

Attributes:
    PAGES (list): Streamlit Page 객체 리스트.
        - 홈 페이지
        - Lang2SQL 페이지
        - 그래프 빌더 페이지
"""

import streamlit as st

PAGES = [
    st.Page("app_pages/home.py", title="🏠 홈"),
    st.Page("app_pages/lang2sql.py", title="🔍 Lang2SQL"),
    st.Page("app_pages/graph_builder.py", title="📊 그래프 빌더"),
    st.Page("app_pages/settings.py", title="⚙️ 설정"),
]
