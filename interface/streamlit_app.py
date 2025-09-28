"""
Streamlit 멀티 페이지 애플리케이션 설정 파일.

- PAGES 딕셔너리로 페이지 정보(page 경로 및 제목)를 관리합니다.
- PAGES를 기반으로 Streamlit 네비게이션 메뉴를 생성하고 실행합니다.
"""

import streamlit as st

PAGES = {
    "lang2sql": {
        "page": "lang2sql.py",
        "title": "Lang2SQL",
    },
    "graph_builder": {
        "page": "graph_builder.py",
        "title": "Graph Builder",
    },
}


def validate_pages(
    *,
    pages_dict: dict,
) -> None:
    """
    PAGES 딕셔너리의 구조와 값을 검증합니다.

    검증 항목:
        - 최상위 객체는 딕셔너리(dict)여야 합니다.
        - 각 항목은 'page'와 'title' 키를 가진 딕셔너리여야 합니다.
        - 'page'와 'title' 값은 비어 있지 않은 문자열이어야 합니다.

    오류 발생:
        - 조건을 만족하지 않으면 ValueError를 발생시킵니다.

    Args:
        pages_dict (dict): 페이지 정보가 담긴 딕셔너리

    Raises:
        ValueError: 유효하지 않은 구조나 값을 가진 경우
    """

    if not isinstance(pages_dict, dict):
        raise ValueError("PAGES must be a dictionary.")

    for key, page_info in pages_dict.items():
        if not isinstance(page_info, dict):
            raise ValueError(
                f"Each page info must be a dictionary. Error at key: {key}"
            )

        if "page" not in page_info or "title" not in page_info:
            raise ValueError(
                f"Each page must have 'page' and 'title' fields. Error at key: {key}"
            )

        if not isinstance(page_info["page"], str) or not page_info["page"]:
            raise ValueError(f"'page' must be a non-empty string. Error at key: {key}")

        if not isinstance(page_info["title"], str) or not page_info["title"]:
            raise ValueError(f"'title' must be a non-empty string. Error at key: {key}")


validate_pages(pages_dict=PAGES)

pages = [
    st.Page(
        page=page_info["page"],
        title=page_info["title"],
    )
    for page_info in PAGES.values()
]

st.navigation(pages).run()
