"""
Tests for KeywordRetriever — 14 cases.

Pattern follows test_core_base.py:
- pytest, inline fixtures, MemoryHook
"""

import pytest

from lang2sql.components.retrieval import KeywordRetriever
from lang2sql.core.hooks import MemoryHook
from lang2sql.flows.baseline import SequentialFlow

# -------------------------
# Shared test catalog
# -------------------------

ORDER_TABLE = {
    "name": "order_table",
    "description": "고객 주문 정보를 저장하는 테이블",
    "columns": {"order_id": "주문 고유 ID", "amount": "주문 금액"},
    "meta": {"primary_key": "order_id", "tags": ["finance", "core"]},
}

USER_TABLE = {
    "name": "user_table",
    "description": "사용자 계정 정보 테이블",
    "columns": {"user_id": "사용자 고유 ID", "email": "이메일"},
    "meta": {"primary_key": "user_id"},
}

PRODUCT_TABLE = {
    "name": "product_table",
    "description": "상품 목록 및 재고 테이블",
    "columns": {"product_id": "상품 ID", "stock": "재고 수량"},
}

CATALOG = [ORDER_TABLE, USER_TABLE, PRODUCT_TABLE]


# -------------------------
# Tests
# -------------------------


def test_basic_search_returns_relevant_table():
    """'주문' 질문 → order_table이 top 위치."""
    retriever = KeywordRetriever(catalog=CATALOG)
    results = retriever("주문 정보 조회")

    assert results
    assert results[0]["name"] == "order_table"


def test_top_n_limits_results():
    """top_n=2 → 최대 2개 반환."""
    retriever = KeywordRetriever(catalog=CATALOG, top_n=2)
    results = retriever("테이블")

    assert len(results) <= 2


def test_top_n_larger_than_catalog():
    """top_n=10, catalog 3개 → 최대 3개 반환."""
    retriever = KeywordRetriever(catalog=CATALOG, top_n=10)
    results = retriever("테이블")

    assert len(results) <= len(CATALOG)


def test_zero_results_returns_empty_list():
    """완전히 무관한 query → []."""
    retriever = KeywordRetriever(catalog=CATALOG)
    results = retriever("xyzzy_no_match_token_12345")

    assert results == []


def test_returns_list_of_dict():
    """결과가 list[dict]인지 확인."""
    retriever = KeywordRetriever(catalog=CATALOG)
    results = retriever("주문")

    assert isinstance(results, list)
    assert len(results) > 0
    assert isinstance(results[0], dict)


def test_hook_start_end_events():
    """MemoryHook으로 start/end 이벤트 확인."""
    hook = MemoryHook()
    retriever = KeywordRetriever(catalog=CATALOG, hook=hook)
    retriever("주문")

    assert len(hook.events) == 2
    assert hook.events[0].name == "component.run"
    assert hook.events[0].phase == "start"
    assert hook.events[1].name == "component.run"
    assert hook.events[1].phase == "end"
    assert hook.events[1].duration_ms is not None
    assert hook.events[1].duration_ms >= 0.0


def test_empty_catalog():
    """catalog=[] → []."""
    retriever = KeywordRetriever(catalog=[])
    results = retriever("주문")

    assert results == []


def test_meta_preserved_in_results():
    """meta 필드가 결과 dict에 그대로 포함되는지 확인."""
    retriever = KeywordRetriever(catalog=CATALOG)
    results = retriever("주문")

    assert "meta" in results[0]
    assert results[0]["meta"]["primary_key"] == "order_id"


def test_index_fields_meta():
    """index_fields=["description","meta"] → meta 텍스트도 검색에 반영."""
    # finance라는 단어는 meta.tags에만 존재 (name/description/columns에는 없음)
    catalog = [
        {
            "name": "alpha",
            "description": "일반 데이터 저장소",
            "meta": {"tags": ["finance", "core"]},
        },
        {
            "name": "beta",
            "description": "기타 로그 테이블",
            "meta": {"tags": ["logging"]},
        },
    ]

    retriever = KeywordRetriever(
        catalog=catalog,
        index_fields=["description", "meta"],
    )
    results = retriever("finance")

    assert len(results) > 0
    assert results[0]["name"] == "alpha"


def test_result_order_by_relevance():
    """관련도 높은 테이블이 앞에 위치하는지 확인."""
    catalog = [
        {
            "name": "order_summary",
            "description": "주문 요약 주문 집계 주문 통계",  # '주문' 3회
        },
        {
            "name": "user_table",
            "description": "사용자 주문 기록",  # '주문' 1회
        },
    ]

    retriever = KeywordRetriever(catalog=catalog)
    results = retriever("주문")

    assert len(results) >= 2
    assert results[0]["name"] == "order_summary"


def test_columns_text_indexed():
    """컬럼명/컬럼설명으로 검색 가능한지 확인."""
    catalog = [
        {
            "name": "sales",
            "description": "판매 데이터",
            "columns": {"revenue": "매출액", "region": "지역"},
        },
        {
            "name": "logs",
            "description": "시스템 로그",
            "columns": {"event_type": "이벤트 유형"},
        },
    ]

    retriever = KeywordRetriever(catalog=catalog)
    results = retriever("매출액")

    assert len(results) > 0
    assert results[0]["name"] == "sales"


def test_missing_optional_fields_no_error():
    """columns/meta 없는 entry가 있어도 crash 없음."""
    catalog = [
        {"name": "minimal", "description": "최소 필드만 있는 테이블"},
        {
            "name": "full",
            "description": "전체 필드",
            "columns": {"id": "ID"},
            "meta": {},
        },
    ]

    retriever = KeywordRetriever(catalog=catalog)
    results = retriever("테이블")
    assert isinstance(results, list)


def test_end_to_end_in_sequential_flow():
    """SequentialFlow(steps=[retriever]).run('...') 가 동작하는지 확인."""
    retriever = KeywordRetriever(catalog=CATALOG)
    flow = SequentialFlow(steps=[retriever])

    results = flow.run("주문 내역 확인")

    assert isinstance(results, list)
    assert len(results) > 0
    assert results[0]["name"] == "order_table"
