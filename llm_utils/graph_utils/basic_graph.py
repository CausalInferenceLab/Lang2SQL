import json

from langgraph.graph import StateGraph, END
from llm_utils.graph_utils.base import (
    QueryMakerState,
    GET_TABLE_INFO,
    QUERY_REFINER,
    QUERY_MAKER,
    get_table_info_node,
    query_refiner_node,
    query_maker_node,
)

"""
기본 워크플로우를 위한 StateGraph 구성입니다.
GET_TABLE_INFO -> QUERY_REFINER -> QUERY_MAKER 순서로 실행됩니다.
"""

# StateGraph 생성 및 구성
builder = StateGraph(QueryMakerState)
builder.set_entry_point(GET_TABLE_INFO)

# 노드 추가
builder.add_node(GET_TABLE_INFO, get_table_info_node)
builder.add_node(QUERY_REFINER, query_refiner_node)
builder.add_node(QUERY_MAKER, query_maker_node)

# 기본 엣지 설정
builder.add_edge(GET_TABLE_INFO, QUERY_REFINER)
builder.add_edge(QUERY_REFINER, QUERY_MAKER)

# QUERY_MAKER 노드 후 종료
builder.add_edge(QUERY_MAKER, END)
