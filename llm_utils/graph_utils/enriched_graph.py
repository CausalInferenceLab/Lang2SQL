import json

from langgraph.graph import StateGraph, END
from llm_utils.graph_utils.base import (
    QueryMakerState,
    GET_TABLE_INFO,
    PROFILE_EXTRACTION,
    QUERY_REFINER,
    CONTEXT_ENRICHMENT,
    QUERY_MAKER,
    get_table_info_node,
    profile_extraction_node,
    query_refiner_with_profile_node,
    context_enrichment_node,
    query_maker_node,
)

"""
기본 워크플로우에 '프로파일 추출(PROFILE_EXTRACTION)'과 '컨텍스트 보강(CONTEXT_ENRICHMENT)'를 
추가한 확장된 그래프입니다.   
"""

# StateGraph 생성 및 구성
builder = StateGraph(QueryMakerState)
builder.set_entry_point(GET_TABLE_INFO)

# 노드 추가
builder.add_node(GET_TABLE_INFO, get_table_info_node)
builder.add_node(PROFILE_EXTRACTION, profile_extraction_node)
builder.add_node(QUERY_REFINER, query_refiner_with_profile_node)
builder.add_node(CONTEXT_ENRICHMENT, context_enrichment_node)
builder.add_node(QUERY_MAKER, query_maker_node)

# 기본 엣지 설정
builder.add_edge(GET_TABLE_INFO, PROFILE_EXTRACTION)
builder.add_edge(PROFILE_EXTRACTION, QUERY_REFINER)
builder.add_edge(QUERY_REFINER, CONTEXT_ENRICHMENT)
builder.add_edge(CONTEXT_ENRICHMENT, QUERY_MAKER)

# QUERY_MAKER 노드 후 종료
builder.add_edge(QUERY_MAKER, END)
