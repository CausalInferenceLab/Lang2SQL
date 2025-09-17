import json

from langgraph.graph import StateGraph, END
from llm_utils.graph_utils.base import (
    QueryMakerState,
    QUESTION_GATE,
    GET_TABLE_INFO,
    EVALUATE_DOCUMENT_SUITABILITY,
    PROFILE_EXTRACTION,
    CONTEXT_ENRICHMENT,
    QUERY_MAKER,
    question_gate_node,
    get_table_info_node,
    document_suitability_node,
    profile_extraction_node,
    context_enrichment_node,
    query_maker_node,
)

"""
기본 워크플로우에 '프로파일 추출(PROFILE_EXTRACTION)'과 '컨텍스트 보강(CONTEXT_ENRICHMENT)'를 
추가한 확장된 그래프입니다.
"""

# StateGraph 생성 및 구성
builder = StateGraph(QueryMakerState)
builder.set_entry_point(QUESTION_GATE)

# 노드 추가
builder.add_node(QUESTION_GATE, question_gate_node)
builder.add_node(GET_TABLE_INFO, get_table_info_node)
builder.add_node(EVALUATE_DOCUMENT_SUITABILITY, document_suitability_node)
builder.add_node(PROFILE_EXTRACTION, profile_extraction_node)
builder.add_node(CONTEXT_ENRICHMENT, context_enrichment_node)
builder.add_node(QUERY_MAKER, query_maker_node)


def _route_after_gate(state: QueryMakerState):
    return GET_TABLE_INFO


builder.add_conditional_edges(
    QUESTION_GATE,
    _route_after_gate,
    {
        GET_TABLE_INFO: GET_TABLE_INFO,
        END: END,
    },
)

# 기본 엣지 설정
builder.add_edge(GET_TABLE_INFO, EVALUATE_DOCUMENT_SUITABILITY)
builder.add_edge(EVALUATE_DOCUMENT_SUITABILITY, PROFILE_EXTRACTION)
builder.add_edge(PROFILE_EXTRACTION, CONTEXT_ENRICHMENT)
builder.add_edge(CONTEXT_ENRICHMENT, QUERY_MAKER)

# QUERY_MAKER 노드 후 종료
builder.add_edge(QUERY_MAKER, END)
