import json

from typing_extensions import TypedDict, Annotated
from langgraph.graph.message import add_messages


from llm_utils.chains import (
    query_maker_chain,
    profile_extraction_chain,
    query_enrichment_chain,
    question_gate_chain,
    document_suitability_chain,
)

from llm_utils.retrieval import search_tables

# 노드 식별자 정의
QUESTION_GATE = "question_gate"
EVALUATE_DOCUMENT_SUITABILITY = "evaluate_document_suitability"
GET_TABLE_INFO = "get_table_info"
TOOL = "tool"
TABLE_FILTER = "table_filter"
QUERY_MAKER = "query_maker"
PROFILE_EXTRACTION = "profile_extraction"
CONTEXT_ENRICHMENT = "context_enrichment"


# 상태 타입 정의 (추가 상태 정보와 메시지들을 포함)
class QueryMakerState(TypedDict):
    messages: Annotated[list, add_messages]
    user_database_env: str
    searched_tables: dict[str, dict[str, str]]
    document_suitability: dict
    best_practice_query: str
    question_profile: dict
    generated_query: str
    retriever_name: str
    top_n: int
    device: str
    question_gate_result: dict


# 노드 함수: QUESTION_GATE 노드
def question_gate_node(state: QueryMakerState):
    """
    사용자의 질문이 SQL로 답변 가능한지 판별하고, 구조화된 결과를 반환하는 게이트 노드입니다.

    - question_gate_chain 으로 적합성을 판정하여
      `question_gate_result`를 설정합니다.

    Args:
        state (QueryMakerState): 그래프 상태

    Returns:
        QueryMakerState: 게이트 판정 결과가 반영된 상태
    """

    question_text = state["messages"][0].content
    suitability = question_gate_chain.invoke({"question": question_text})
    state["question_gate_result"] = {
        "reason": getattr(suitability, "reason", ""),
        "missing_entities": getattr(suitability, "missing_entities", []),
        "requires_data_science": getattr(suitability, "requires_data_science", False),
    }
    return state


# 노드 함수: PROFILE_EXTRACTION 노드
def profile_extraction_node(state: QueryMakerState):
    """
    자연어 쿼리로부터 질문 유형(PROFILE)을 추출하는 노드입니다.

    이 노드는 주어진 자연어 쿼리에서 질문의 특성을 분석하여, 해당 질문이 시계열 분석, 집계 함수 사용, 조건 필터 필요 여부,
    그룹화, 정렬/순위, 기간 비교 등 다양한 특성을 갖는지 여부를 추출합니다.

    추출된 정보는 `QuestionProfile` 모델에 맞춰 저장됩니다. `QuestionProfile` 모델의 필드는 다음과 같습니다:
    - `is_timeseries`: 시계열 분석 필요 여부
    - `is_aggregation`: 집계 함수 필요 여부
    - `has_filter`: 조건 필터 필요 여부
    - `is_grouped`: 그룹화 필요 여부
    - `has_ranking`: 정렬/순위 필요 여부
    - `has_temporal_comparison`: 기간 비교 포함 여부
    - `intent_type`: 질문의 주요 의도 유형

    """
    result = profile_extraction_chain.invoke({"question": state["messages"][0].content})

    state["question_profile"] = result
    print("profile_extraction_node : ", result)
    return state


# 노드 함수: CONTEXT_ENRICHMENT 노드
def context_enrichment_node(state: QueryMakerState):
    """
    주어진 질문과 관련된 메타데이터를 기반으로 질문을 풍부하게 만드는 노드입니다.

    이 함수는 `refined_question`, `profiles`, `related_tables` 정보를 이용하여 자연어 질문을 보강합니다.
    보강 과정에서는 질문의 의도를 유지하면서, 추가적인 세부 정보를 제공하거나 잘못된 용어를 수정합니다.

    주요 작업:
    - 주어진 질문의 메타데이터 (`question_profile` 및 `searched_tables`)를 활용하여, 질문을 수정하거나 추가 정보를 삽입합니다.
    - 질문이 시계열 분석 또는 집계 함수 관련인 경우, 이를 명시적으로 강조합니다 (예: "지난 30일 동안").
    - 자연어에서 실제 열 이름 또는 값으로 잘못 매칭된 용어를 수정합니다 (예: '미국' → 'USA').
    - 보강된 질문을 출력합니다.

    Args:
        state (QueryMakerState): 쿼리와 관련된 상태 정보를 담고 있는 객체.
                                상태 객체는 `messages`, `question_profile`, `searched_tables` 등의 정보를 포함합니다.

    Returns:
        QueryMakerState: 보강된 질문이 포함된 상태 객체.

    Example:
        Given the refined question "What are the total sales in the last month?",
        the function would enrich it with additional information such as:
        - Ensuring the time period is specified correctly.
        - Correcting any column names if necessary.
        - Returning the enriched version of the question.
    """

    searched_tables = state["searched_tables"]
    searched_tables_json = json.dumps(searched_tables, ensure_ascii=False, indent=2)

    # question_profile이 BaseModel인 경우 model_dump() 사용, dict인 경우 그대로 사용
    if hasattr(state["question_profile"], "model_dump"):
        question_profile = state["question_profile"].model_dump()
    else:
        question_profile = state["question_profile"]
    question_profile_json = json.dumps(question_profile, ensure_ascii=False, indent=2)

    # 초기 사용자 입력 사용
    refined_question = state["messages"][0].content

    enriched_text = query_enrichment_chain.invoke(
        input={
            "refined_question": refined_question,
            "profiles": question_profile_json,
            "related_tables": searched_tables_json,
        }
    )

    state["messages"].append(enriched_text)
    print("After context enrichment : ", enriched_text.content)

    return state


def get_table_info_node(state: QueryMakerState):
    # retriever_name과 top_n을 이용하여 검색 수행
    documents_dict = search_tables(
        query=state["messages"][0].content,
        retriever_name=state["retriever_name"],
        top_n=state["top_n"],
        device=state["device"],
    )
    state["searched_tables"] = documents_dict

    return state


# 노드 함수: DOCUMENT_SUITABILITY 노드
def document_suitability_node(state: QueryMakerState):
    """
    GET_TABLE_INFO에서 수집된 테이블 후보들에 대해 문서 적합성 점수를 계산하는 노드입니다.

    질문(`messages[0].content`)과 `searched_tables`(테이블→칼럼 설명 맵)를 입력으로
    프롬프트 체인(`document_suitability_chain`)을 호출하고, 결과 딕셔너리를
    `document_suitability` 상태 키에 저장합니다.

    Returns:
        QueryMakerState: 문서 적합성 평가 결과가 포함된 상태
    """

    # 관련 테이블이 없으면 즉시 반환
    if not state.get("searched_tables"):
        state["document_suitability"] = {}
        return state

    res = document_suitability_chain.invoke(
        {
            "question": state["messages"][0].content,
            "tables": state["searched_tables"],
        }
    )

    items = (
        res.get("results", [])
        if isinstance(res, dict)
        else getattr(res, "results", None)
        or (res.model_dump().get("results", []) if hasattr(res, "model_dump") else [])
    )

    normalized = {}
    for x in items:
        d = (
            x.model_dump()
            if hasattr(x, "model_dump")
            else (
                x
                if isinstance(x, dict)
                else {
                    "table_name": getattr(x, "table_name", ""),
                    "score": getattr(x, "score", 0),
                    "reason": getattr(x, "reason", ""),
                    "matched_columns": getattr(x, "matched_columns", []),
                    "missing_entities": getattr(x, "missing_entities", []),
                }
            )
        )
        t = d.get("table_name")
        if not t:
            continue
        normalized[t] = {
            "score": float(d.get("score", 0)),
            "reason": d.get("reason", ""),
            "matched_columns": d.get("matched_columns", []),
            "missing_entities": d.get("missing_entities", []),
        }

    state["document_suitability"] = normalized

    return state


# 노드 함수: QUERY_MAKER 노드
def query_maker_node(state: QueryMakerState):
    # 사용자 원 질문 + (있다면) 컨텍스트 보강 결과를 하나의 문자열로 결합
    parts = [state["messages"][0].content]
    if len(state["messages"]) > 1:
        last_msg = state["messages"][-1]
        last_content = (
            last_msg.content if hasattr(last_msg, "content") else str(last_msg)
        )
        if isinstance(last_content, str) and last_content.strip():
            parts.append(last_content)

    combined_input = "\n\n---\n\n".join(parts)
    searched_tables_json = json.dumps(
        state["searched_tables"], ensure_ascii=False, indent=2
    )

    res = query_maker_chain.invoke(
        input={
            "user_input": combined_input,
            "user_database_env": state["user_database_env"],
            "searched_tables": searched_tables_json,
        }
    )
    state["generated_query"] = res
    state["messages"].append(res)
    return state
