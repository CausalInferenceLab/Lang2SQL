import os
import json

from typing_extensions import TypedDict, Annotated
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langchain.chains.sql_database.prompt import SQL_PROMPTS
from pydantic import BaseModel, Field
from llm_utils.llm_factory import get_llm

from llm_utils.chains import (
    query_refiner_chain,
    query_maker_chain,
    query_refiner_with_profile_chain,
    profile_extraction_chain,
    query_enrichment_chain,
)

from llm_utils.tools import get_info_from_db
from llm_utils.retrieval import search_tables
from llm_utils.utils import profile_to_text

# 노드 식별자 정의
QUERY_REFINER = "query_refiner"
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
    best_practice_query: str
    refined_input: str
    question_profile: dict
    generated_query: str
    retriever_name: str
    top_n: int
    device: str


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


# 노드 함수: QUERY_REFINER 노드
def query_refiner_node(state: QueryMakerState):
    res = query_refiner_chain.invoke(
        input={
            "user_input": [state["messages"][0].content],
            "user_database_env": [state["user_database_env"]],
            "best_practice_query": [state["best_practice_query"]],
            "searched_tables": [json.dumps(state["searched_tables"])],
        }
    )
    state["messages"].append(res)
    state["refined_input"] = res
    return state


# 노드 함수: QUERY_REFINER 노드
def query_refiner_with_profile_node(state: QueryMakerState):
    """
    자연어 쿼리로부터 질문 유형(PROFILE)을 사용해 자연어 질의를 확장하는 노드입니다.

    """

    profile_bullets = profile_to_text(state["question_profile"])
    res = query_refiner_with_profile_chain.invoke(
        input={
            "user_input": [state["messages"][0].content],
            "user_database_env": [state["user_database_env"]],
            "best_practice_query": [state["best_practice_query"]],
            "searched_tables": [json.dumps(state["searched_tables"])],
            "profile_prompt": [profile_bullets],
        }
    )
    state["messages"].append(res)
    state["refined_input"] = res

    print("refined_input before context enrichment : ", res.content)
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
                                상태 객체는 `refined_input`, `question_profile`, `searched_tables` 등의 정보를 포함합니다.

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

    question_profile = state["question_profile"].model_dump()
    question_profile_json = json.dumps(question_profile, ensure_ascii=False, indent=2)

    enriched_text = query_enrichment_chain.invoke(
        input={
            "refined_question": state["refined_input"],
            "profiles": question_profile_json,
            "related_tables": searched_tables_json,
        }
    )

    state["refined_input"] = enriched_text
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


# 노드 함수: QUERY_MAKER 노드
def query_maker_node(state: QueryMakerState):
    res = query_maker_chain.invoke(
        input={
            "user_input": [state["messages"][0].content],
            "refined_input": [state["refined_input"]],
            "searched_tables": [json.dumps(state["searched_tables"])],
            "user_database_env": [state["user_database_env"]],
        }
    )
    state["generated_query"] = res
    state["messages"].append(res)
    return state


class SQLResult(BaseModel):
    sql: str = Field(description="SQL 쿼리 문자열")
    explanation: str = Field(description="SQL 쿼리 설명")


def query_maker_node_with_db_guide(state: QueryMakerState):
    sql_prompt = SQL_PROMPTS[state["user_database_env"]]
    llm = get_llm()
    chain = sql_prompt | llm.with_structured_output(SQLResult)
    res = chain.invoke(
        input={
            "input": "\n\n---\n\n".join(
                [state["messages"][0].content] + [state["refined_input"].content]
            ),
            "table_info": [json.dumps(state["searched_tables"])],
            "top_k": 10,
        }
    )
    state["generated_query"] = res.sql
    state["messages"].append(res.explanation)
    return state
