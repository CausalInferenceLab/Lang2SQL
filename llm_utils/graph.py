import json

from langgraph.graph import END, StateGraph
from langchain.chains.sql_database.prompt import SQL_PROMPTS
from pydantic import BaseModel, Field
from .agent import manager_agent, manager_agent_edge
from .llm_factory import get_llm
from llm_utils.chains import (
    query_refiner_chain,
    query_maker_chain,
)
from llm_utils.retrieval import search_tables
from langchain.schema import AIMessage
from .state import QueryMakerState

# 노드 식별자 정의
QUERY_REFINER = "query_refiner"
GET_TABLE_INFO = "get_table_info"
TOOL = "tool"
TABLE_FILTER = "table_filter"
QUERY_MAKER = "query_maker"
MANAGER_AGENT = "manager_agent"
EXCEPTION_END_NODE = "exception_end_node"


def exception_end_node(state: QueryMakerState):
    intent_reason = state.get("intent_reason", "SQL 쿼리 생성을 위한 질문을 해주세요")
    end_message_prompt = f"""
다음과 같은 이유로 답변을 할 수 없습니다!  
```   
{intent_reason}
```
"""
    return {
        "messages": state["messages"] + [AIMessage(content=end_message_prompt)],
    }


# 노드 함수: QUERY_REFINER 노드
def query_refiner_node(state: QueryMakerState):
    # refined_node의 결과값으로 바로 AIMessages 반환
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


def get_table_info_node(state: QueryMakerState):
    # retriever_name과 top_n을 이용하여 검색 수행
    documents_dict = search_tables(
        query=state["user_input"],
        retriever_name=state["retriever_name"],
        top_n=state["top_n"],
        device=state["device"],
    )
    state["searched_tables"] = documents_dict

    return state


# 노드 함수: QUERY_MAKER 노드
def query_maker_node(state: QueryMakerState):
    # sturctured output 사용
    res = query_maker_chain.invoke(
        input={
            "user_input": [state["user_input"]],
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


# StateGraph 생성 및 구성
builder = StateGraph(QueryMakerState)
# 노드 추가
builder.add_node(MANAGER_AGENT, manager_agent)
builder.add_node(GET_TABLE_INFO, get_table_info_node)
builder.add_node(QUERY_REFINER, query_refiner_node)
builder.add_node(QUERY_MAKER, query_maker_node)  #  query_maker_node_with_db_guide
builder.add_node(EXCEPTION_END_NODE, exception_end_node)
# builder.add_node(
#     QUERY_MAKER, query_maker_node_with_db_guide
# )  #  query_maker_node_with_db_guide

# 기본 엣지 설정
builder.set_entry_point(MANAGER_AGENT)
builder.add_edge(GET_TABLE_INFO, QUERY_REFINER)
builder.add_edge(QUERY_REFINER, QUERY_MAKER)

# 조건부 엣지
builder.add_conditional_edges(
    MANAGER_AGENT,
    manager_agent_edge,
    {
        "end": EXCEPTION_END_NODE,
        "make_query": GET_TABLE_INFO,
    },
)

# QUERY_MAKER 노드 후 종료
builder.add_edge(QUERY_MAKER, END)

# EXCEPTION_END_NODE 노드 후 종료
builder.add_edge(EXCEPTION_END_NODE, END)
