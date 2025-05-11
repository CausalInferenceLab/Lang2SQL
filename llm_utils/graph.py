import os
import json
import sys

from typing_extensions import TypedDict, Annotated
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages

from llm_utils.chains import (
    query_refiner_chain,
    query_maker_chain,
)

from llm_utils.tools import get_info_from_db
from agent.query_generator.query_generator import QueryGenerator

# 노드 식별자 정의
QUERY_REFINER = "query_refiner"
GET_TABLE_INFO = "get_table_info"
TOOL = "tool"
TABLE_FILTER = "table_filter"
QUERY_MAKER = "query_maker"
QUERY_GENERATOR = "query_generator"  


# 상태 타입 정의 (추가 상태 정보와 메시지들을 포함)
class QueryMakerState(TypedDict):
    messages: Annotated[list, add_messages]
    user_database_env: str
    searched_tables: dict[str, dict[str, str]]
    best_practice_query: str
    refined_input: str
    generated_query: str


# 노드 함수: QUERY_REFINER 노드
def query_refiner_node(state: QueryMakerState):
    res = query_refiner_chain.invoke(
        input={
            "user_input": [state["messages"][0].content],
            "user_database_env": [state["user_database_env"]],
            "best_practice_query": [state["best_practice_query"]],
        }
    )
    state["messages"].append(res)
    state["refined_input"] = res
    return state


def get_table_info_node(state: QueryMakerState):
    from langchain_community.vectorstores import FAISS
    from langchain_openai import OpenAIEmbeddings

    embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
    try:
        db = FAISS.load_local(
            os.getcwd() + "/table_info_db",
            embeddings,
            allow_dangerous_deserialization=True,
        )
    except:
        documents = get_info_from_db()
        db = FAISS.from_documents(documents, embeddings)
        db.save_local(os.getcwd() + "/table_info_db")
        print("table_info_db not found")
    doc_res = db.similarity_search(state["messages"][-1].content)
    documents_dict = {}

    for doc in doc_res:
        lines = doc.page_content.split("\n")

        # 테이블명 및 설명 추출
        table_name, table_desc = lines[0].split(": ", 1)

        # 컬럼 정보 추출
        columns = {}
        if len(lines) > 2 and lines[1].strip() == "Columns:":
            for line in lines[2:]:
                if ": " in line:
                    col_name, col_desc = line.split(": ", 1)
                    columns[col_name.strip()] = col_desc.strip()

        # 딕셔너리 저장
        documents_dict[table_name] = {
            "table_description": table_desc.strip(),
            **columns,  # 컬럼 정보 추가
        }
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



_query_generator = QueryGenerator()


def query_generator_node(state: QueryMakerState):
    """
    쿼리 제너레이터를 사용하여 SQL을 생성하는 노드입니다.
    사용자 질문만 입력으로 받습니다.
    """
    # 사용자 질문 추출 (원본 질문과 정제된 질문 모두 사용)
    original_question = state["messages"][0].content
    refined_question = state["refined_input"]
    
    # 쿼리 제너레이터로 SQL 생성
    sql_result = _query_generator.graph_response(refined_question)
    
    # 결과 저장
    state["generated_query"] = sql_result["messages"][-1].content
    state["messages"].append(sql_result["messages"][-1])
    state["searched_tables"] = sql_result.get("searched_tables", {})
    
    return state


# StateGraph 생성 및 구성
builder = StateGraph(QueryMakerState)
builder.set_entry_point(QUERY_REFINER)

# 노드 추가
builder.add_node(QUERY_REFINER, query_refiner_node)
builder.add_node(GET_TABLE_INFO, get_table_info_node)
builder.add_node(QUERY_MAKER, query_maker_node)
builder.add_node(QUERY_GENERATOR, query_generator_node)  

# 기본 엣지 설정
builder.add_edge(QUERY_REFINER, QUERY_GENERATOR)  
builder.add_edge(QUERY_GENERATOR, END)  


# builder.add_edge(QUERY_REFINER, GET_TABLE_INFO)
# builder.add_edge(GET_TABLE_INFO, QUERY_MAKER) 
# builder.add_edge(QUERY_MAKER, END)
