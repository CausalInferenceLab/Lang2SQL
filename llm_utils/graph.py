import os
import json
import re

from typing_extensions import TypedDict, Annotated
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langchain.chains.sql_database.prompt import SQL_PROMPTS
from pydantic import BaseModel, Field
from .llm_factory import get_llm

from llm_utils.chains import (
    query_refiner_chain,
    query_maker_chain,
)

from llm_utils.tools import get_info_from_db

# 노드 식별자 정의
DETECT_LANGUAGE = "detect_language"
QUERY_REFINER = "query_refiner"
GET_TABLE_INFO = "get_table_info"
TOOL = "tool"
TABLE_FILTER = "table_filter"
QUERY_MAKER = "query_maker"


# 상태 타입 정의 (추가 상태 정보와 메시지들을 포함)
class QueryMakerState(TypedDict):
    messages: Annotated[list, add_messages]
    user_database_env: str
    searched_tables: dict[str, dict[str, str]]
    best_practice_query: str
    refined_input: str
    generated_query: str


# 노드 함수: 언어 감지
def detect_language_regex(state: QueryMakerState):
    """
    정규표현식을 사용해 텍스트의 언어를 감지하는 함수.

    Args:
        text (str): 감지할 텍스트

    Returns:
        dict: 감지된 언어와 관련 정보
    """
    # 언어별 고유 문자 패턴 정의
    patterns = {
        "ko": r"[\u3131-\u3163\uAC00-\uD7A3]",  # 한글 (Hangul)
        "ja": r"[\u3040-\u309F\u30A0-\u30FF]",  # 일본어 (Hiragana, Katakana)
        "zh": r"[\u4E00-\u9FFF]",  # 중국어 (Han characters)
        "ru": r"[\u0400-\u04FF]",  # 러시아어 (Cyrillic)
        "fr": r"[àâçéèêëîïôûùüÿ]",  # 프랑스어 고유 문자
        "es": r"[áéíóúñ¿¡]",  # 스페인어 고유 문자
        "en": r"[a-zA-Z]",  # 영어 (기본 Latin alphabet)
    }
    text = state["messages"][-1].content

    # 특수 문자와 공백 제거
    cleaned_text = re.sub(r"[!@#$%^&*(),.?\"':{}|<>]", "", text)
    cleaned_text = cleaned_text.strip()

    if not cleaned_text:
        return {"language": None, "confidence": 0.0, "method": "regex"}

    # 각 언어별 문자 수 계산
    char_counts = {}
    total_chars = len(cleaned_text)

    for lang, pattern in patterns.items():
        matches = re.findall(pattern, cleaned_text)
        char_count = len(matches)

        # 언어별 가중치 적용
        if lang in ["fr", "es"]:
            # 프랑스어나 스페인어 고유 문자가 있으면 해당 언어일 가능성이 매우 높음
            if char_count > 0:
                char_count = total_chars
        elif lang == "en":
            # 영어는 라틴 알파벳을 공유하는 언어들이 많으므로 가중치 감소
            char_count *= 0.8

        if char_count > 0:
            char_counts[lang] = char_count

    if not char_counts:
        return {"language": None, "confidence": 0.0, "method": "regex"}

    # 가장 많은 문자 수를 가진 언어 선택
    detected_lang = max(char_counts, key=char_counts.get)
    confidence = char_counts[detected_lang] / total_chars

    # 신뢰도 조정
    if detected_lang in ["fr", "es"] and confidence > 0.1:
        confidence = 0.95  # 고유 문자가 있으면 높은 신뢰도
    elif detected_lang == "en":
        # 다른 언어의 문자가 없을 때만 영어 신뢰도 상승
        other_chars = sum(
            char_counts.get(lang, 0) for lang in char_counts if lang != "en"
        )
        if other_chars == 0:
            confidence = 0.95

    return {
        "language": detected_lang,
        "confidence": round(confidence, 4),
        "method": "regex",
    }


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


class SQLResult(BaseModel):
    sql: str = Field(description="SQL 쿼리 문자열")
    explanation: str = Field(description="SQL 쿼리 설명")


def query_maker_node_with_db_guide(state: QueryMakerState):
    sql_prompt = SQL_PROMPTS[state["user_database_env"]]
    llm = get_llm(
        model_type="openai",
        model_name="gpt-4o-mini",
        openai_api_key=os.getenv("OPENAI_API_KEY"),
    )
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
builder.set_entry_point(QUERY_REFINER)

# 노드 추가
builder.add_node(QUERY_REFINER, query_refiner_node)
builder.add_node(GET_TABLE_INFO, get_table_info_node)
# builder.add_node(QUERY_MAKER, query_maker_node)  #  query_maker_node_with_db_guide
builder.add_node(
    QUERY_MAKER, query_maker_node_with_db_guide
)  #  query_maker_node_with_db_guide

# 기본 엣지 설정
builder.add_edge(QUERY_REFINER, GET_TABLE_INFO)
builder.add_edge(GET_TABLE_INFO, QUERY_MAKER)

# QUERY_MAKER 노드 후 종료
builder.add_edge(QUERY_MAKER, END)
