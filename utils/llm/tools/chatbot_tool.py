"""
LangGraph ChatBot에서 사용하는 도구(Tool) 함수들
"""

from langchain_core.tools import tool
from utils.data.datahub_services.base_client import DataHubBaseClient
from utils.data.datahub_services.glossary_service import GlossaryService
from utils.data.datahub_services.query_service import QueryService
from utils.llm.retrieval import search_glossary, search_query_examples


@tool
def search_database_tables(
    query: str, retriever_name: str = "기본", top_n: int = 5, device: str = "cpu"
) -> dict:
    """
    사용자의 자연어 쿼리를 기반으로 관련된 데이터베이스 테이블 정보를 검색합니다.

    이 함수는 SQL 쿼리 생성을 위해 필요한 테이블과 컬럼 정보를 찾아줍니다.
    사용자가 어떤 테이블을 사용해야 할지, 어떤 컬럼이 있는지 물어보거나,
    SQL 쿼리를 만들기 위한 스키마 정보가 필요할 때 이 도구를 사용하세요.

    Args:
        query (str): 검색하려는 자연어 질문입니다.
                    예: "고객 정보를 조회하려면?", "주문 관련 테이블"
        retriever_name (str, optional): 검색기 유형입니다.
                                       "기본" 또는 "Reranker" 중 선택. 기본값은 "기본"
        top_n (int, optional): 검색할 테이블 개수입니다. 기본값은 5개
        device (str, optional): 모델 실행 장치입니다. "cpu" 또는 "cuda". 기본값은 "cpu"

    Returns:
        dict: 테이블 정보가 담긴 딕셔너리입니다.
              각 테이블은 키로 저장되며, 값으로 테이블 설명과 컬럼 정보를 포함합니다.

              예시 형태:
              {
                  "customers": {
                      "table_description": "고객 정보 테이블",
                      "customer_id": "고객 고유 ID",
                      "name": "고객 이름",
                      "email": "고객 이메일"
                  },
                  "orders": {
                      "table_description": "주문 정보 테이블",
                      "order_id": "주문 ID",
                      "customer_id": "고객 ID (외래키)"
                  }
              }

    Examples:
        >>> search_database_tables("고객 정보가 필요해")
        {'customers': {'table_description': '고객 정보 테이블', ...}}

    Note:
        이 도구는 다음과 같은 경우에 사용하세요:
        - "어떤 테이블을 사용해야 해?"
        - "고객 관련 테이블 정보를 알려줘"
        - "주문 데이터는 어디에 있어?"
        - "사용 가능한 컬럼을 보여줘"
        - SQL 쿼리를 생성하기 전에 스키마 정보가 필요할 때
    """
    from utils.llm.retrieval import search_tables

    return search_tables(
        query=query, retriever_name=retriever_name, top_n=top_n, device=device
    )


def _simplify_glossary_data(glossary_data):
    """
    용어집 데이터를 name, description, children만 포함하는 간단한 형태로 변환

    Args:
        glossary_data: 처리된 용어집 데이터

    Returns:
        list: 간소화된 용어집 데이터 (name, description, children만 포함)
    """
    if "error" in glossary_data:
        return glossary_data

    result = []

    for node in glossary_data.get("nodes", []):
        simplified_node = {
            "name": node.get("name"),
            "description": node.get("description"),
        }

        # children 정보가 있으면 추가
        if "details" in node and "children" in node["details"]:
            children = []
            for child in node["details"]["children"]:
                child_info = {
                    "name": child.get("name"),
                    "description": child.get("description"),
                }
                children.append(child_info)

            if children:
                simplified_node["children"] = children

        result.append(simplified_node)

    return result


@tool
def get_glossary_terms(query: str, force_update: bool = False) -> list:
    """
    DataHub에서 용어집(Glossary) 정보를 검색합니다.

    이 함수는 사용자의 질문과 관련된 용어 정의를 찾기 위해 Vector Search를 수행합니다.
    용어집은 비즈니스 용어, 도메인 지식, 데이터 정의 등을 표준화하여 관리하는 곳입니다.

    **중요**: 사용자의 질문이나 대화에서 다음과 같은 상황이 발생하면 반드시 이 도구를 사용하세요:
    1. 이해되지 않거나 모호한 단어가 나왔을 때
    2. 특정 조직이나 도메인에서 고유하게 사용되는 전문 용어가 나왔을 때
    3. 일반적이지 않은 약어나 줄임말이 나왔을 때
    4. 조직 내부에서만 통용되는 용어가 나왔을 때
    5. 표준 정의가 필요한 비즈니스 용어가 나왔을 때

    Args:
        query (str): 검색할 용어 또는 관련 질문입니다.
        force_update (bool, optional): True일 경우 데이터를 새로고침하여 검색 인덱스를 재생성합니다. 기본값은 False.

    Returns:
        list: 검색된 용어집 데이터 리스트입니다.
              각 항목은 name, description 등을 포함합니다.

              예시 형태:
              [
                  {
                      "name": "가짜연구소",
                      "description": "스터디 단체 가짜연구소를 의미하며...",
                      "type": "term"
                  },
                  ...
              ]

    Examples:
        >>> get_glossary_terms("PII가 뭐야?")
        [{'name': 'PII', 'description': '개인 식별 정보...', ...}]

    Note:
        이 도구는 다음과 같은 경우에 **반드시** 사용하세요:

        [명시적 요청]
        - "용어집을 보여줘"
        - "비즈니스 용어가 뭐가 있어?"
        - "데이터 사전 정보를 알려줘"
        - "정의된 용어들을 보여줘"

        [이해되지 않는 단어 감지 - 매우 중요!]
        - 일반적이지 않은 약어나 전문 용어가 대화에 등장할 때
        - 표준 정의가 없는 도메인 특화 용어가 나올 때
        - 질문의 맥락에서 모호하거나 불명확한 용어가 있을 때

        [조직/도메인 특화 상황]
        - 특정 조직에서만 사용하는 내부 용어가 나올 때
        - 업계/도메인 전문 용어가 필요할 때
        - 데이터나 테이블 관련 비즈니스 컨텍스트를 이해하기 위해

        **핵심**: 응답하기 전에 사용자의 질문에 조직 특화 용어나 모호한 단어가
        있는지 확인하고, 있다면 먼저 이 도구를 호출하여 정확한 정의를 파악하세요.
    """
    try:
        return search_glossary(query=query, force_update=force_update)

    except Exception as e:
        return {"error": True, "message": f"용어집 조회 중 오류 발생: {str(e)}"}


@tool
def get_query_examples(
    query: str,
    force_update: bool = False,
    count: int = 5,
) -> list:
    """
    DataHub에서 저장된 쿼리 예제들을 검색합니다.

    이 함수는 사용자의 질문과 관련된 SQL 쿼리 예제를 찾기 위해 Vector Search를 수행합니다.
    조직에서 실제로 사용되고 검증된 쿼리 패턴을 참고하여 더 정확한 SQL을 생성할 수 있습니다.

    **중요**: 사용자의 질문이나 대화에서 다음과 같은 상황이 발생하면 반드시 이 도구를 사용하세요:
    1. 일반적인 SQL 패턴으로 해결하기 어려운 복잡한 쿼리 요청일 때
    2. 조직 특화된 비즈니스 로직이나 데이터 처리 방식이 필요할 때
    3. 특정 도메인의 표준 쿼리 패턴이나 관례를 따라야 할 때
    4. 여러 테이블 간의 복잡한 JOIN이나 집계가 필요할 때
    5. 사용자가 과거 실행했던 쿼리와 유사한 작업을 요청할 때
    6. 조직 내에서 검증된 쿼리 작성 방식을 확인해야 할 때

    Args:
        query (str): 검색할 쿼리 관련 질문이나 키워드입니다.
        force_update (bool, optional): True일 경우 데이터를 새로고침하여 검색 인덱스를 재생성합니다. 기본값은 False.
        count (int, optional): 반환할 검색 결과 개수입니다. 기본값은 5.

    Returns:
        list: 쿼리 정보 리스트입니다.
              각 항목은 name, description, statement 필드를 포함합니다.

              예시 형태:
              [
                  {
                      "name": "고객별 주문 수 조회",
                      "description": "각 고객별 주문 건수를 집계하는 쿼리",
                      "statement": "SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id"
                  },
                  ...
              ]

    Examples:
        >>> get_query_examples("매출 집계 쿼리 보여줘")
        [{'name': '월별 매출 현황', 'description': '...', 'statement': 'SELECT ...'}]

    Note:
        이 도구는 다음과 같은 경우에 **반드시** 사용하세요:

        [명시적 요청]
        - "쿼리 예제를 보여줘"
        - "저장된 쿼리들을 알려줘"
        - "과거 쿼리 내역을 보고 싶어"
        - "SQL 예제가 있어?"

        [도메인/조직 특화 패턴 감지 - 매우 중요!]
        - 조직 특화된 데이터 처리 방식이나 계산 로직이 필요할 때
        - 특정 도메인의 관례적인 쿼리 패턴을 따라야 할 때
        - 데이터 품질 규칙이나 비즈니스 룰이 반영된 쿼리가 필요할 때
        - 조직 내에서 표준화된 쿼리 작성 방식을 확인해야 할 때

        [쿼리 작성 참고]
        - "이런 유형의 쿼리는 어떻게 작성해?"
        - "비슷한 쿼리 있어?"
        - "다른 사람들은 어떻게 쿼리를 작성했어?"
        - "참고할만한 쿼리가 있을까?"
        - "이 테이블들을 어떻게 조인해야 해?"

        **핵심**: SQL 쿼리를 생성하기 전에 사용자의 요청이 복잡하거나,
        조직 특화된 비즈니스 로직이 필요하거나, 일반적인 패턴으로 커버하기
        어렵다고 판단되면, 먼저 이 도구를 호출하여 조직에서 검증된
        쿼리 예제를 참고하세요. 이는 더 정확하고 조직의 표준을 따르는
        SQL을 생성하는 데 큰 도움이 됩니다.
    """
    try:
        return search_query_examples(
            query=query, force_update=force_update, top_n=count
        )

    except Exception as e:
        return {
            "error": True,
            "message": f"쿼리 예제 조회 중 오류 발생: {str(e)}",
        }
