"""
DataHub 쿼리 서비스 모듈

DataHub의 쿼리 관련 기능을 제공합니다.
"""

from data_utils.queries import (
    LIST_QUERIES_QUERY,
    QUERIES_BY_URN_QUERY,
)
from data_utils.datahub_services.base_client import DataHubBaseClient


class QueryService:
    """쿼리 관련 서비스 클래스"""

    def __init__(self, client: DataHubBaseClient):
        """
        쿼리 서비스 초기화

        Args:
            client (DataHubBaseClient): DataHub 기본 클라이언트
        """
        self.client = client

    def get_queries(self, start=0, count=10, query="*", filters=None):
        """
        DataHub에서 쿼리 목록을 가져오는 함수

        Args:
            start (int): 시작 인덱스 (기본값=0)
            count (int): 반환할 쿼리 수 (기본값=10)
            query (str): 필터링에 사용할 쿼리 문자열 (기본값="*")
            filters (list): 추가 필터 (기본값=None)

        Returns:
            dict: 쿼리 목록 정보
        """
        # GraphQL 요청용 입력 변수 준비
        input_params = {"start": start, "count": count, "query": query}

        if filters:
            input_params["filters"] = filters

        variables = {"input": input_params}

        return self.client.execute_graphql_query(LIST_QUERIES_QUERY, variables)

    def process_queries(self, result):
        """
        쿼리 목록 결과를 처리하고 간소화된 형태로 반환하는 함수

        Args:
            result (dict): API 응답 결과

        Returns:
            dict: 처리된 쿼리 목록 데이터 (urn, name, description, statement만 포함)
        """
        if "error" in result:
            return result

        processed_result = {"total_queries": 0, "count": 0, "start": 0, "queries": []}

        if "data" in result and "listQueries" in result["data"]:
            list_queries = result["data"]["listQueries"]
            processed_result["total_queries"] = list_queries.get("total", 0)
            processed_result["count"] = list_queries.get("count", 0)
            processed_result["start"] = list_queries.get("start", 0)

            for query in list_queries.get("queries", []):
                query_info = {"urn": query.get("urn")}

                props = query.get("properties", {})
                query_info["name"] = props.get("name")
                query_info["description"] = props.get("description")
                query_info["statement"] = props.get("statement", {}).get("value")

                processed_result["queries"].append(query_info)

        return processed_result

    def get_query_data(self, start=0, count=10, query="*", filters=None):
        """
        DataHub에서 쿼리 목록을 가져와 처리하는 함수

        Args:
            start (int): 시작 인덱스 (기본값=0)
            count (int): 반환할 쿼리 수 (기본값=10)
            query (str): 필터링에 사용할 쿼리 문자열 (기본값="*")
            filters (list): 추가 필터 (기본값=None)

        Returns:
            dict: 처리된 쿼리 목록 데이터
        """
        # DataHub 서버에 연결하여 쿼리 목록 가져오기
        result = self.get_queries(start, count, query, filters)

        # 결과 처리
        if result:
            try:
                return self.process_queries(result)
            except KeyError as e:
                return {"error": True, "message": f"결과 구조 파싱 중 오류 발생: {e}"}
        else:
            return {"error": True, "message": "쿼리 목록을 가져오지 못했습니다."}

    def get_queries_by_urn(self, dataset_urn):
        """
        특정 데이터셋 URN과 연관된 쿼리들을 조회하는 함수

        전체 쿼리를 가져온 후 클라이언트 사이드에서 필터링하는 방식 사용

        Args:
            dataset_urn (str): 데이터셋 URN

        Returns:
            dict: 연관된 쿼리 목록
        """
        # 먼저 전체 쿼리 목록을 가져옴
        input_params = {"start": 0, "count": 1000, "query": "*"}  # 충분히 큰 수로 설정

        variables = {"input": input_params}
        result = self.client.execute_graphql_query(QUERIES_BY_URN_QUERY, variables)

        if (
            "error" not in result
            and "data" in result
            and "listQueries" in result["data"]
        ):
            # 클라이언트 사이드에서 특정 URN과 연관된 쿼리만 필터링
            all_queries = result["data"]["listQueries"]["queries"]
            filtered_queries = []

            for query in all_queries:
                subjects = query.get("subjects", [])
                for subject in subjects:
                    if subject.get("dataset", {}).get("urn") == dataset_urn:
                        filtered_queries.append(query)
                        break

            # 필터링된 결과로 응답 구조 재구성
            result["data"]["listQueries"]["queries"] = filtered_queries
            result["data"]["listQueries"]["count"] = len(filtered_queries)

        return result

    def get_glossary_terms_by_urn(self, dataset_urn):
        """
        특정 데이터셋 URN의 glossary terms를 조회하는 함수

        Args:
            dataset_urn (str): 데이터셋 URN

        Returns:
            dict: glossary terms 정보
        """
        from data_utils.queries import GLOSSARY_TERMS_BY_URN_QUERY

        variables = {"urn": dataset_urn}
        return self.client.execute_graphql_query(GLOSSARY_TERMS_BY_URN_QUERY, variables)
