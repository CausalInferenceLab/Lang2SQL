"""
DataHub 기본 클라이언트 모듈

DataHub GMS 서버와의 기본 연결 및 통신 기능을 제공합니다.
"""

import requests
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph


class DataHubBaseClient:
    """DataHub 기본 클라이언트 클래스"""

    def __init__(self, gms_server="http://localhost:8080", extra_headers={}):
        """
        DataHub 클라이언트 초기화

        Args:
            gms_server (str): DataHub GMS 서버 URL
            extra_headers (dict): 추가 HTTP 헤더
        """
        # gms_server 주소 유효성 검사
        if not self._is_valid_gms_server(gms_server):
            raise ValueError(f"유효하지 않은 GMS 서버 주소: {gms_server}")

        self.gms_server = gms_server
        self.extra_headers = extra_headers

        # DataHub 클라이언트 초기화
        self.emitter = DatahubRestEmitter(
            gms_server=gms_server, extra_headers=extra_headers
        )
        self.datahub_graph = self.emitter.to_graph()

    def _is_valid_gms_server(self, gms_server):
        """
        GMS 서버 주소의 유효성을 검사하는 함수

        Args:
            gms_server (str): 검사할 GMS 서버 URL

        Returns:
            bool: 서버가 유효한 경우 True
        """
        query = {"query": "{ health { status } }"}
        headers = {"Content-Type": "application/json"}

        try:
            response = requests.post(
                f"{gms_server}/api/graphql", json=query, headers=headers
            )
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False

    def execute_graphql_query(self, query, variables=None):
        """
        GraphQL 쿼리 실행

        Args:
            query (str): GraphQL 쿼리 문자열
            variables (dict, optional): 쿼리 변수

        Returns:
            dict: GraphQL 응답
        """
        headers = {"Content-Type": "application/json"}
        payload = {"query": query}

        if variables:
            payload["variables"] = variables

        response = requests.post(
            f"{self.gms_server}/api/graphql",
            json=payload,
            headers=headers,
        )

        if response.status_code == 200:
            return response.json()
        else:
            return {
                "error": True,
                "status_code": response.status_code,
                "message": response.text,
            }

    def get_datahub_graph(self):
        """DataHub Graph 클라이언트 반환"""
        return self.datahub_graph

    def get_urns(self):
        """필터를 적용하여 데이터셋의 URN 가져오기"""
        return self.datahub_graph.get_urns_by_filter()
