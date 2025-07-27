"""
DataHub 용어집 서비스 모듈

DataHub의 glossary 관련 기능을 제공합니다.
"""

from data_utils.queries import (
    ROOT_GLOSSARY_NODES_QUERY,
    GLOSSARY_NODE_QUERY,
    GLOSSARY_TERMS_BY_URN_QUERY,
)
from data_utils.datahub_services.base_client import DataHubBaseClient


class GlossaryService:
    """용어집 관련 서비스 클래스"""

    def __init__(self, client: DataHubBaseClient):
        """
        용어집 서비스 초기화

        Args:
            client (DataHubBaseClient): DataHub 기본 클라이언트
        """
        self.client = client

    def get_root_glossary_nodes(self):
        """
        DataHub에서 루트 용어집 노드를 가져오는 함수

        Returns:
            dict: 루트 용어집 노드 정보
        """
        return self.client.execute_graphql_query(ROOT_GLOSSARY_NODES_QUERY)

    def get_glossary_node_by_urn(self, urn):
        """
        DataHub에서 특정 URN의 용어집 노드 및 그 자식 항목을 가져오는 함수

        Args:
            urn (str): 용어집 노드의 URN

        Returns:
            dict: 용어집 노드 정보와 자식 항목
        """
        variables = {"urn": urn}
        return self.client.execute_graphql_query(GLOSSARY_NODE_QUERY, variables)

    def get_node_basic_info(self, node, index):
        """
        용어집 노드의 기본 정보를 딕셔너리로 반환하는 함수

        Args:
            node (dict): 용어집 노드 정보
            index (int): 노드의 인덱스

        Returns:
            dict: 노드의 기본 정보
        """
        result = {"index": index, "name": node["properties"]["name"]}

        if node["properties"] and node["properties"].get("description"):
            result["description"] = node["properties"]["description"]

        # 자식 노드/용어 관계 정보 수 추가
        if "children" in node and node["children"]["total"] > 0:
            result["child_count"] = node["children"]["total"]

        return result

    def get_child_entity_info(self, entity, index):
        """
        자식 엔티티(용어 또는 노드)의 정보를 딕셔너리로 반환하는 함수

        Args:
            entity (dict): 자식 엔티티 정보
            index (int): 엔티티의 인덱스

        Returns:
            dict: 엔티티 정보
        """
        entity_type = entity["type"]
        result = {"index": index, "type": entity_type}

        if entity_type == "GLOSSARY_TERM":
            if "properties" in entity and entity["properties"]:
                result["name"] = entity["properties"].get("name", "N/A")

                if (
                    "description" in entity["properties"]
                    and entity["properties"]["description"]
                ):
                    result["description"] = entity["properties"]["description"]

        elif entity_type == "GLOSSARY_NODE":
            if "properties" in entity and entity["properties"]:
                result["name"] = entity["properties"].get("name", "N/A")

        return result

    def process_node_details(self, node):
        """
        노드의 상세 정보를 처리하고 딕셔너리로 반환하는 함수

        Args:
            node (dict): 용어집 노드 정보

        Returns:
            dict: 노드의 상세 정보
        """
        node_urn = node["urn"]
        detailed_node = self.get_glossary_node_by_urn(node_urn)

        result = {"name": node["properties"]["name"], "children": []}

        if (
            detailed_node
            and "data" in detailed_node
            and "glossaryNode" in detailed_node["data"]
        ):
            node_detail = detailed_node["data"]["glossaryNode"]

            # 자식 항목 정보 추출
            if "children" in node_detail and node_detail["children"]["total"] > 0:
                relationships = node_detail["children"]["relationships"]

                for j, rel in enumerate(relationships, 1):
                    entity = rel["entity"]
                    result["children"].append(self.get_child_entity_info(entity, j))

        return result

    def process_glossary_nodes(self, result):
        """
        용어집 노드 결과를 처리하고 딕셔너리로 반환하는 함수

        Args:
            result (dict): API 응답 결과

        Returns:
            dict: 처리된 용어집 노드 데이터
        """
        if "error" in result:
            return result

        processed_result = {"total_nodes": 0, "nodes": []}

        # 노드 목록 추출
        nodes = result["data"]["getRootGlossaryNodes"]["nodes"]
        processed_result["total_nodes"] = len(nodes)

        for i, node in enumerate(nodes, 1):
            node_info = self.get_node_basic_info(node, i)

            # 자식 노드가 있으면 상세 정보 처리
            if "children" in node and node["children"]["total"] > 0:
                node_details = self.process_node_details(node)
                node_info["details"] = node_details

            processed_result["nodes"].append(node_info)

        return processed_result

    def get_glossary_data(self):
        """
        DataHub에서 전체 용어집 데이터를 가져와 처리하는 함수

        Returns:
            dict: 처리된 용어집 데이터
        """
        # DataHub 서버에 연결하여 용어집 노드 가져오기
        result = self.get_root_glossary_nodes()

        # 결과 처리
        if result:
            try:
                return self.process_glossary_nodes(result)
            except KeyError as e:
                return {"error": True, "message": f"결과 구조 파싱 중 오류 발생: {e}"}
        else:
            return {"error": True, "message": "용어집 노드를 가져오지 못했습니다."}

    def get_glossary_terms_by_urn(self, dataset_urn):
        """
        특정 데이터셋 URN의 glossary terms를 조회하는 함수

        Args:
            dataset_urn (str): 데이터셋 URN

        Returns:
            dict: glossary terms 정보
        """
        variables = {"urn": dataset_urn}
        return self.client.execute_graphql_query(GLOSSARY_TERMS_BY_URN_QUERY, variables)
