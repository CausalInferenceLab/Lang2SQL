"""
DataHub 메타데이터 페처 - 리팩토링된 버전

기존 DatahubMetadataFetcher의 모든 기능을 유지하면서
내부적으로는 분리된 서비스 모듈들을 사용합니다.

기존 코드와의 완벽한 호환성을 보장합니다.
"""

from data_utils.datahub_services.base_client import DataHubBaseClient
from data_utils.datahub_services.metadata_service import MetadataService
from data_utils.datahub_services.query_service import QueryService
from data_utils.datahub_services.glossary_service import GlossaryService


class DatahubMetadataFetcher:
    """
    DataHub 메타데이터 페처 - 기존 인터페이스 유지

    내부적으로는 분리된 서비스들을 사용하지만
    외부 인터페이스는 기존과 동일하게 유지됩니다.
    """

    def __init__(self, gms_server="http://localhost:8080", extra_headers={}):
        """
        DataHub 메타데이터 페처 초기화

        Args:
            gms_server (str): DataHub GMS 서버 URL
            extra_headers (dict): 추가 HTTP 헤더
        """
        # 기본 클라이언트 초기화
        self.client = DataHubBaseClient(gms_server, extra_headers)

        # 서비스들 초기화
        self.metadata_service = MetadataService(self.client)
        self.query_service = QueryService(self.client)
        self.glossary_service = GlossaryService(self.client)

        # 기존 속성들 호환성을 위해 유지
        self.gms_server = gms_server
        self.emitter = self.client.emitter
        self.datahub_graph = self.client.datahub_graph

    # === 기존 인터페이스 유지 - 메타데이터 관련 ===

    def get_urns(self):
        """필터를 적용하여 데이터셋의 URN 가져오기"""
        return self.client.get_urns()

    def get_table_name(self, urn):
        """URN에 대한 테이블 이름 가져오기"""
        return self.metadata_service.get_table_name(urn)

    def get_table_description(self, urn):
        """URN에 대한 테이블 설명 가져오기"""
        return self.metadata_service.get_table_description(urn)

    def get_column_names_and_descriptions(self, urn):
        """URN에 대한 컬럼 이름 및 설명 가져오기"""
        return self.metadata_service.get_column_names_and_descriptions(urn)

    def get_table_lineage(
        self, urn, counts=100, direction="DOWNSTREAM", degree_values=None
    ):
        """URN에 대한 DOWNSTREAM/UPSTREAM lineage entity를 counts 만큼 가져오는 함수"""
        return self.metadata_service.get_table_lineage(
            urn, counts, direction, degree_values
        )

    def get_column_lineage(self, urn):
        """URN에 대한 UPSTREAM lineage의 column source를 가져오는 함수"""
        return self.metadata_service.get_column_lineage(urn)

    def min_degree_lineage(self, lineage_result):
        """lineage 중 최소 degree만 가져오는 함수"""
        return self.metadata_service.min_degree_lineage(lineage_result)

    def build_table_metadata(self, urn, max_degree=2, sort_by_degree=True):
        """테이블 단위로 테이블 이름, 설명, 컬럼, 테이블 별 리니지(downstream/upstream), 컬럼 별 리니지(upstream)이 포함된 메타데이터 생성 함수"""
        return self.metadata_service.build_table_metadata(
            urn, max_degree, sort_by_degree
        )

    def get_urn_info(self, urn):
        """특정 URN에 대한 모든 관련 정보를 가져오는 함수"""
        return self.metadata_service.get_urn_info(urn)

    def _print_urn_details(self, metadata):
        """URN 메타데이터를 보기 좋게 출력하는 내부 함수"""
        return self.metadata_service._print_urn_details(metadata)

    # === 기존 인터페이스 유지 - 용어집 관련 ===

    def get_root_glossary_nodes(self):
        """DataHub에서 루트 용어집 노드를 가져오는 함수"""
        return self.glossary_service.get_root_glossary_nodes()

    def get_glossary_node_by_urn(self, urn):
        """DataHub에서 특정 URN의 용어집 노드 및 그 자식 항목을 가져오는 함수"""
        return self.glossary_service.get_glossary_node_by_urn(urn)

    def get_node_basic_info(self, node, index):
        """용어집 노드의 기본 정보를 딕셔너리로 반환하는 함수"""
        return self.glossary_service.get_node_basic_info(node, index)

    def get_child_entity_info(self, entity, index):
        """자식 엔티티(용어 또는 노드)의 정보를 딕셔너리로 반환하는 함수"""
        return self.glossary_service.get_child_entity_info(entity, index)

    def process_node_details(self, node):
        """노드의 상세 정보를 처리하고 딕셔너리로 반환하는 함수"""
        return self.glossary_service.process_node_details(node)

    def process_glossary_nodes(self, result):
        """용어집 노드 결과를 처리하고 딕셔너리로 반환하는 함수"""
        return self.glossary_service.process_glossary_nodes(result)

    def get_glossary_data(self):
        """DataHub에서 전체 용어집 데이터를 가져와 처리하는 함수"""
        return self.glossary_service.get_glossary_data()

    def get_queries(self, start=0, count=10, query="*", filters=None):
        """DataHub에서 쿼리 목록을 가져오는 함수"""
        return self.query_service.get_queries(start, count, query, filters)

    def process_queries(self, result):
        """쿼리 목록 결과를 처리하고 간소화된 형태로 반환하는 함수"""
        return self.query_service.process_queries(result)

    def get_query_data(self, start=0, count=10, query="*", filters=None):
        """DataHub에서 쿼리 목록을 가져와 처리하는 함수"""
        return self.query_service.get_query_data(start, count, query, filters)

    def get_queries_by_urn(self, dataset_urn):
        """특정 데이터셋 URN과 연관된 쿼리들을 조회하는 함수"""
        return self.query_service.get_queries_by_urn(dataset_urn)

    def get_glossary_terms_by_urn(self, dataset_urn):
        """특정 데이터셋 URN의 glossary terms를 조회하는 함수"""
        return self.glossary_service.get_glossary_terms_by_urn(dataset_urn)

    def _is_valid_gms_server(self, gms_server):
        """GMS 서버 주소의 유효성을 검사하는 함수 (하위 호환성)"""
        return self.client._is_valid_gms_server(gms_server)
