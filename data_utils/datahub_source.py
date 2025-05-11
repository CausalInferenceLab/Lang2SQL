import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import DatasetPropertiesClass, SchemaMetadataClass
from datahub.emitter.rest_emitter import DatahubRestEmitter
import requests


# "http://localhost:8080"
class DatahubMetadataFetcher:
    def __init__(self, gms_server="http://localhost:8080", extra_headers={}):
        # gms_server 주소 유효성 검사
        if not self._is_valid_gms_server(gms_server):
            raise ValueError(f"유효하지 않은 GMS 서버 주소: {gms_server}")

        self.emitter = DatahubRestEmitter(
            gms_server=gms_server, extra_headers=extra_headers
        )
        self.datahub_graph = self.emitter.to_graph()

    def _is_valid_gms_server(self, gms_server):
        # GMS 서버 주소의 유효성을 검사하는 로직 추가
        # GraphQL 요청을 사용하여 서버 상태 확인
        query = {"query": "{ health { status } }"}
        headers = {"Content-Type": "application/json"}

        try:
            response = requests.post(
                f"{gms_server}/api/graphql", json=query, headers=headers
            )
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False

    def get_urns(self):
        # 필터를 적용하여 데이터셋의 URN 가져오기
        return self.datahub_graph.get_urns_by_filter()

    def get_table_name(self, urn):
        # URN에 대한 테이블 이름 가져오기
        dataset_properties = self.datahub_graph.get_aspect(
            urn, aspect_type=DatasetPropertiesClass
        )
        if dataset_properties:
            return dataset_properties.get("name", None)
        return None

    def get_table_description(self, urn):
        # URN에 대한 테이블 설명 가져오기
        dataset_properties = self.datahub_graph.get_aspect(
            urn, aspect_type=DatasetPropertiesClass
        )
        if dataset_properties:
            return dataset_properties.get("description", None)
        return None

    def get_column_names_and_descriptions(self, urn):
        # URN에 대한 컬럼 이름 및 설명 가져오기
        schema_metadata = self.datahub_graph.get_aspect(
            urn, aspect_type=SchemaMetadataClass
        )
        columns = []
        if schema_metadata:
            for field in schema_metadata.fields:
                columns.append(
                    {
                        "column_name": field.fieldPath,
                        "column_description": field.description,
                    }
                )
        return columns
