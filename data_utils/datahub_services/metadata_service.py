"""
DataHub 메타데이터 서비스 모듈

테이블 메타데이터, 리니지, URN 관련 기능을 제공합니다.
"""

from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    SchemaMetadataClass,
    UpstreamLineageClass,
)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from collections import defaultdict

from data_utils.datahub_services.base_client import DataHubBaseClient


class MetadataService:
    """메타데이터 관련 서비스 클래스"""

    def __init__(self, client: DataHubBaseClient):
        """
        메타데이터 서비스 초기화

        Args:
            client (DataHubBaseClient): DataHub 기본 클라이언트
        """
        self.client = client
        self.datahub_graph = client.get_datahub_graph()
        self.gms_server = client.gms_server

    def get_table_name(self, urn):
        """URN에 대한 테이블 이름 가져오기"""
        dataset_properties = self.datahub_graph.get_aspect(
            urn, aspect_type=DatasetPropertiesClass
        )
        if dataset_properties:
            database_info = dataset_properties.get("customProperties", {}).get(
                "dbt_unique_id", ""
            )
            if database_info:
                database_info = database_info.split(".")[-2]
            else:
                database_info = ""
            table_info = dataset_properties.get("name", None)
            return database_info + "." + table_info
        return None

    def get_table_description(self, urn):
        """URN에 대한 테이블 설명 가져오기"""
        dataset_properties = self.datahub_graph.get_aspect(
            urn, aspect_type=DatasetPropertiesClass
        )
        if dataset_properties:
            return dataset_properties.get("description", None)
        return None

    def get_column_names_and_descriptions(self, urn):
        """URN에 대한 컬럼 이름 및 설명 가져오기"""
        schema_metadata = self.datahub_graph.get_aspect(
            urn, aspect_type=SchemaMetadataClass
        )
        columns = []
        if schema_metadata:
            for field in schema_metadata.fields:
                # nativeDataType가 없거나 빈 문자열인 경우 None 처리
                native_type = getattr(field, "nativeDataType", None)
                column_type = (
                    native_type if native_type and native_type.strip() else None
                )

                columns.append(
                    {
                        "column_name": field.fieldPath,
                        "column_description": field.description,
                        "column_type": column_type,
                    }
                )
        return columns

    def get_table_lineage(
        self,
        urn,
        counts=100,
        direction="DOWNSTREAM",
        degree_values=None,
    ):
        """URN에 대한 DOWNSTREAM/UPSTREAM lineage entity를 counts 만큼 가져오는 함수"""
        if degree_values is None:
            degree_values = ["1", "2"]

        graph = DataHubGraph(DatahubClientConfig(server=self.gms_server))

        query = """
            query scrollAcrossLineage($input: ScrollAcrossLineageInput!) {
            scrollAcrossLineage(input: $input) {
                searchResults {
                    degree
                    entity {
                        urn
                        type
                    }
                }
            }
        }
        """
        variables = {
            "input": {
                "query": "*",
                "urn": urn,
                "count": counts,
                "direction": direction,
                "orFilters": [
                    {
                        "and": [
                            {
                                "condition": "EQUAL",
                                "negated": "false",
                                "field": "degree",
                                "values": degree_values,
                            }
                        ]
                    }
                ],
            }
        }

        result = graph.execute_graphql(query=query, variables=variables)
        return urn, result

    def get_column_lineage(self, urn):
        """URN에 대한 UPSTREAM lineage의 column source를 가져오는 함수"""
        # DataHub 연결 및 lineage 가져오기
        graph = DataHubGraph(DatahubClientConfig(server=self.gms_server))
        result = graph.get_aspect(entity_urn=urn, aspect_type=UpstreamLineageClass)

        # downstream dataset (URN 테이블명) 파싱
        try:
            down_dataset = urn.split(",")[1]
            table_name = down_dataset.split(".")[1]
        except IndexError:
            # URN이 유효하지 않는 경우
            print(f"[ERROR] Invalid URN format: {urn}")
            return {}

        # upstream_dataset별로 column lineage
        upstream_map = defaultdict(list)

        if not result:
            return {"downstream_dataset": table_name, "lineage_by_upstream_dataset": []}

        for fg in result.fineGrainedLineages or []:
            confidence_score = (
                fg.confidenceScore if fg.confidenceScore is not None else 1.0
            )
            for down in fg.downstreams:
                down_column = down.split(",")[-1].replace(")", "")
                for up in fg.upstreams:
                    up_dataset = up.split(",")[1]
                    up_dataset = up_dataset.split(".")[1]
                    up_column = up.split(",")[-1].replace(")", "")

                    upstream_map[up_dataset].append(
                        {
                            "upstream_column": up_column,
                            "downstream_column": down_column,
                            "confidence": confidence_score,
                        }
                    )

        # 최종 결과 구조 생성
        parsed_lineage = {
            "downstream_dataset": table_name,
            "lineage_by_upstream_dataset": [],
        }

        for up_dataset, column_mappings in upstream_map.items():
            parsed_lineage["lineage_by_upstream_dataset"].append(
                {"upstream_dataset": up_dataset, "columns": column_mappings}
            )

        return parsed_lineage

    def min_degree_lineage(self, lineage_result):
        """lineage 중 최소 degree만 가져오는 함수"""
        table_degrees = {}
        urn, lineage_data = lineage_result

        for item in lineage_data["scrollAcrossLineage"]["searchResults"]:
            table = item["entity"]["urn"].split(",")[1]
            table_name = table.split(".")[1]
            degree = item["degree"]
            table_degrees[table_name] = min(
                degree, table_degrees.get(table_name, float("inf"))
            )

        return table_degrees

    def build_table_metadata(self, urn, max_degree=2, sort_by_degree=True):
        """테이블 단위로 테이블 이름, 설명, 컬럼, 테이블 별 리니지(downstream/upstream), 컬럼 별 리니지(upstream)이 포함된 메타데이터 생성 함수"""
        metadata = {
            "table_name": self.get_table_name(urn),
            "description": self.get_table_description(urn),
            "columns": self.get_column_names_and_descriptions(urn),
            "lineage": {},
        }

        def process_lineage(direction):
            # direction : DOWNSTREAM/UPSTREAM 별로 degree가 최소인 lineage를 가져오는 함수
            # 테이블 lineage 가져오기
            lineage_result = self.get_table_lineage(urn, direction=direction)
            table_degrees = self.min_degree_lineage(lineage_result)
            current_table_name = metadata["table_name"]

            # degree 필터링
            filtered_lineage = [
                {"table": table, "degree": degree}
                for table, degree in table_degrees.items()
                if degree <= max_degree and table != current_table_name
            ]

            # degree 기준 정렬
            if sort_by_degree:
                filtered_lineage.sort(key=lambda x: x["degree"])

            return filtered_lineage

        # DOWNSTREAM / UPSTREAM 링크 추가
        metadata["lineage"]["downstream"] = process_lineage("DOWNSTREAM")
        metadata["lineage"]["upstream"] = process_lineage("UPSTREAM")

        # 컬럼 단위 lineage 추가
        column_lineage = self.get_column_lineage(urn)
        metadata["lineage"]["upstream_columns"] = column_lineage.get(
            "lineage_by_upstream_dataset", []
        )

        return metadata

    def get_urn_info(self, urn):
        """
        특정 URN에 대한 모든 관련 정보를 가져오는 함수

        Args:
            urn (str): 조회할 데이터셋 URN

        Returns:
            dict: URN에 대한 전체 메타데이터 정보
        """
        print(f"\n=== URN 정보 조회: {urn} ===\n")

        try:
            # 기본 테이블 메타데이터 가져오기
            metadata = self.build_table_metadata(urn)

            # 결과 출력
            self._print_urn_details(metadata)

            return metadata

        except Exception as e:
            error_msg = f"URN 정보 조회 중 오류 발생: {str(e)}"
            print(error_msg)
            return {"error": True, "message": error_msg}

    def _print_urn_details(self, metadata):
        """URN 메타데이터를 보기 좋게 출력하는 내부 함수"""

        # 테이블 기본 정보
        print("📋 테이블 정보:")
        print(f"  이름: {metadata.get('table_name', 'N/A')}")
        print(f"  설명: {metadata.get('description', 'N/A')}\n")

        # 컬럼 정보
        columns = metadata.get("columns", [])
        if columns:
            print(f"📊 컬럼 정보 ({len(columns)}개):")
            for i, col in enumerate(columns, 1):
                print(f"  {i}. {col['column_name']} ({col.get('column_type', 'N/A')})")
                if col.get("column_description"):
                    print(f"     → {col['column_description']}")
            print()

        # 리니지 정보
        lineage = metadata.get("lineage", {})

        # Downstream 테이블
        downstream = lineage.get("downstream", [])
        if downstream:
            print(f"⬇️ Downstream 테이블 ({len(downstream)}개):")
            for table in downstream:
                print(f"  - {table['table']} (degree: {table['degree']})")
            print()

        # Upstream 테이블
        upstream = lineage.get("upstream", [])
        if upstream:
            print(f"⬆️ Upstream 테이블 ({len(upstream)}개):")
            for table in upstream:
                print(f"  - {table['table']} (degree: {table['degree']})")
            print()

        # 컬럼 레벨 리니지
        upstream_columns = lineage.get("upstream_columns", [])
        if upstream_columns:
            print("🔗 컬럼 레벨 리니지:")
            for upstream_dataset in upstream_columns:
                dataset_name = upstream_dataset["upstream_dataset"]
                columns = upstream_dataset["columns"]
                print(f"  📋 {dataset_name}:")
                for col in columns:
                    confidence = col.get("confidence", 1.0)
                    print(
                        f"    {col['upstream_column']} → {col['downstream_column']} (신뢰도: {confidence})"
                    )
            print()
