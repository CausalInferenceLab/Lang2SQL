import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import DatasetPropertiesClass, SchemaMetadataClass
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import UpstreamLineageClass
from collections import defaultdict
import requests


class DatahubMetadataFetcher:
    def __init__(self, gms_server="http://localhost:8080", extra_headers={}):
        # gms_server 주소 유효성 검사
        if not self._is_valid_gms_server(gms_server):
            raise ValueError(f"유효하지 않은 GMS 서버 주소: {gms_server}")

        self.emitter = DatahubRestEmitter(
            gms_server=gms_server, extra_headers=extra_headers
        )
        self.datahub_graph = self.emitter.to_graph()
        self.gms_server = gms_server

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

    def get_table_lineage(
        self,
        urn,
        counts=100,
        direction="DOWNSTREAM",
        degree_values=None,
    ):
        # URN에 대한 DOWNSTREAM/UPSTREAM lineage entity를 counts 만큼 가져오는 함수
        # degree_values에 따라 lineage depth가 결정
        """
        Fetches downstream/upstream lineage entities for a given dataset URN using DataHub's GraphQL API.

        Args:
            urn (str): Dataset URN to fetch lineage for.
            count (int): Maximum number of entities to fetch (default=100).
            direction (str): DOWNSTREAM or UPSTREAM.
            degree_values (List[str]): Degree filter values like ["1", "2", "3+"]. Defaults to ["1", "2"].

        Returns:
            List[str, dict]: A list containing the dataset URN and its lineage result.
        """

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
        # URN에 대한 UPSTREAM lineage의 column source를 가져오는 함수
        """
        Fetches fine-grained column-level lineage grouped by upstream datasets.

        Args:
            urn (str): Dataset URN to fetch lineage for.

        Returns:
            dict: {
                'downstream_dataset': str,
                'lineage_by_upstream_dataset': List[{
                    'upstream_dataset': str,
                    'columns': List[{'upstream_column': str, 'downstream_column': str}]
                }]
            }
        """

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
        # lineage 중 최소 degree만 가져오는 함수
        """
        Returns the minimum degree from the lineage result (fetched by get_table_lineage().)

        Args:
            lineage_result : (List[str, dict]): Result from get_table_lineage().

        Returns:
            dict : {table_name : minimum_degree}
        """

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
        # 테이블 단위로 테이블 이름, 설명, 컬럼, 테이블 별 리니지(downstream/upstream), 컬럼 별 리니지(upstream)이 포함된 메타데이터 생성 함수
        """
        Builds table metadata including description, columns, and lineage info.

        Args:
            urn (str): Dataset URN
            max_degree (int): Max lineage depth to include (filtering)
            sort_by_degree (bool): Whether to sort downstream/upstream tables by degree

        Returns:
            dict: Table metadata
        """
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

            # degree 필터링
            filtered_lineage = [
                {"table": table, "degree": degree}
                for table, degree in table_degrees.items()
                if degree <= max_degree
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
