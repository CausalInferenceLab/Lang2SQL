from qdrant_client import QdrantClient, models
from typing import List, Dict, Any, Optional, Union, Callable
import os
from dotenv import load_dotenv

load_dotenv()


class QdrantDB:
    def __init__(
        self, url: str = "http://localhost:6333", api_key: Optional[str] = None
    ):
        """
        Qdrant 클라이언트를 초기화합니다.

        Args:
            url: Qdrant 서버 URL.
            api_key: Qdrant 클라우드 또는 인증된 인스턴스를 위한 API 키.
        """
        self.client = QdrantClient(url=url, api_key=api_key)

    def create_collection(
        self, collection_name: str, dense_dim: int = 1536, colbert_dim: int = 128
    ):
        """
        Dense, ColBERT, Sparse 벡터 구성을 포함한 컬렉션을 생성합니다.

        Args:
            collection_name: 생성할 컬렉션의 이름.
            dense_dim: Dense 벡터의 차원 (기본값: OpenAI small 모델 기준 1536).
            colbert_dim: ColBERT 벡터의 차원 (기본값: 128).
        """
        if not self.client.collection_exists(collection_name):
            self.client.create_collection(
                collection_name=collection_name,
                vectors_config={
                    "dense": models.VectorParams(
                        size=dense_dim, distance=models.Distance.COSINE
                    ),
                    "colbert": models.VectorParams(
                        size=colbert_dim,
                        distance=models.Distance.COSINE,
                        multivector_config=models.MultiVectorConfig(
                            comparator=models.MultiVectorComparator.MAX_SIM
                        ),
                        hnsw_config=models.HnswConfigDiff(m=0),
                    ),
                },
                sparse_vectors_config={"sparse": models.SparseVectorParams()},
            )
            print(f"Collection '{collection_name}' created.")
        else:
            print(f"Collection '{collection_name}' already exists.")

    def upsert(self, collection_name: str, points: List[Dict[str, Any]]):
        """
        컬렉션에 포인트들을 업서트(Upsert)합니다.

        Args:
            collection_name: 컬렉션 이름.
            points: 다음 항목들을 포함하는 딕셔너리 리스트:
                - id: 고유 식별자 (int 또는 str)
                - vector: 'dense', 'colbert', 'sparse' 키와 해당 벡터 값을 포함하는 딕셔너리.
                - payload: 메타데이터를 포함하는 딕셔너리.
        """
        point_structs = []
        for point in points:
            if "id" not in point or "vector" not in point:
                raise ValueError("Each point must contain 'id' and 'vector' keys.")

            point_structs.append(
                models.PointStruct(
                    id=point["id"],
                    vector=point["vector"],
                    payload=point.get("payload", {}),
                )
            )

        self.client.upload_points(collection_name=collection_name, points=point_structs)
        print(
            f"Successfully upserted {len(point_structs)} points to '{collection_name}'."
        )

    def search(
        self,
        collection_name: str,
        query_vector: Union[List[float], tuple],
        query_filter: Optional[models.Filter] = None,
        limit: int = 10,
        with_payload: bool = True,
    ) -> List[models.ScoredPoint]:
        """
        특정 컬렉션에서 벡터 검색을 수행합니다.

        Args:
            collection_name: 검색할 컬렉션의 이름.
            query_vector: 검색에 사용할 쿼리 벡터. 명명된 벡터를 사용하는 경우 ('vector_name', vector) 튜플로 전달해야 합니다.
            query_filter: 검색 시 적용할 필터 (선택 사항).
            limit: 반환할 결과의 최대 개수 (기본값: 10).
            with_payload: 결과에 페이로드를 포함할지 여부 (기본값: True).

        Returns:
            검색 결과 리스트 (ScoredPoint 객체들의 리스트).
        """
        print("This is QdrantDB search")
        return self.client.search(
            collection_name=collection_name,
            query_vector=query_vector,
            query_filter=query_filter,
            limit=limit,
            with_payload=with_payload,
        )

    def similarity_search(
        self, query: str, k: int = 5, collection_name: str = "lang2sql_table_schema"
    ) -> List[Any]:
        """
        LangChain 호환성을 위한 유사도 검색 메서드.

        Args:
            query: 검색 쿼리 문자열.
            k: 반환할 결과 개수.
            collection_name: 검색할 컬렉션 이름.

        Returns:
            LangChain Document 객체 리스트.
        """
        from langchain.schema import Document
        from utils.llm.core import get_embeddings

        embeddings = get_embeddings()
        query_vector = embeddings.embed_query(query)

        results = self.search(
            collection_name=collection_name,
            query_vector=("dense", query_vector),
            limit=k,
        )

        documents = []
        for res in results:
            payload = res.payload
            # payload를 page_content와 metadata로 변환
            # 여기서는 payload의 모든 내용을 metadata로 넣고,
            # 특정 필드를 page_content로 구성하거나 payload 전체를 문자열로 변환

            # 기존 faiss_db.py의 로직을 참고하여 page_content 구성
            # table_name: table_description
            # Columns:
            #  col1: desc1

            table_name = payload.get("table_name", "Unknown Table")
            table_description = payload.get("table_description", "")
            columns = payload.get("columns", {})

            column_info_str = "\n".join(
                [f"{key}: {val}" for key, val in columns.items()]
            )
            page_content = (
                f"{table_name}: {table_description}\nColumns:\n {column_info_str}"
            )

            documents.append(Document(page_content=page_content, metadata=payload))

        return documents

    def as_retriever(self, search_kwargs: Optional[Dict] = None):
        """
        LangChain Retriever 인터페이스 호환 메서드.
        """
        return self

    def invoke(self, query: str):
        """
        Retriever 인터페이스의 invoke 메서드 구현.
        """
        # search_kwargs에서 k 값 가져오기 (기본값 5)
        # as_retriever 호출 시 저장된 설정이 있다면 그것을 사용해야 하지만,
        # 여기서는 간단하게 구현
        return self.similarity_search(query)

    def _get_table_schema_points(self) -> List[Dict[str, Any]]:
        """
        기본 테이블 스키마 정보를 가져와서 포인트 리스트로 변환합니다.
        """
        from utils.llm.tools.datahub import get_table_schema
        from utils.llm.core import get_embeddings

        raw_data = get_table_schema()
        embeddings = get_embeddings()

        points = []
        for idx, item in enumerate(raw_data):
            for table_name, table_info in item.items():
                # 벡터 생성을 위한 텍스트 구성
                column_info_str = "\n".join(
                    [f"{k}: {v}" for k, v in table_info["columns"].items()]
                )
                text_to_embed = f"{table_name}: {table_info['table_description']}"

                vector = embeddings.embed_query(text_to_embed)

                # payload 구성
                payload = {
                    "table_name": table_name,
                    "table_description": table_info["table_description"],
                    "columns": table_info["columns"],
                }

                points.append(
                    {
                        "id": idx,
                        "vector": {"dense": vector},  # dense vector only for now
                        "payload": payload,
                    }
                )
        return points

    def initialize_collection_if_empty(
        self,
        collection_name: str = "lang2sql_table_schema",
        force_update: bool = False,
        data_loader: Optional[Callable[[], List[Dict[str, Any]]]] = None,
    ):
        """
        컬렉션이 비어있거나 없으면 데이터를 채웁니다.

        Args:
            collection_name: 초기화할 컬렉션 이름.
            force_update: 데이터가 있어도 강제로 업데이트할지 여부.
            data_loader: 데이터를 가져오는 함수. 포인트 리스트(id, vector, payload)를 반환해야 합니다.
                         None인 경우 기본 테이블 스키마 로더를 사용합니다.
        """
        # 컬렉션 존재 여부 확인 및 생성
        if not self.client.collection_exists(collection_name):
            self.create_collection(collection_name)

        # 데이터 존재 여부 확인
        if not force_update:
            count_result = self.client.count(collection_name=collection_name)
            if count_result.count > 0:
                print(
                    f"Collection '{collection_name}' is not empty. Skipping initialization."
                )
                return

        print(f"Initializing collection '{collection_name}'...")

        # 데이터 로드
        if data_loader is None:
            # 기본 동작: 테이블 스키마 정보 사용
            points = self._get_table_schema_points()
        else:
            points = data_loader()

        if points:
            self.upsert(collection_name, points)
        else:
            print("No data found to initialize.")
