from qdrant_client import QdrantClient, models
from typing import List, Dict, Any, Optional, Union
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
