"""
VectorDB 팩토리 모듈 - 환경 변수에 따라 적절한 VectorDB 인스턴스를 생성
"""

import os
from typing import Optional

from utils.llm.vectordb.faiss_db import get_faiss_vector_db
from utils.llm.vectordb.pgvector_db import get_pgvector_db
from utils.llm.vectordb.qdrant_db import QdrantDB


def get_qdrant_vector_db(url: Optional[str] = None, api_key: Optional[str] = None):
    """Qdrant VectorDB 인스턴스를 반환하고 초기화합니다."""
    if url is None:
        url = os.getenv("QDRANT_URL", "http://localhost:6333")

    if api_key is None:
        api_key = os.getenv("QDRANT_API_KEY")

    db = QdrantDB(url=url, api_key=api_key)
    db.initialize_collection_if_empty()
    return db


def get_vector_db(
    vectordb_type: Optional[str] = None, vectordb_location: Optional[str] = None
):
    """
    VectorDB 타입과 위치에 따라 적절한 VectorDB 인스턴스를 반환합니다.

    Args:
        vectordb_type: VectorDB 타입 ("faiss", "pgvector", "qdrant"). None인 경우 환경 변수에서 읽음.
        vectordb_location: VectorDB 위치 (FAISS: 디렉토리 경로, pgvector: 연결 문자열). None인 경우 환경 변수에서 읽음.

    Returns:
        VectorDB 인스턴스 (FAISS, PGVector, 또는 Qdrant)
    """
    if vectordb_type is None:
        vectordb_type = os.getenv("VECTORDB_TYPE", "faiss").lower()

    if vectordb_location is None:
        vectordb_location = os.getenv("VECTORDB_LOCATION")

    if vectordb_type == "faiss":
        return get_faiss_vector_db(vectordb_location)
    elif vectordb_type == "pgvector":
        return get_pgvector_db(vectordb_location)
    elif vectordb_type == "qdrant":
        return get_qdrant_vector_db(url=vectordb_location)
    else:
        raise ValueError(
            f"지원하지 않는 VectorDB 타입: {vectordb_type}. 'faiss', 'pgvector', 또는 'qdrant'를 사용하세요."
        )
