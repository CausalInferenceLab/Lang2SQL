"""
pgvector VectorDB 구현
"""

import os
from typing import Optional
import psycopg2
from sqlalchemy.orm import Session
from langchain_postgres.vectorstores import PGVector

from llm_utils.tools import get_info_from_db
from llm_utils.llm import get_embeddings


def _check_collection_exists(connection_string: str, collection_name: str) -> bool:
    """PostgreSQL에서 collection이 존재하는지 확인합니다."""
    try:
        # 연결 문자열에서 연결 정보 추출
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()

        # langchain_pg_embedding 테이블에서 collection_name이 존재하는지 확인
        cursor.execute(
            "SELECT COUNT(*) FROM langchain_pg_embedding WHERE collection_name = %s",
            (collection_name,),
        )
        result = cursor.fetchone()
        count = result[0] if result else 0

        cursor.close()
        conn.close()

        return count > 0
    except Exception as e:
        print(f"Collection 존재 여부 확인 중 오류: {e}")
        return False


def get_pgvector_db(
    connection_string: Optional[str] = None, collection_name: Optional[str] = None
):
    """pgvector 벡터 데이터베이스를 로드하거나 생성합니다."""
    embeddings = get_embeddings()

    if connection_string is None:
        # 환경 변수에서 연결 정보 읽기 (기존 방식)
        host = os.getenv("PGVECTOR_HOST", "localhost")
        port = os.getenv("PGVECTOR_PORT", "5432")
        user = os.getenv("PGVECTOR_USER", "postgres")
        password = os.getenv("PGVECTOR_PASSWORD", "postgres")
        database = os.getenv("PGVECTOR_DATABASE", "postgres")
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

    if collection_name is None:
        collection_name = os.getenv("PGVECTOR_COLLECTION", "lang2sql_table_info_db")
    try:
        vector_store = PGVector(
            embeddings=embeddings,
            collection_name=collection_name,
            connection=connection_string,
        )

        results = vector_store.similarity_search("test", k=1)
        if not results:
            raise RuntimeError(f"Collection '{collection_name}' is empty")

        # 컬렉션이 존재하면 실제 검색도 진행해 볼 수 있습니다.
        vector_store.similarity_search("test", k=1)
        return vector_store

    except Exception as e:
        print(f"exception: {e}")
        # 컬렉션이 없거나 불러오기에 실패한 경우, 문서를 다시 인덱싱
        documents = get_info_from_db()
        vector_store = PGVector.from_documents(
            documents=documents,
            embedding=embeddings,
            connection=connection_string,
            collection_name=collection_name,
        )
        return vector_store
