"""
VectorDB 모듈 - FAISS와 pgvector를 지원하는 벡터 데이터베이스 추상화
"""

from .factory import get_vector_db

__all__ = ["get_vector_db"]
