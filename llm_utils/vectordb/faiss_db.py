"""
FAISS VectorDB 구현
"""

import os
from typing import Optional

from langchain_community.vectorstores import FAISS

from llm_utils.llm import get_embeddings
from llm_utils.tools import get_info_from_db


def get_faiss_vector_db(vectordb_path: Optional[str] = None):
    """FAISS 벡터 데이터베이스를 로드하거나 생성합니다."""
    embeddings = get_embeddings()

    # 기본 경로 설정
    if vectordb_path is None:
        vectordb_path = os.path.join(os.getcwd(), "dev/table_info_db")

    try:
        db = FAISS.load_local(
            vectordb_path,
            embeddings,
            allow_dangerous_deserialization=True,
        )
    except:
        documents = get_info_from_db()
        db = FAISS.from_documents(documents, embeddings)
        db.save_local(vectordb_path)
        print(f"VectorDB를 새로 생성했습니다: {vectordb_path}")
    return db
