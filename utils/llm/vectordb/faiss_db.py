"""
FAISS VectorDB 구현
"""

import os
from typing import Optional

from langchain_community.vectorstores import FAISS
from langchain.schema import Document

from utils.llm.core import get_embeddings
from utils.llm.tools import get_table_schema


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
        raw_data = get_table_schema()
        documents = []
        for item in raw_data:
            for table_name, table_info in item.items():
                column_info_str = "\n".join(
                    [f"{k}: {v}" for k, v in table_info["columns"].items()]
                )
                page_content = f"{table_name}: {table_info['table_description']}\nColumns:\n {column_info_str}"
                documents.append(Document(page_content=page_content))
        db = FAISS.from_documents(documents, embeddings)
        db.save_local(vectordb_path)
        print(f"VectorDB를 새로 생성했습니다: {vectordb_path}")
    return db
