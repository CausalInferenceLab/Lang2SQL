"""
dev/create_pgvector.py

CSV 파일에서 테이블과 컬럼 정보를 불러와 OpenAI 임베딩으로 벡터화한 뒤,
pgvector에 적재한다.

환경 변수:
    OPEN_AI_KEY: OpenAI API 키
    OPEN_AI_EMBEDDING_MODEL: 사용할 임베딩 모델 이름
    VECTORDB_LOCATION: pgvector 연결 문자열
    PGVECTOR_COLLECTION: pgvector 컬렉션 이름
"""

import csv
import os
from collections import defaultdict

from dotenv import load_dotenv
from langchain.schema import Document
from langchain_openai import OpenAIEmbeddings
from langchain_postgres.vectorstores import PGVector

load_dotenv()
# CSV 파일 경로
CSV_PATH = "./dev/table_catalog.csv"
# .env의 VECTORDB_LOCATION과 동일하게 맞추세요
CONN = (
    os.getenv("VECTORDB_LOCATION")
    or "postgresql://pgvector:pgvector@localhost:5432/postgres"
)
COLLECTION = os.getenv("PGVECTOR_COLLECTION", "table_info_db")

tables = defaultdict(lambda: {"desc": "", "columns": []})
with open(CSV_PATH, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        t = row["table_name"].strip()
        tables[t]["desc"] = row["table_description"].strip()
        col = row["column_name"].strip()
        col_desc = row["column_description"]
        tables[t]["columns"].append((col, col_desc))

docs = []
for t, info in tables.items():
    cols = "\n".join([f"{c}: {d}" for c, d in info["columns"]])
    docs.append(Document(page_content=f"{t}: {info['desc']}\nColumns:\n {cols}"))

emb = OpenAIEmbeddings(
    model=os.getenv("OPEN_AI_EMBEDDING_MODEL"), openai_api_key=os.getenv("OPEN_AI_KEY")
)
PGVector.from_documents(
    documents=docs, embedding=emb, connection=CONN, collection_name=COLLECTION
)
print(f"pgvector collection populated: {COLLECTION}")
