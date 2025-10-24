"""
dev/create_qdrant.py

CSV 파일에서 테이블과 컬럼 정보를 불러와 OpenAI 임베딩으로 벡터화한 뒤,
Qdrant 인덱스를 생성하고 로컬 디렉토리에 저장한다.

환경 변수:
    OPEN_AI_KEY: OpenAI API 키
    OPEN_AI_EMBEDDING_MODEL: 사용할 임베딩 모델 이름

출력:
    지정된 OUTPUT_DIR 경로에 Qdrant 인덱스 저장
"""

import csv
import os
from collections import defaultdict

from dotenv import load_dotenv
from langchain_community.vectorstores import FAISS
# from langchain_openai import OpenAIEmbeddings
from langchain_openai import AzureOpenAIEmbeddings
from langchain.schema import Document

from langchain_qdrant import QdrantVectorStore
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams

    
load_dotenv()
# CSV 파일 경로
# CSV_PATH = "./dev/krcdp_catalog.csv"
# .env의 VECTORDB_LOCATION과 동일하게 맞추세요
# OUTPUT_DIR = "./dev/table_info_qdrant"

CSV_PATH = r"D:\Code\lang2sql\Lang2SQL\dev\krcdp_catalog.csv"
OUTPUT_DIR = "./dev/table_info_qdrant"


client = QdrantClient(path=OUTPUT_DIR)

client.create_collection(
    collection_name="demo_collection",
    vectors_config=VectorParams(size=1536, distance=Distance.COSINE),
)


# emb = AzureOpenAIEmbeddings(
#     azure_deployment=os.getenv('AZURE_OPENAI_EMBEDDING_MODEL'),
#     azure_endpoint=os.getenv('AZURE_OPENAI_EMBEDDING_ENDPOINT'),
#     api_key=os.getenv('AZURE_OPENAI_EMBEDDING_KEY'),
#     openai_api_version=os.getenv('AZURE_OPENAI_EMBEDDING_API_VERSION')
# )


emb = AzureOpenAIEmbeddings(
    azure_deployment='text-embedding-3-small-1',
    azure_endpoint='https://dev.dxengws.apim.lgedx.biz/shared-embedding',
    api_key='c440d78ccd5a41f9852a5ad4df92478e',
    openai_api_version='2024-10-21'
)


vector_store = QdrantVectorStore(
    client=client,
    collection_name="demo_collection",
    embedding=emb,
)

tables = defaultdict(lambda: {"desc": "", "columns": []})
with open(CSV_PATH, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        # t = row["table_name"].strip()
        # tables[t]["desc"] = row["table_description"].strip()
        # col = row["column_name"].strip()
        # col_desc = row["column_description"].strip()
        # tables[t]["columns"].append((col, col_desc))

        t = row['테이블명'].strip()
        tables[t]["desc"] = row["목적"].strip()
        col = row["항목명"].strip()
        col_desc = row["설명"].strip()
        tables[t]["columns"].append((col, col_desc))

docs = []
for t, info in tables.items():
    cols = "\n".join([f"{c}: {d}" for c, d in info["columns"]])
    page = f"{t}: {info['desc']}\nColumns:\n {cols}"

    docs.append(Document(page_content=page, metadata={"source": "krcdp"}))


db = vector_store.add_documents(docs)
client.close()