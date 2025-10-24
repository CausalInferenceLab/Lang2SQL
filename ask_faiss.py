"""
FAISS VectorDB 구현
"""

import os
from typing import Optional
from dotenv import load_dotenv


from langchain_community.vectorstores import FAISS

from utils.llm.core import get_embeddings
from utils.llm.tools import get_info_from_db

from langchain_openai import (
    AzureChatOpenAI,
    AzureOpenAIEmbeddings,
    ChatOpenAI,
    OpenAIEmbeddings,
)

load_dotenv()

embeddings = AzureOpenAIEmbeddings(
    api_key=os.getenv("AZURE_OPENAI_EMBEDDING_KEY"),
    azure_endpoint=os.getenv("AZURE_OPENAI_EMBEDDING_ENDPOINT"),
    azure_deployment=os.getenv("AZURE_OPENAI_EMBEDDING_MODEL"),
    api_version=os.getenv("AZURE_OPENAI_EMBEDDING_API_VERSION"),
)
# 기본 경로 설정
vectordb_path = os.path.join(os.getcwd(), "dev/table_info_db")

db = FAISS.load_local(
    vectordb_path,
    embeddings,
    allow_dangerous_deserialization=True,
)

doc_res = db.similarity_search("고객 데이터를 기반으로 유니크한 유저 수를 카운트하는 쿼리", k=2)

for res in doc_res:
    print(res)
    print(f"* {res.page_content} [{res.metadata}]")