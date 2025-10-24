import os

from dotenv import load_dotenv

from langchain_openai import AzureOpenAIEmbeddings
from langchain_qdrant import QdrantVectorStore
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams

load_dotenv()

CSV_PATH = r"D:\Code\lang2sql\Lang2SQL\dev\table_catalog.csv"
OUTPUT_DIR = r"D:\Code\lang2sql\Lang2SQL\dev\table_info_qdrant"


client = QdrantClient(path=OUTPUT_DIR)

# client.create_collection(
#     collection_name="demo_collection",
#     vectors_config=VectorParams(size=3072, distance=Distance.COSINE),
# )

# emb = AzureOpenAIEmbeddings(
#     azure_deployment=os.getenv("AZURE_OPENAI_EMBEDDING_MODEL"),
#     azure_endpoint=os.getenv("AZURE_OPENAI_EMBEDDING_ENDPOINT"),
#     api_key=os.getenv("AZURE_OPENAI_EMBEDDING_KEY"),
#     openai_api_version=os.getenv("AZURE_OPENAI_EMBEDDING_API_VERSION"),
# )


# vector_store = QdrantVectorStore(
#     client=client,
#     collection_name="demo_collection",
#     embedding=emb,
# )


# results = vector_store.similarity_search(
#     "계약 종료일이 2024년 1월이고 케어솔루션 서비스를 이용한 고객의 고객번호를 알려주세요.",
#     k=2,
# )
# for res in results:
#     print(f"* {res.page_content} [{res.metadata}]")

client.upsert(
    collection_name="demo_collection",
    points=[
        
    ]
)

client.close()
