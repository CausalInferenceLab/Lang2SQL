import os
from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
from langchain.retrievers import ContextualCompressionRetriever
from langchain.retrievers.document_compressors import CrossEncoderReranker
from langchain_community.cross_encoders import HuggingFaceCrossEncoder
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from .tools import get_info_from_db


def get_vector_db():
    """벡터 데이터베이스를 로드하거나 생성합니다."""
    embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
    try:
        db = FAISS.load_local(
            os.getcwd() + "/table_info_db",
            embeddings,
            allow_dangerous_deserialization=True,
        )
    except:
        documents = get_info_from_db()
        db = FAISS.from_documents(documents, embeddings)
        db.save_local(os.getcwd() + "/table_info_db")
        print("table_info_db not found")
    return db


def load_reranker_model():
    """한국어 reranker 모델을 로드하거나 다운로드합니다."""
    local_model_path = os.path.join(os.getcwd(), "ko_reranker_local")

    # 로컬에 저장된 모델이 있으면 불러오고, 없으면 다운로드 후 저장
    if os.path.exists(local_model_path) and os.path.isdir(local_model_path):
        print("🔄 ko-reranker 모델 로컬에서 로드 중...")
    else:
        print("⬇️ ko-reranker 모델 다운로드 및 저장 중...")
        model = AutoModelForSequenceClassification.from_pretrained(
            "Dongjin-kr/ko-reranker"
        )
        tokenizer = AutoTokenizer.from_pretrained("Dongjin-kr/ko-reranker")
        model.save_pretrained(local_model_path)
        tokenizer.save_pretrained(local_model_path)

    return HuggingFaceCrossEncoder(model_name=local_model_path)


def get_retriever(use_rerank=False):
    """검색기를 생성합니다. use_rerank가 True이면 reranking을 적용합니다."""
    db = get_vector_db()
    retriever = db.as_retriever(search_kwargs={"k": 10})

    if use_rerank:
        model = load_reranker_model()
        compressor = CrossEncoderReranker(model=model, top_n=3)
        return ContextualCompressionRetriever(
            base_compressor=compressor, base_retriever=retriever
        )
    else:
        return retriever


def search_tables(query, use_rerank=False):
    """쿼리에 맞는 테이블 정보를 검색합니다."""
    if use_rerank:
        retriever = get_retriever(use_rerank=True)
        doc_res = retriever.invoke(query)
    else:
        db = get_vector_db()
        doc_res = db.similarity_search(query, k=10)

    # 결과를 사전 형태로 변환
    documents_dict = {}
    for doc in doc_res:
        lines = doc.page_content.split("\n")

        # 테이블명 및 설명 추출
        table_name, table_desc = lines[0].split(": ", 1)

        # 컬럼 정보 추출
        columns = {}
        if len(lines) > 2 and lines[1].strip() == "Columns:":
            for line in lines[2:]:
                if ": " in line:
                    col_name, col_desc = line.split(": ", 1)
                    columns[col_name.strip()] = col_desc.strip()

        # 딕셔너리 저장
        documents_dict[table_name] = {
            "table_description": table_desc.strip(),
            **columns,  # 컬럼 정보 추가
        }

    return documents_dict
