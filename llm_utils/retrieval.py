import os
from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
from langchain.retrievers import ContextualCompressionRetriever
from langchain.retrievers.document_compressors import CrossEncoderReranker
from langchain_community.cross_encoders import HuggingFaceCrossEncoder
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from .tools import get_info_from_db
from .llm_factory import get_embeddings


def get_vector_db():
    """벡터 데이터베이스를 로드하거나 생성합니다."""
    embeddings = get_embeddings()
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


def load_reranker_model(device: str = "cpu"):
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

    return HuggingFaceCrossEncoder(
        model_name=local_model_path,
        model_kwargs={"device": device},
    )


def get_retriever(retriever_name: str = "기본", top_n: int = 5, device: str = "cpu"):
    """검색기 타입에 따라 적절한 검색기를 생성합니다.

    Args:
        retriever_name: 사용할 검색기 이름 ("기본", "재순위", 등)
        top_n: 반환할 상위 결과 개수
    """
    print(device)
    retrievers = {
        "기본": lambda: get_vector_db().as_retriever(search_kwargs={"k": top_n}),
        "Reranker": lambda: ContextualCompressionRetriever(
            base_compressor=CrossEncoderReranker(
                model=load_reranker_model(device), top_n=top_n
            ),
            base_retriever=get_vector_db().as_retriever(search_kwargs={"k": top_n}),
        ),
    }

    if retriever_name not in retrievers:
        print(
            f"경고: '{retriever_name}' 검색기를 찾을 수 없습니다. 기본 검색기를 사용합니다."
        )
        retriever_name = "기본"

    return retrievers[retriever_name]()


def search_tables(
    query: str, retriever_name: str = "기본", top_n: int = 5, device: str = "cpu"
):
    """쿼리에 맞는 테이블 정보를 검색합니다."""
    if retriever_name == "기본":
        db = get_vector_db()
        doc_res = db.similarity_search(query, k=top_n)
    else:
        retriever = get_retriever(
            retriever_name=retriever_name, top_n=top_n, device=device
        )
        doc_res = retriever.invoke(query)

    # 결과를 사전 형태로 변환
    documents_dict = {}
    for doc in doc_res:
        lines = doc.page_content.split("\n")

        # 테이블명 및 설명 추출
        table_name, table_desc = lines[0].split(": ", 1)

        # 섹션별로 정보 추출
        columns = {}
        queries = []
        terms = []

        current_section = None
        current_query = {}
        current_term = {}

        for i, line in enumerate(lines[1:], 1):
            line = line.strip()

            # 섹션 헤더 확인
            if line == "Columns:":
                current_section = "columns"
                continue
            elif line == "Queries:":
                current_section = "queries"
                continue
            elif line == "Terms:":
                current_section = "terms"
                continue

            # 각 섹션의 내용 파싱
            if current_section == "columns" and ": " in line:
                col_name, col_desc = line.split(": ", 1)
                columns[col_name.strip()] = col_desc.strip()

            elif current_section == "queries" and line and line != "No queries":
                # 쿼리 구분자 확인
                if line == "---":
                    # 이전 쿼리 저장
                    if current_query:
                        queries.append(current_query)
                        current_query = {}
                elif line.startswith("Name: "):
                    # 이전 쿼리가 있다면 저장
                    if current_query:
                        queries.append(current_query)
                    current_query = {"name": line[6:]}  # "Name: " 제거
                elif line.startswith("Description: "):
                    if current_query:
                        current_query["description"] = line[13:]  # "Description: " 제거
                elif line.startswith("Query: "):
                    if current_query:
                        current_query["statement"] = line[7:]  # "Query: " 제거

            elif current_section == "terms" and line and line != "No terms":
                if line.startswith("Term: "):
                    # 이전 용어가 있다면 저장
                    if current_term:
                        terms.append(current_term)
                    # 새로운 용어 시작
                    current_term = {"name": line[6:]}  # "Term: " 제거
                elif line.startswith("Description: ") and current_term:
                    current_term["description"] = line[13:]  # "Description: " 제거
                elif line.startswith("Definition: ") and current_term:
                    current_term["definition"] = line[12:]  # "Definition: " 제거

        # 마지막 쿼리 저장
        if current_query and current_section == "queries":
            queries.append(current_query)

        # 마지막 용어 저장
        if current_term and current_section == "terms":
            terms.append(current_term)

        # 딕셔너리 저장
        documents_dict[table_name] = {
            "table_description": table_desc.strip(),
            **columns,  # 컬럼 정보 추가
            "queries": queries,  # 쿼리 정보 추가 (딕셔너리 형태로)
            "glossary_terms": terms,  # 용어집 정보 추가
        }

    return documents_dict
