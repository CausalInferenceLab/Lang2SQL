import os

from langchain.retrievers import ContextualCompressionRetriever
from langchain.retrievers.document_compressors import CrossEncoderReranker
from langchain_community.cross_encoders import HuggingFaceCrossEncoder
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from utils.llm.vectordb import get_vector_db
from utils.llm.tools.datahub import get_glossary_vector_data, get_query_vector_data
from utils.llm.core import get_embeddings


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

        # 섹션별로 정보 추출 (테이블/컬럼만 사용)
        columns = {}
        current_section = None

        for i, line in enumerate(lines[1:], 1):
            line = line.strip()

            # 섹션 헤더 확인
            if line == "Columns:":
                current_section = "columns"
                continue

            # 각 섹션의 내용 파싱
            if current_section == "columns" and ": " in line:
                col_name, col_desc = line.split(": ", 1)
                columns[col_name.strip()] = col_desc.strip()

        # 딕셔너리 저장
        documents_dict[table_name] = {
            "table_description": table_desc.strip(),
            **columns,  # 컬럼 정보 추가
        }

    return documents_dict


def _prepare_vector_data(data_fetcher, text_fields):
    """
    데이터를 가져와서 임베딩을 생성하는 헬퍼 함수
    """
    points = data_fetcher()
    embeddings = get_embeddings()

    for point in points:
        payload = point["payload"]
        # 텍스트 필드들을 결합하여 임베딩 생성
        text_to_embed = " ".join([str(payload.get(field, "")) for field in text_fields])
        vector = embeddings.embed_query(text_to_embed)
        point["vector"] = {"dense": vector}

    return points


def search_glossary(query: str, force_update: bool = False, top_n: int = 5) -> list:
    """
    용어집 검색 함수
    """
    collection_name = "lang2sql_glossary"
    db = get_vector_db()

    # 데이터 로더 정의 (임베딩 생성 포함)
    def data_loader():
        return _prepare_vector_data(get_glossary_vector_data, ["name", "description"])

    # 컬렉션 초기화 (필요시)
    db.initialize_collection_if_empty(
        collection_name=collection_name,
        force_update=force_update,
        data_loader=data_loader,
    )

    # 검색 수행
    embeddings = get_embeddings()
    query_vector = embeddings.embed_query(query)

    results = db.search(
        collection_name=collection_name,
        query_vector=("dense", query_vector),
        limit=top_n,
    )

    # 결과 포맷팅
    return [res.payload for res in results]


def search_query_examples(
    query: str, force_update: bool = False, top_n: int = 5
) -> list:
    """
    쿼리 예제 검색 함수
    """
    collection_name = "lang2sql_query_example"
    db = get_vector_db()

    # 데이터 로더 정의 (임베딩 생성 포함)
    def data_loader():
        return _prepare_vector_data(get_query_vector_data, ["name", "description"])

    # 컬렉션 초기화 (필요시)
    db.initialize_collection_if_empty(
        collection_name=collection_name,
        force_update=force_update,
        data_loader=data_loader,
    )

    # 검색 수행
    embeddings = get_embeddings()
    query_vector = embeddings.embed_query(query)

    results = db.search(
        collection_name=collection_name,
        query_vector=("dense", query_vector),
        limit=top_n,
    )

    # 결과 포맷팅
    return [res.payload for res in results]
