import os
from typing import List, Dict, Optional, TypeVar, Callable, Iterable, Any

from langchain.schema import Document

from data_utils.datahub_source import DatahubMetadataFetcher
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

T = TypeVar("T")
R = TypeVar("R")


def parallel_process(
    items: Iterable[T],
    process_fn: Callable[[T], R],
    max_workers: int = 8,
    desc: Optional[str] = None,
    show_progress: bool = True,
) -> List[R]:
    """병렬 처리를 위한 유틸리티 함수

    Args:
        items (Iterable[T]): 처리할 아이템들
        process_fn (Callable[[T], R]): 각 아이템을 처리할 함수
        max_workers (int, optional): 최대 쓰레드 수. Defaults to 8.
        desc (Optional[str], optional): 진행 상태 메시지. Defaults to None.
        show_progress (bool, optional): 진행 상태 표시 여부. Defaults to True.

    Returns:
        List[R]: 처리 결과 리스트
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_fn, item) for item in items]
        if show_progress:
            futures = tqdm(futures, desc=desc)
        return [future.result() for future in futures]


def set_gms_server(gms_server: str):
    try:
        os.environ["DATAHUB_SERVER"] = gms_server
        fetcher = DatahubMetadataFetcher(gms_server=gms_server)
    except ValueError as e:
        raise ValueError(f"GMS 서버 설정 실패: {str(e)}")


def _get_fetcher():
    gms_server = os.getenv("DATAHUB_SERVER")
    if not gms_server:
        raise ValueError("GMS 서버가 설정되지 않았습니다.")
    return DatahubMetadataFetcher(gms_server=gms_server)


def _process_urn(urn: str, fetcher: DatahubMetadataFetcher) -> tuple[str, str]:
    table_name = fetcher.get_table_name(urn)
    table_description = fetcher.get_table_description(urn)
    return (table_name, table_description)


def _process_column_info(
    urn: str, table_name: str, fetcher: DatahubMetadataFetcher
) -> Optional[List[Dict[str, str]]]:
    if fetcher.get_table_name(urn) == table_name:
        return fetcher.get_column_names_and_descriptions(urn)
    return None


def _get_table_info(max_workers: int = 8) -> Dict[str, str]:
    """전체 테이블 이름과 설명을 가져오는 함수

    Args:
        max_workers (int, optional): 병렬 처리에 사용할 최대 쓰레드 수. Defaults to 8.

    Returns:
        Dict[str, str]: 테이블 이름과 설명을 담은 딕셔너리
    """
    fetcher = _get_fetcher()
    urns = fetcher.get_urns()
    table_info = {}

    results = parallel_process(
        urns,
        lambda urn: _process_urn(urn, fetcher),
        max_workers=max_workers,
        desc="테이블 정보 수집 중",
    )

    for table_name, table_description in results:
        if table_name and table_description:
            table_info[table_name] = table_description

    return table_info


def _get_column_info(
    table_name: str, urn_table_mapping: Dict[str, str], max_workers: int = 8
) -> List[Dict[str, str]]:
    """table_name에 해당하는 컬럼 이름과 설명을 가져오는 함수

    Args:
        table_name (str): 테이블 이름
        urn_table_mapping (Dict[str, str]): URN-테이블명 매핑 딕셔너리
        max_workers (int, optional): 병렬 처리에 사용할 최대 쓰레드 수. Defaults to 8.

    Returns:
        List[Dict[str, str]]: 컬럼 정보 리스트
    """
    # 해당 테이블의 URN 직접 찾기
    target_urn = urn_table_mapping.get(table_name)
    if not target_urn:
        return []

    # Fetcher 생성 및 컬럼 정보 가져오기
    fetcher = _get_fetcher()
    column_info = fetcher.get_column_names_and_descriptions(target_urn)

    return column_info


def get_info_from_db(max_workers: int = 8) -> List[Document]:
    """전체 테이블 이름과 설명, 컬럼 이름과 설명을 가져오는 함수

    Args:
        max_workers (int, optional): 병렬 처리에 사용할 최대 쓰레드 수. Defaults to 8.

    Returns:
        List[Document]: 테이블과 컬럼 정보를 담은 Document 객체 리스트
    """
    table_info = _get_table_info(max_workers=max_workers)

    # URN-테이블명 매핑을 한 번만 생성
    fetcher = _get_fetcher()
    urns = list(fetcher.get_urns())
    urn_table_mapping = {}
    for urn in urns:
        table_name = fetcher.get_table_name(urn)
        if table_name:
            urn_table_mapping[table_name] = urn

    def process_table_info(item: tuple[str, str]) -> str:
        table_name, table_description = item
        urn = urn_table_mapping.get(table_name, "")

        # fetcher 인스턴스 생성
        local_fetcher = _get_fetcher()

        # 컬럼 정보 가져오기
        column_info = _get_column_info(
            table_name, urn_table_mapping, max_workers=max_workers
        )
        column_info_str = "\n".join(
            [
                f"{col['column_name']}: {col['column_description']}"
                for col in column_info
            ]
        )

        # 쿼리 및 용어집 정보 가져오기
        queries_result = local_fetcher.get_queries_by_urn(urn) if urn else {}
        glossary_terms_result = (
            local_fetcher.get_glossary_terms_by_urn(urn) if urn else {}
        )

        # GraphQL 응답에서 실제 쿼리 리스트 추출
        queries = []
        if (
            queries_result
            and "data" in queries_result
            and "listQueries" in queries_result["data"]
            and "queries" in queries_result["data"]["listQueries"]
        ):
            queries = queries_result["data"]["listQueries"]["queries"]

        # GraphQL 응답에서 실제 glossary terms 추출
        glossary_terms = []
        if (
            glossary_terms_result
            and "data" in glossary_terms_result
            and "dataset" in glossary_terms_result["data"]
            and "glossaryTerms" in glossary_terms_result["data"]["dataset"]
            and glossary_terms_result["data"]["dataset"]["glossaryTerms"] is not None
            and "terms" in glossary_terms_result["data"]["dataset"]["glossaryTerms"]
        ):
            terms_data = glossary_terms_result["data"]["dataset"]["glossaryTerms"][
                "terms"
            ]
            for term_item in terms_data:
                if "term" in term_item and "properties" in term_item["term"]:
                    props = term_item["term"]["properties"]
                    name = props.get("name", "")
                    description = props.get("description", "")
                    definition = props.get("definition", "")
                    glossary_terms.append(
                        {
                            "name": name,
                            "description": description,
                            "definition": definition,
                        }
                    )

        # 쿼리 정보를 name, description, statement.value만 추출하여 포맷
        if queries:
            formatted_queries = []
            for q in queries[:3]:  # 최대 3개 쿼리만
                if isinstance(q, dict) and "properties" in q:
                    props = q["properties"]
                    name = props.get("name", "No name")
                    description = props.get("description", "No description")
                    statement_value = props.get("statement", {}).get(
                        "value", "No query statement"
                    )
                    formatted_query = f"Name: {name}\nDescription: {description}\nQuery: {statement_value}"
                    formatted_queries.append(formatted_query)
            queries_str = (
                "\n---\n".join(formatted_queries) if formatted_queries else "No queries"
            )
        else:
            queries_str = "No queries"
        terms_str = (
            "\n".join(
                [
                    f"Term: {term['name']}\nDescription: {term['description']}\nDefinition: {term['definition']}"
                    for term in glossary_terms
                ]
            )
            if glossary_terms
            else "No terms"
        )

        return f"{table_name}: {table_description}\nColumns:\n {column_info_str}\nQueries:\n {queries_str}\nTerms:\n {terms_str}"

    table_info_str_list = parallel_process(
        table_info.items(),
        process_table_info,
        max_workers=max_workers,
        desc="컬럼 정보 수집 중",
    )

    return [Document(page_content=info) for info in table_info_str_list]


def get_metadata_from_db() -> List[Dict]:
    """
    전체 테이블의 메타데이터(테이블 이름, 설명, 컬럼 이름, 설명, 테이블 lineage, 컬럼 별 lineage)를 가져오는 함수
    """

    fetcher = _get_fetcher()
    urns = list(fetcher.get_urns())

    metadata = []
    total = len(urns)
    for idx, urn in enumerate(urns, 1):
        print(f"[{idx}/{total}] Processing URN: {urn}")
        table_metadata = fetcher.build_table_metadata(urn)
        metadata.append(table_metadata)

    return metadata
