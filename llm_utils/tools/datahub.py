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
    """병렬 처리를 위한 유틸리티 함수"""
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
    target_urn = urn_table_mapping.get(table_name)
    if not target_urn:
        return []

    fetcher = _get_fetcher()
    column_info = fetcher.get_column_names_and_descriptions(target_urn)

    return column_info


def get_info_from_db(max_workers: int = 8) -> List[Document]:
    table_info = _get_table_info(max_workers=max_workers)

    fetcher = _get_fetcher()
    urns = list(fetcher.get_urns())
    urn_table_mapping = {}
    for urn in urns:
        table_name = fetcher.get_table_name(urn)
        if table_name:
            urn_table_mapping[table_name] = urn

    def process_table_info(item: tuple[str, str]) -> str:
        table_name, table_description = item
        column_info = _get_column_info(
            table_name, urn_table_mapping, max_workers=max_workers
        )
        column_info_str = "\n".join(
            [
                f"{col['column_name']}: {col['column_description']}"
                for col in column_info
            ]
        )
        return f"{table_name}: {table_description}\nColumns:\n {column_info_str}"

    table_info_str_list = parallel_process(
        table_info.items(),
        process_table_info,
        max_workers=max_workers,
        desc="컬럼 정보 수집 중",
    )

    return [Document(page_content=info) for info in table_info_str_list]


def get_metadata_from_db() -> List[Dict]:
    fetcher = _get_fetcher()
    urns = list(fetcher.get_urns())

    metadata = []
    total = len(urns)
    for idx, urn in enumerate(urns, 1):
        print(f"[{idx}/{total}] Processing URN: {urn}")
        table_metadata = fetcher.build_table_metadata(urn)
        metadata.append(table_metadata)

    return metadata
