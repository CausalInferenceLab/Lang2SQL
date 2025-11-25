import os
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, Iterable, List, Optional, TypeVar

from langchain.schema import Document
from tqdm import tqdm

from utils.data.datahub_source import DatahubMetadataFetcher

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
    table_name: str, urn_table_mapping: Dict[str, str]
) -> List[Dict[str, str]]:
    target_urn = urn_table_mapping.get(table_name)
    if not target_urn:
        return []

    fetcher = _get_fetcher()
    column_info = fetcher.get_column_names_and_descriptions(target_urn)

    return column_info


def _extract_dataset_name_from_urn(urn: str) -> Optional[str]:
    """URN 문자열에서 데이터셋 이름(예: delta.default.stg_gh_events)만 추출.

    지원 패턴:
    - dataset URN: urn:li:dataset:(urn:li:dataPlatform:dbt,delta.default.stg_gh_events,PROD)
    - schemaField URN: urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:dbt,delta.default.stg_gh_events,PROD),event_id)
    """
    match = re.search(
        r"urn:li:dataset:\(urn:li:dataPlatform:[^,]+,([^,]+),[^)]+\)", urn
    )
    if match:
        return match.group(1)
    return None


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


def _prepare_datahub_metadata_mappings(max_workers: int = 8):
    table_info = _get_table_info(max_workers=max_workers)

    fetcher = _get_fetcher()
    urns = list(fetcher.get_urns())
    urn_table_mapping = {}
    display_name_by_table = {}
    for urn in urns:
        original_name = fetcher.get_table_name(urn)
        if original_name:
            urn_table_mapping[original_name] = urn
            parsed_name = _extract_dataset_name_from_urn(urn)
            if parsed_name:
                display_name_by_table[original_name] = parsed_name

    return table_info, urn_table_mapping, display_name_by_table


def _format_datahub_table_info(
    item: tuple[str, str, str], urn_table_mapping: Dict[str, str]
) -> Dict:
    original_table_name, table_description, display_table_name = item
    # 컬럼 조회는 기존 테이블 이름으로 수행 (urn_table_mapping과 일치)
    column_info = _get_column_info(original_table_name, urn_table_mapping)

    columns = {col["column_name"]: col["column_description"] for col in column_info}

    used_name = display_table_name or original_table_name
    return {
        used_name: {
            "table_description": table_description,
            "columns": columns,
        }
    }


def get_table_schema(max_workers: int = 8) -> List[Dict]:
    table_info, urn_table_mapping, display_name_by_table = (
        _prepare_datahub_metadata_mappings(max_workers)
    )

    # 표시용 이름을 세 번째 파라미터로 함께 전달
    items_with_display = [
        (
            name,
            desc,
            display_name_by_table.get(name, name),
        )
        for name, desc in table_info.items()
    ]

    # parallel_process에 전달할 함수 래핑
    def process_fn(item):
        return _format_datahub_table_info(item, urn_table_mapping)

    table_info_list = parallel_process(
        items_with_display,
        process_fn,
        max_workers=max_workers,
        desc="컬럼 정보 수집 중",
    )

    return table_info_list
