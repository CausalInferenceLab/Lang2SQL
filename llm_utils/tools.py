import os
from typing import List, Dict

from langchain.schema import Document

from data_utils.datahub_source import DatahubMetadataFetcher


def set_gms_server(gms_server: str):
    try:
        os.environ["DATAHUB_SERVER"] = gms_server
        fetcher = DatahubMetadataFetcher(gms_server=gms_server)
    except ValueError as e:
        # 유효하지 않은 GMS 서버 주소일 경우 예외를 발생시킴
        raise ValueError(f"GMS 서버 설정 실패: {str(e)}")


def _get_fetcher():
    gms_server = os.getenv("DATAHUB_SERVER")
    if not gms_server:
        raise ValueError("GMS 서버가 설정되지 않았습니다.")
    return DatahubMetadataFetcher(gms_server=gms_server)


def _get_table_info() -> Dict[str, str]:
    """전체 테이블 이름과 설명을 가져오는 함수"""
    fetcher = _get_fetcher()
    urns = fetcher.get_urns()
    table_info = {}
    for urn in urns:
        table_name = fetcher.get_table_name(urn)
        table_description = fetcher.get_table_description(urn)
        if table_name and table_description:
            table_info[table_name] = table_description
    return table_info


def _get_column_info(table_name: str) -> List[Dict[str, str]]:
    """table_name에 해당하는 컬럼 이름과 설명을 가져오는 함수"""
    fetcher = _get_fetcher()
    urns = fetcher.get_urns()
    for urn in urns:
        if fetcher.get_table_name(urn) == table_name:
            return fetcher.get_column_names_and_descriptions(urn)
    return []


def get_info_from_db() -> List[Document]:
    """
    전체 테이블 이름과 설명, 컬럼 이름과 설명을 가져오는 함수
    """

    table_info_str_list = []
    table_info = _get_table_info()
    for table_name, table_description in table_info.items():
        column_info = _get_column_info(table_name)
        column_info_str = "\n".join(
            [
                f"{col['column_name']}: {col['column_description']}"
                for col in column_info
            ]
        )
        table_info_str_list.append(
            f"{table_name}: {table_description}\nColumns:\n {column_info_str}"
        )

    # table_info_str_list를 Document 객체 리스트로 변환
    return [Document(page_content=info) for info in table_info_str_list]


def get_metadata_from_db() -> List[Dict]:
    """
    전체 테이블의 메타데이터(테이블 이름, 설명, 컬럼 이름, 설명, 테이블 lineage, 컬럼 별 lineage)를 가져오는 함수
    """

    fetcher = _get_fetcher()
    urns = fetcher.get_urns()

    metadata = []
    for urn in urns:
        print(f"Processing URN: {urn}")
        table_metadata = fetcher.build_table_metadata(urn)
        metadata.append(table_metadata)

    return metadata
