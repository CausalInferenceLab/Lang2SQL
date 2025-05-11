from dotenv import load_dotenv

load_dotenv()

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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
    # gms_server = os.getenv("DATAHUB_SERVER", "http://34.125.222.80:9002/")
    # gms_server = os.getenv("DATAHUB_SERVER", "http://34.125.222.80:8080")
    # gms_server = "http://34.16.140.205:8080"
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


def get_column_info(table_name: str) -> Dict:
    """table_name에 해당하는 테이블 이름과 컬럼 정보를 가져오는 함수"""
    fetcher = _get_fetcher()
    urns = fetcher.get_urns()

    for urn in urns:
        if fetcher.get_table_name(urn) == table_name:
            columns = fetcher.get_column_names_and_descriptions(urn)
            # 테이블 설명도 함께 가져오기 (필요시)
            table_description = fetcher.get_table_description(urn)

            # 테이블 이름과 컬럼 정보를 함께 반환
            return {
                "table_name": table_name,
                "table_description": table_description,
                "columns": columns,
            }

    # 해당 테이블이 없는 경우
    raise ValueError(f"테이블 '{table_name}'을(를) 찾을 수 없습니다.")


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


if __name__ == "__main__":
    # print(get_info_from_db("client_stream_activated_on_product"))
    print(_get_column_info("client_stream_activated_on_product"))
