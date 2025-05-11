import os
import sys
from pprint import pprint

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from langchain_openai import OpenAIEmbeddings


# from llm_utils.tools import get_info_from_db, get_column_info
from .test_table_info import table_info

from .prepositions_conjunctions_list import prepositions_conjunctions_list


table_info = table_info
embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
persist_directory = os.path.join(os.getcwd(), "table_info_chroma_db")

def format_docs(docs):
    return "\n\n".join(doc.page_content for doc in docs)


def table_name_splitter():
    # table_info = get_info_from_db()

    table_names_list = []
    split_table_names = []
    

    for table in table_info:
        table_name = table.page_content.split(":")[0].strip()
        table_names_list.append(table_name)
        split_names = table_name.split("_")
        split_table_names.append(split_names)


    max_len = max(len(names) for names in split_table_names)

    result_lists = [[] for _ in range(max_len)]

    for names in split_table_names:
        for idx in range(max_len):
            if idx < len(names):
                if names[idx] not in prepositions_conjunctions_list:
                    result_lists[idx].append(names[idx])
                else:
                    result_lists[idx].append("")

    ordinal_names = [
        "first",
        "second",
        "third",
        "fourth",
        "fifth",
        "sixth",
        "seventh",
        "eighth",
        "ninth",
        "tenth",
        "eleventh",
        "twelfth",
        "thirteenth",
        "fourteenth",
        "fifteenth",
    ]

    named_lists = {}
    for idx, lst in enumerate(result_lists):
        if idx < len(ordinal_names):
            var_name = f"{ordinal_names[idx]}_list"
        else:
            var_name = f"{idx+1}_list"
        named_lists[var_name] = lst

    unique_named_lists = {}
    for key, value_list in named_lists.items():
        unique_list = list(dict.fromkeys(value_list))
        unique_named_lists[key] = unique_list

    sample_tables = []
    first_categories = unique_named_lists.get("first_list", [])

    # 카테고리별로 샘플 테이블 찾기
    category_count = min(len(first_categories), 5)  # 최대 5개 카테고리까지만

    for i in range(category_count):
        category = first_categories[i]
        # 해당 카테고리로 시작하는 테이블 찾기
        for table_name in table_names_list:
            parts = table_name.split("_")
            if parts and parts[0] == category:
                sample_tables.append(table_name)
                break  # 각 카테고리당 1개만 추가

    return table_names_list, unique_named_lists, sample_tables




import pickle
import os
import sys

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)


# 저장 파일 경로
PERSIST_DIRECTORY = os.path.join(os.getcwd(), "table_info_pickle")
RESULT_FILE = os.path.join(PERSIST_DIRECTORY, "table_info_results.pkl")


# 결과 확인 및 생성 함수
def get_results():
    """결과 파일이 있으면 로드하고, 없으면 생성하여 반환"""

    # 디렉토리가 없으면 생성
    if not os.path.exists(PERSIST_DIRECTORY):
        os.makedirs(PERSIST_DIRECTORY)
        print(f"저장 디렉토리 생성됨: {PERSIST_DIRECTORY}")

    # 파일이 있는지 확인
    if os.path.exists(RESULT_FILE):
        try:
            print(f"기존 결과 파일 로드 중: {RESULT_FILE}")
            with open(RESULT_FILE, "rb") as f:
                results = pickle.load(f)
            print("결과 파일 로드 완료!")
            return results
        except Exception as e:
            print(f"결과 파일 로드 실패: {e}")
            # 로드 실패 시 파일 삭제 (선택 사항)
            os.remove(RESULT_FILE)
            print("손상된 결과 파일 삭제됨")

    # 파일이 없거나 로드 실패 시 새로 계산
    print("새 결과 계산 시작...")

    db_info = format_docs(table_info)
    table_names_list, unique_named_lists, sample_tables = table_name_splitter()

    # 결과 저장 (벡터 스토어는 이미 디스크에 저장됨)
    results = {
        "db_info": db_info,
        "table_names_list": table_names_list,
        "unique_named_lists": unique_named_lists,
        "sample_tables": sample_tables,
    }

    # 피클 파일에 저장
    try:
        print(f"결과 파일 저장 중: {RESULT_FILE}")
        with open(RESULT_FILE, "wb") as f:
            pickle.dump(results, f)
        print("결과 파일 저장 완료!")
    except Exception as e:
        print(f"결과 파일 저장 실패: {e}")

    return results


# 개별 결과 접근 함수들


def get_db_info():
    """데이터베이스 정보 반환"""
    return get_results()["db_info"]


def get_table_info_from_results(table_name):
    """
    피클에 저장된 결과에서 특정 테이블의 정보만 반환합니다.
    
    Args:
        table_name (str): 찾고자 하는 테이블 이름
        
    Returns:
        str: 테이블 정보 문자열, 테이블을 찾지 못한 경우 None 반환
    """
    db_info = get_results()["db_info"]
    for table_info in db_info.split("\n\n"):
        if table_info.startswith(table_name + ":"):
            return table_info
    return None


def get_table_names_list():
    """테이블 이름 리스트 반환"""
    return get_results()["table_names_list"]


def get_unique_named_lists():
    """고유 이름 리스트 반환"""
    return get_results()["unique_named_lists"]


def get_sample_tables():
    """샘플 테이블 리스트 반환"""
    return get_results()["sample_tables"]


# 결과 무효화 함수 (필요시 사용)
def invalidate_results():
    """결과 피클 파일을 삭제하여 다음 호출 시 재계산 강제"""
    if os.path.exists(RESULT_FILE):
        os.remove(RESULT_FILE)
        print("결과 캐시 초기화됨")


# pprint(get_table_names_list())

if __name__ == "__main__":
    invalidate_results()
    get_results()
    print(get_table_info_from_results("deal_stream_created_renewal_opportunity"))
