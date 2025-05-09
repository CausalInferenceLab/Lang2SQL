import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import Chroma


from llm_utils.tools import get_info_from_db
from table_info import table_info

from prepositions_conjunctions_list import prepositions_conjunctions_list


table_info = table_info
embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
persist_directory = os.path.join(os.getcwd(), "table_info_chroma_db")


def format_docs(docs):
    return "\n\n".join(doc.page_content for doc in docs)

def chroma_db_generator(index_name, table_info):
    try:
    # Chroma DB 로드 시도
        db = Chroma(
            collection_name=index_name,
            persist_directory=persist_directory,
            embedding_function=embeddings
        )
        # 컬렉션이 비어있는지 확인 (선택적)
        if db._collection.count() == 0:
            raise ValueError("빈 Chroma 컬렉션")
        
    except Exception as e:
        print(f"Chroma DB를 로드하지 못했습니다: {e}")
        print("새 Chroma DB를 생성합니다...")
        
        # 기존 정보 가져오기
        documents = table_info
        
        # 새 Chroma DB 생성 
        db = Chroma.from_documents(
            collection_name=index_name,
            documents=documents,
            embedding=embeddings,
            persist_directory=persist_directory
        )   

    return db



def table_name_splitter():
    # table_info = get_info_from_db()
    
    table_names_list = []
    split_table_names = []
    
    for table in table_info:
        table_name = table.page_content.split(':')[0].strip()
        table_names_list.append(table_name)
        split_names = table_name.split('_')
        split_table_names.append(split_names)
        

    max_len = max(len(names) for names in split_table_names)

    result_lists = [[] for _ in range(max_len)]


    for names in split_table_names:
        for idx in range(max_len):
            if idx < len(names):
                if names[idx] not in prepositions_conjunctions_list:
                    result_lists[idx].append(names[idx])
                else:
                    result_lists[idx].append('')  


    ordinal_names = [
    "first", "second", "third", "fourth", "fifth",
    "sixth", "seventh", "eighth", "ninth", "tenth",
    "eleventh", "twelfth", "thirteenth", "fourteenth", "fifteenth"
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
        
    return table_names_list, unique_named_lists


# result_storage.py - 새로운 파일로 만들기
import pickle
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


# 저장 파일 경로
PERSIST_DIRECTORY = os.path.join(os.getcwd(), "table_info_chroma_db")
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
            with open(RESULT_FILE, 'rb') as f:
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
    
    # 계산 수행
    table_names_list, unique_named_lists = table_name_splitter()
    table_names_vector_store = chroma_db_generator(index_name="table-name", table_info=table_names_list)
    
    # 결과 저장 (벡터 스토어는 이미 디스크에 저장됨)
    results = {
        "table_names_list": table_names_list,
        "unique_named_lists": unique_named_lists
    }
    
    # 피클 파일에 저장
    try:
        print(f"결과 파일 저장 중: {RESULT_FILE}")
        with open(RESULT_FILE, 'wb') as f:
            pickle.dump(results, f)
        print("결과 파일 저장 완료!")
    except Exception as e:
        print(f"결과 파일 저장 실패: {e}")
    
    # 벡터 스토어는 이미 디스크에 저장되어 있으므로 메모리에만 추가
    results["table_names_vector_store"] = table_names_vector_store
    
    return results

# 개별 결과 접근 함수들
def get_table_names_list():
    """테이블 이름 리스트 반환"""
    return get_results()["table_names_list"]

def get_unique_named_lists():
    """고유 이름 리스트 반환"""
    return get_results()["unique_named_lists"]


# 결과 무효화 함수 (필요시 사용)
def invalidate_results():
    """결과 피클 파일을 삭제하여 다음 호출 시 재계산 강제"""
    if os.path.exists(RESULT_FILE):
        os.remove(RESULT_FILE)
        print("결과 캐시 초기화됨")