from dotenv import load_dotenv

load_dotenv()

from langchain.schema import Document

# set_debug(True)


from .prompt import *
from .retriever_chroma_db import chroma_db_generator

# from prompt import *
# from agent.query_generator.retriever_chroma_db import chroma_db_generator


def add_to_vectorstore(question: str, answer: str, index_name: str, meta: dict = None):
    """
    사용자의 질문과 답변을 벡터스토어에 저장합니다.

    """
    if meta is None:
        meta = {}

    # 문서 생성
    doc = Document(page_content=f"Q: {question}\nA: {answer}", metadata=meta)

    # 벡터스토어에 문서 저장
    try:
        # 문서를 포함한 벡터스토어 생성 또는 로드 (한 번에 처리)
        db = chroma_db_generator(index_name, [doc])
        print(f"문서가 {index_name} 벡터스토어에 저장되었습니다.")
        return True
    except Exception as e:
        print(f"{index_name} 벡터스토어 저장 중 오류 발생: {e}")
        return False



def add_to_few_shot_vectorstore(question: str, answer: str, meta: dict = None):
    
    return add_to_vectorstore(question, answer, "few_shot", meta)
