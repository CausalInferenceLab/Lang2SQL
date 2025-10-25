"""
Lang2SQL 실행 모듈.

이 모듈은 자연어로 입력된 질문을 SQL 쿼리로 변환하고,
지정된 데이터베이스 환경에서 실행하는 함수(`run_lang2sql`)를 제공합니다.
내부적으로 `engine.query_executor.execute_query`를 호출하여
Lang2SQL 전체 파이프라인을 간단히 실행할 수 있도록 합니다.
"""

from engine.query_executor import execute_query as execute_query_common


def run_lang2sql(
    query,
    database_env,
    retriever_name,
    top_n,
    device,
    use_enriched,
):
    """
    Lang2SQL 실행 함수.

    주어진 자연어 질문을 SQL 쿼리로 변환하고 지정된 데이터베이스 환경에서 실행합니다.
    내부적으로 `engine.query_executor.execute_query`를 호출합니다.

    Args:
        query (str): 사용자 입력 자연어 질문.
        database_env (str): 사용할 데이터베이스 환경 이름.
        retriever_name (str): 검색기(retriever) 유형 이름.
        top_n (int): 검색할 테이블 정보 개수.
        device (str): 모델 실행 장치 ("cpu" 또는 "cuda").

    Returns:
        dict: Lang2SQL 실행 결과를 담은 딕셔너리.
    """

    return execute_query_common(
        query=query,
        database_env=database_env,
        retriever_name=retriever_name,
        top_n=top_n,
        device=device,
        use_enriched_graph=use_enriched,
    )
