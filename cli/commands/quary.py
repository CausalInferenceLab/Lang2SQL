"""자연어 질문을 SQL 쿼리로 변환하는 CLI 명령어 정의 모듈.

이 모듈은 사용자가 입력한 자연어 질문을 SQL 쿼리로 변환하여 출력하는
`query` CLI 명령어를 제공합니다.
"""

import os

import click

from cli.utils.logger import configure_logging

logger = configure_logging()


@click.command(name="query")
@click.argument("question", type=str)
@click.option(
    "--database-env",
    default="clickhouse",
    help="사용할 데이터베이스 환경 (기본값: clickhouse)",
)
@click.option(
    "--retriever-name",
    default="기본",
    help="테이블 검색기 이름 (기본값: 기본)",
)
@click.option(
    "--top-n",
    type=int,
    default=5,
    help="검색된 상위 테이블 수 제한 (기본값: 5)",
)
@click.option(
    "--device",
    default="cpu",
    help="LLM 실행에 사용할 디바이스 (기본값: cpu)",
)
@click.option(
    "--use-enriched-graph",
    is_flag=True,
    help="확장된 그래프(프로파일 추출 + 컨텍스트 보강) 사용 여부",
)
@click.option(
    "--vectordb-type",
    type=click.Choice(["faiss", "pgvector"]),
    default="faiss",
    help="사용할 벡터 데이터베이스 타입 (기본값: faiss)",
)
@click.option(
    "--vectordb-location",
    help=(
        "VectorDB 위치 설정\n"
        "- FAISS: 디렉토리 경로 (예: ./my_vectordb)\n"
        "- pgvector: 연결 문자열 (예: postgresql://user:pass@host:port/db)\n"
        "기본값: FAISS는 './dev/table_info_db', pgvector는 환경변수 사용"
    ),
)
def query_command(
    question: str,
    database_env: str,
    retriever_name: str,
    top_n: int,
    device: str,
    use_enriched_graph: bool,
    vectordb_type: str = "faiss",
    vectordb_location: str = None,
) -> None:
    """자연어 질문을 SQL 쿼리로 변환하여 출력합니다.

    Args:
        question (str): SQL로 변환할 자연어 질문
        database_env (str): 사용할 데이터베이스 환경
        retriever_name (str): 테이블 검색기 이름
        top_n (int): 검색된 상위 테이블 수 제한
        device (str): LLM 실행 디바이스
        use_enriched_graph (bool): 확장된 그래프 사용 여부
        vectordb_type (str): 벡터 데이터베이스 타입 ("faiss" 또는 "pgvector")
        vectordb_location (Optional[str]): 벡터DB 경로 또는 연결 URL
    """
    try:
        from engine.query_executor import execute_query, extract_sql_from_result

        os.environ["VECTORDB_TYPE"] = vectordb_type

        if vectordb_location:
            os.environ["VECTORDB_LOCATION"] = vectordb_location

        res = execute_query(
            query=question,
            database_env=database_env,
            retriever_name=retriever_name,
            top_n=top_n,
            device=device,
            use_enriched_graph=use_enriched_graph,
        )

        sql = extract_sql_from_result(res)
        if sql:
            print(sql)
        else:
            generated_query = res.get("generated_query")
            if generated_query:
                query_text = (
                    generated_query.content
                    if hasattr(generated_query, "content")
                    else str(generated_query)
                )
                print(query_text)

    except Exception as e:
        logger.error("쿼리 처리 중 오류 발생: %s", e)
        raise
