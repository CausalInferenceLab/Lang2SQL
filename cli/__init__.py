"""Lang2SQL CLI 프로그램입니다.
이 프로그램은 Datahub GMS 서버 URL을 설정하고, 필요 시 Streamlit 인터페이스를 실행합니다.

명령어 예시: lang2sql --datahub_server http://localhost:8080 --run-streamlit
"""

import click

from cli.commands.quary import query_command
from cli.commands.run_streamlit import run_streamlit_cli_command
from cli.core.environment import initialize_environment
from cli.core.streamlit_runner import run_streamlit_command
from cli.utils.logger import configure_logging
from infra.monitoring.check_server import CheckServer
from llm_utils.tools import set_gms_server
from version import __version__

logger = configure_logging()


# pylint: disable=redefined-outer-name,broad-exception-caught
@click.group()
@click.version_option(version=__version__)
@click.pass_context
@click.option(
    "--datahub_server",
    default="http://localhost:8080",
    help=(
        "Datahub GMS 서버의 URL을 설정합니다. "
        "기본값은 'http://localhost:8080'이며, "
        "운영 환경 또는 테스트 환경에 맞게 변경할 수 있습니다."
    ),
)
@click.option(
    "--run-streamlit",
    is_flag=True,
    help=(
        "이 옵션을 지정하면 CLI 실행 시 Streamlit 애플리케이션을 바로 실행합니다. "
        "별도의 명령어 입력 없이 웹 인터페이스를 띄우고 싶을 때 사용합니다."
    ),
)
@click.option(
    "-p",
    "--port",
    type=int,
    default=8501,
    help=(
        "Streamlit 서버가 바인딩될 포트 번호를 지정합니다. "
        "기본 포트는 8501이며, 포트 충돌을 피하거나 여러 인스턴스를 실행할 때 변경할 수 있습니다."
    ),
)
@click.option(
    "--env-file-path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True),
    help="환경 변수를 로드할 .env 파일의 경로를 지정합니다. 지정하지 않으면 기본 경로를 사용합니다.",
)
@click.option(
    "--prompt-dir-path",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True),
    help="프롬프트 템플릿(.md 파일)이 저장된 디렉토리 경로를 지정합니다. 지정하지 않으면 기본 경로를 사용합니다.",
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
def cli(
    ctx: click.Context,
    datahub_server: str,
    run_streamlit: bool,
    port: int,
    env_file_path: str | None = None,
    prompt_dir_path: str | None = None,
    vectordb_type: str = "faiss",
    vectordb_location: str = None,
) -> None:
    """Lang2SQL CLI 엔트리포인트.

    - 환경 변수 및 VectorDB 설정 초기화
    - GMS 서버 연결 및 헬스체크
    - 필요 시 Streamlit 애플리케이션 실행
    """

    try:
        initialize_environment(
            env_file_path=env_file_path,
            prompt_dir_path=prompt_dir_path,
            vectordb_type=vectordb_type,
            vectordb_location=vectordb_location,
        )
    except Exception:
        logger.error("Initialization failed.", exc_info=True)
        ctx.exit(1)

    logger.info(
        "Initialization started: GMS server = %s, run_streamlit = %s, port = %d",
        datahub_server,
        run_streamlit,
        port,
    )

    if CheckServer.is_gms_server_healthy(url=datahub_server):
        set_gms_server(datahub_server)
        logger.info("GMS server URL successfully set: %s", datahub_server)
    else:
        logger.error("GMS server health check failed. URL: %s", datahub_server)
        # ctx.exit(1)

    if run_streamlit:
        run_streamlit_command(port)


cli.add_command(run_streamlit_cli_command)
cli.add_command(query_command)
