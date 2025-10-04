"""Lang2SQL CLI 프로그램입니다.
이 프로그램은 환경 초기화와 Streamlit 실행을 제공합니다.

주의: --datahub_server 옵션은 더 이상 사용되지 않습니다(deprecated).
DataHub 설정은 UI의 설정 > 데이터 소스 탭에서 관리하세요.
"""

import click

from cli.commands.quary import query_command
from cli.commands.run_streamlit import run_streamlit_cli_command
from cli.core.environment import initialize_environment
from cli.core.streamlit_runner import run_streamlit_command
from cli.utils.logger import configure_logging

from version import __version__

logger = configure_logging()


# pylint: disable=redefined-outer-name,broad-exception-caught
@click.group()
@click.version_option(version=__version__)
@click.pass_context
@click.option(
    "--datahub_server",
    default=None,
    help=("[Deprecated] DataHub GMS URL. 이제는 UI 설정 > 데이터 소스에서 관리하세요."),
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
    default=None,
    help="[Deprecated] VectorDB 타입. 이제는 UI 설정 > 데이터 소스에서 관리하세요.",
)
@click.option(
    "--vectordb-location",
    default=None,
    help="[Deprecated] VectorDB 위치. 이제는 UI 설정 > 데이터 소스에서 관리하세요.",
)
def cli(
    ctx: click.Context,
    datahub_server: str | None,
    run_streamlit: bool,
    port: int,
    env_file_path: str | None = None,
    prompt_dir_path: str | None = None,
    vectordb_type: str | None = None,
    vectordb_location: str | None = None,
) -> None:
    """Lang2SQL CLI 엔트리포인트.

    - 환경 변수 및 VectorDB 설정 초기화
    - 필요 시 Streamlit 애플리케이션 실행
    """

    try:
        initialize_environment(
            env_file_path=env_file_path, prompt_dir_path=prompt_dir_path
        )
    except Exception:
        logger.error("Initialization failed.", exc_info=True)
        ctx.exit(1)

    logger.info(
        "Initialization started: run_streamlit = %s, port = %d",
        run_streamlit,
        port,
    )

    # Deprecated 안내: CLI에서 DataHub 설정은 더 이상 처리하지 않습니다
    if datahub_server:
        click.secho(
            "[Deprecated] --datahub_server 옵션은 더 이상 사용되지 않습니다. 설정 > 데이터 소스 탭에서 설정하세요.",
            fg="yellow",
        )

    # Deprecated 안내: CLI에서 VectorDB 설정은 더 이상 처리하지 않습니다
    if vectordb_type or vectordb_location:
        click.secho(
            "[Deprecated] --vectordb-type/--vectordb-location 옵션은 더 이상 사용되지 않습니다. 설정 > 데이터 소스 탭에서 설정하세요.",
            fg="yellow",
        )

    if run_streamlit:
        run_streamlit_command(port)


cli.add_command(run_streamlit_cli_command)
cli.add_command(query_command)
