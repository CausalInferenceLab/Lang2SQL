"""Streamlit 실행 CLI 명령어 모듈."""

import click

from cli.core.streamlit_runner import run_streamlit_command
from cli.utils.logger import configure_logging

logger = configure_logging()


@click.command(name="run-streamlit")
@click.option(
    "-p",
    "--port",
    type=int,
    default=8501,
    help=(
        "Streamlit 애플리케이션이 바인딩될 포트 번호를 지정합니다. "
        "기본 포트는 8501이며, 필요 시 다른 포트를 설정할 수 있습니다."
    ),
)
def run_streamlit_cli_command(port: int) -> None:
    """CLI 명령어로 Streamlit 애플리케이션을 실행합니다.

    Args:
        port (int): Streamlit 서버가 바인딩될 포트 번호. 기본값은 8501.
    """
    logger.info("Executing 'run-streamlit' command on port %d...", port)
    run_streamlit_command(port)
