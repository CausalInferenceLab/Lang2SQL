"""Streamlit 실행 유틸리티 모듈."""

import subprocess

from cli.utils.logger import configure_logging

logger = configure_logging()


def run_streamlit_command(port: int) -> None:
    """지정된 포트에서 Streamlit 애플리케이션을 실행합니다.

    Args:
        port (int): 바인딩할 포트 번호.

    Raises:
        subprocess.CalledProcessError: 실행 실패 시 발생.
    """
    logger.info("Starting Streamlit application on port %d...", port)

    try:
        subprocess.run(
            [
                "streamlit",
                "run",
                "interface/streamlit_app.py",
                "--server.address=0.0.0.0",
                "--server.port",
                str(port),
            ],
            check=True,
        )
        logger.info("Streamlit application started successfully.")
    except subprocess.CalledProcessError as e:
        logger.error("Failed to start Streamlit application: %s", e)
        raise
