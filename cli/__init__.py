"""
Datahub GMS 서버 URL을 설정하고, 필요 시 Streamlit 인터페이스를 실행하는 CLI 프로그램입니다.
"""

import os
import subprocess

import click
import dotenv

from llm_utils.tools import set_gms_server


@click.group()
@click.version_option(version="0.1.4")
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
# pylint: disable=redefined-outer-name
def cli(
    ctx: click.Context,
    datahub_server: str,
    run_streamlit: bool,
    port: int,
    env_file_path: str = None,
    prompt_dir_path: str = None,
) -> None:
    """
    Datahub GMS 서버 URL을 설정하고, Streamlit 애플리케이션을 실행할 수 있는 CLI 명령 그룹입니다.

    이 함수는 다음 역할을 수행합니다:
    - 전달받은 'datahub_server' URL을 바탕으로 GMS 서버 연결을 설정합니다.
    - 설정 과정 중 오류가 발생하면 오류 메시지를 출력하고 프로그램을 종료합니다.
    - '--run-streamlit' 옵션이 활성화된 경우, 지정된 포트에서 Streamlit 웹 앱을 즉시 실행합니다.
    - '--env-file-path' 옵션이 지정된 경우, 해당 .env 파일에서 환경 변수를 로드합니다.
    - '--prompt-dir-path' 옵션이 지정된 경우, 해당 디렉토리에서 프롬프트 템플릿을 로드합니다.

    매개변수:
        ctx (click.Context): 명령어 실행 컨텍스트 객체입니다.
        datahub_server (str): 설정할 Datahub GMS 서버의 URL입니다.
        run_streamlit (bool): Streamlit 앱을 실행할지 여부를 나타내는 플래그입니다.
        port (int): Streamlit 서버가 바인딩될 포트 번호입니다.
        env_file_path (str, optional): 환경 변수를 로드할 .env 파일 경로입니다.
        prompt_dir_path (str, optional): 프롬프트 템플릿을 로드할 디렉토리 경로입니다.

    주의:
        'set_gms_server' 함수에서 ValueError가 발생할 경우, 프로그램은 비정상 종료(exit code 1)합니다.
    """

    # 환경 변수 파일 로드
    if env_file_path:
        try:
            if not dotenv.load_dotenv(env_file_path, override=True):
                click.secho(f"환경 변수 파일 로드 실패: {env_file_path}", fg="yellow")
            else:
                click.secho(f"환경 변수 파일 로드 성공: {env_file_path}", fg="green")
        except Exception as e:
            click.secho(f"환경 변수 로드 중 오류 발생: {str(e)}", fg="red")
            ctx.exit(1)

    # 프롬프트 디렉토리를 환경 변수로 설정
    if prompt_dir_path:
        try:
            os.environ["PROMPT_TEMPLATES_DIR"] = prompt_dir_path
            click.secho(
                f"프롬프트 디렉토리 환경변수 설정됨: {prompt_dir_path}", fg="green"
            )
        except Exception as e:
            click.secho(f"프롬프트 디렉토리 환경변수 설정 실패: {str(e)}", fg="red")
            ctx.exit(1)

    try:
        set_gms_server(datahub_server)
    except ValueError as e:
        click.secho(f"GMS 서버 URL 설정 실패: {str(e)}", fg="red")
        ctx.exit(1)
    if run_streamlit:
        run_streamlit_command(port)


def run_streamlit_command(port: int) -> None:
    """
    지정된 포트에서 Streamlit 애플리케이션을 실행하는 함수입니다.

    이 함수는 subprocess를 통해 'streamlit run' 명령어를 실행하여
    'interface/streamlit_app.py' 파일을 웹 서버 형태로 구동합니다.
    사용자가 지정한 포트 번호를 Streamlit 서버의 포트로 설정합니다.

    매개변수:
        port (int): Streamlit 서버가 바인딩될 포트 번호입니다.

    주의:
        - Streamlit이 시스템에 설치되어 있어야 정상 동작합니다.
        - subprocess 호출 실패 시 예외가 발생할 수 있습니다.
    """

    try:
        subprocess.run(
            [
                "streamlit",
                "run",
                "interface/streamlit_app.py",
                "--server.port",
                str(port),
            ],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        click.echo(f"Streamlit 실행 실패: {e}")
        raise


@cli.command(name="run-streamlit")
@click.option(
    "-p",
    "--port",
    type=int,
    default=8501,
    help=(
        "Streamlit 애플리케이션이 바인딩될 포트 번호를 지정합니다. "
        "기본 포트는 8501이며, 필요 시 포트 충돌을 피하거나 "
        "여러 인스턴스를 동시에 실행할 때 다른 포트 번호를 설정할 수 있습니다."
    ),
)
def run_streamlit_cli_command(port: int) -> None:
    """
    CLI 명령어를 통해 Streamlit 애플리케이션을 실행하는 함수입니다.

    이 명령은 'interface/streamlit_app.py' 파일을 Streamlit 서버로 구동하며,
    사용자가 지정한 포트 번호를 바인딩하여 웹 인터페이스를 제공합니다.

    매개변수:
        port (int): Streamlit 서버가 사용할 포트 번호입니다. 기본값은 8501입니다.

    주의:
        - Streamlit이 시스템에 설치되어 있어야 정상적으로 실행됩니다.
        - Streamlit 실행에 실패할 경우 subprocess 호출에서 예외가 발생할 수 있습니다.
    """

    run_streamlit_command(port)
