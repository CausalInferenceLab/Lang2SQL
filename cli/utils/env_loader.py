"""환경 변수 유틸리티 모듈.

.env 파일 로드, 프롬프트 디렉토리 설정,
VectorDB 타입 및 위치 설정을 제공합니다.
"""

import os
from pathlib import Path
from typing import Optional

import click
import dotenv


def load_env(
    *,
    env_file_path: Optional[str] = None,
) -> None:
    """환경 변수 파일(.env)을 로드합니다.

    Args:
        env_file_path (Optional[str]): .env 파일 경로. None이면 기본 경로 사용.
    """
    try:
        if env_file_path:
            loaded = dotenv.load_dotenv(env_file_path, override=True)
            if loaded:
                click.secho(f".env 파일 로드 성공: {env_file_path}", fg="green")
            else:
                click.secho(f".env 파일을 찾을 수 없음: {env_file_path}", fg="yellow")
        else:
            dotenv.load_dotenv(override=True)
            click.secho("기본 .env 파일 로드 시도", fg="blue")
    except Exception as e:
        click.secho(f".env 파일 로드 중 오류 발생: {e}", fg="red")
        raise


def set_prompt_dir(
    *,
    prompt_dir_path: Optional[str],
) -> None:
    """프롬프트 템플릿 디렉토리 경로를 설정합니다.

    Args:
        prompt_dir_path (Optional[str]): 디렉토리 경로. None이면 설정하지 않음.

    Raises:
        ValueError: 경로가 유효하지 않을 경우.
    """
    if not prompt_dir_path:
        click.secho(
            "프롬프트 디렉토리 경로가 지정되지 않아 설정을 건너뜁니다.", fg="yellow"
        )
        return

    path_obj = Path(prompt_dir_path)
    if not path_obj.exists() or not path_obj.is_dir():
        click.secho(f"유효하지 않은 디렉토리 경로: {prompt_dir_path}", fg="red")
        raise ValueError(f"Invalid prompt directory path: {prompt_dir_path}")

    os.environ["PROMPT_TEMPLATES_DIR"] = str(path_obj.resolve())
    click.secho(f"프롬프트 디렉토리 환경변수 설정됨: {path_obj.resolve()}", fg="green")


def set_vectordb(
    *,
    vectordb_type: str,
    vectordb_location: Optional[str] = None,
) -> None:
    """VectorDB 타입과 위치를 설정합니다.

    Args:
        vectordb_type (str): VectorDB 타입 ("faiss" 또는 "pgvector").
        vectordb_location (Optional[str]): 경로 또는 연결 URL.

    Raises:
        ValueError: 잘못된 타입이나 경로/URL일 경우.
    """

    if vectordb_type not in ("faiss", "pgvector"):
        raise ValueError(f"지원하지 않는 VectorDB 타입: {vectordb_type}")

    os.environ["VECTORDB_TYPE"] = vectordb_type
    click.secho(f"VectorDB 타입 설정됨: {vectordb_type}", fg="green")

    if vectordb_location:
        if vectordb_type == "faiss":
            path = Path(vectordb_location)
            if not path.exists() or not path.is_dir():
                raise ValueError(
                    f"유효하지 않은 FAISS 디렉토리 경로: {vectordb_location}"
                )
        elif vectordb_type == "pgvector":
            if not vectordb_location.startswith("postgresql://"):
                raise ValueError(
                    f"pgvector URL은 'postgresql://'로 시작해야 합니다: {vectordb_location}"
                )

        os.environ["VECTORDB_LOCATION"] = vectordb_location
        click.secho(f"VectorDB 경로 설정됨: {vectordb_location}", fg="green")
    else:
        click.secho("VectorDB 경로가 지정되지 않아 기본값을 사용합니다.", fg="yellow")
