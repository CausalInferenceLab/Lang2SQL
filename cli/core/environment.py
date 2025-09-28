"""환경 변수 및 VectorDB 초기화 모듈."""

from typing import Optional

from cli.utils.env_loader import load_env, set_prompt_dir, set_vectordb


def initialize_environment(
    *,
    env_file_path: Optional[str],
    prompt_dir_path: Optional[str],
    vectordb_type: str,
    vectordb_location: Optional[str],
) -> None:
    """환경 변수와 VectorDB 설정을 초기화합니다.

    Args:
        env_file_path (Optional[str]): 로드할 .env 파일 경로. None이면 기본값 사용.
        prompt_dir_path (Optional[str]): 프롬프트 템플릿 디렉토리 경로. None이면 설정하지 않음.
        vectordb_type (str): VectorDB 타입 ("faiss" 또는 "pgvector").
        vectordb_location (Optional[str]): VectorDB 위치. None이면 기본값 사용.

    Raises:
        Exception: 초기화 과정에서 오류가 발생한 경우.
    """
    load_env(env_file_path=env_file_path)
    set_prompt_dir(prompt_dir_path=prompt_dir_path)
    set_vectordb(vectordb_type=vectordb_type, vectordb_location=vectordb_location)
