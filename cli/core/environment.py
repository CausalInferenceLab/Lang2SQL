"""환경 변수 초기화 모듈 (VectorDB 설정은 UI에서 관리)."""

from typing import Optional

from cli.utils.env_loader import load_env, set_prompt_dir


def initialize_environment(
    *,
    env_file_path: Optional[str],
    prompt_dir_path: Optional[str],
) -> None:
    """환경 변수를 초기화합니다. VectorDB 설정은 UI에서 관리합니다.

    Args:
        env_file_path (Optional[str]): 로드할 .env 파일 경로. None이면 기본값 사용.
        prompt_dir_path (Optional[str]): 프롬프트 템플릿 디렉토리 경로. None이면 설정하지 않음.

    Raises:
        Exception: 초기화 과정에서 오류가 발생한 경우.
    """
    load_env(env_file_path=env_file_path)
    set_prompt_dir(prompt_dir_path=prompt_dir_path)
