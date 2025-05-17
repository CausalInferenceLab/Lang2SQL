"""
이 모듈은 프롬프트 템플릿을 로드하는 기능을 제공합니다.
- 프롬프트 템플릿은 마크다운 파일로 관리되고 있으며, 환경변수에서 템플릿 디렉토리를 가져오거나, 없으면 현재 파일 위치 기준으로 설정합니다.
"""

import os


def get_prompt_template(prompt_name: str) -> str:
    # 환경변수에서 템플릿 디렉토리를 가져오거나, 없으면 현재 파일 위치 기준으로 설정
    templates_dir = os.environ.get("PROMPT_TEMPLATES_DIR", os.path.dirname(__file__))

    try:
        template_path = os.path.join(templates_dir, f"{prompt_name}.md")
        with open(template_path, "r", encoding="utf-8") as f:
            template = f.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"경고: '{prompt_name}.md' 파일을 찾을 수 없습니다.")

    return template
