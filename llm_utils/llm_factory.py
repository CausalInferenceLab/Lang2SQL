# llm_factory.py
from typing import Optional

from langchain.llms.base import BaseLanguageModel
from langchain_openai import ChatOpenAI


def get_llm(
    model_type: str,
    model_name: Optional[str] = None,
    openai_api_key: Optional[str] = None,
    **kwargs,
) -> BaseLanguageModel:
    """
    주어진 model_type과 model_name 등에 따라 적절한 LLM 객체를 생성/반환한다.
    """
    if model_type == "openai":
        return ChatOpenAI(
            model=model_name,
            api_key=openai_api_key,
            **kwargs,
        )

    else:
        raise ValueError(f"지원하지 않는 model_type: {model_type}")
