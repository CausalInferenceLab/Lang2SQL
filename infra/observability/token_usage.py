"""
token_usage.py

LLM 응답 메시지에서 토큰 사용량을 집계하기 위한 유틸리티 모듈입니다.

이 모듈은 LLM의 `usage_metadata` 필드를 기반으로 입력 토큰, 출력 토큰, 총 토큰 사용량을 계산하는 기능을 제공합니다.
Streamlit, LangChain 등 LLM 응답을 다루는 애플리케이션에서 비용 분석, 사용량 추적 등에 활용할 수 있습니다.
"""

import logging
from typing import Any, List
from infra.logging import get_logger

# 중앙화된 로깅 시스템 사용
logger = get_logger(__name__)


class TokenUtils:
    """
    LLM 토큰 사용량 집계 유틸리티 클래스입니다.

    이 클래스는 LLM 응답 메시지 리스트에서 usage_metadata 필드를 추출하여
    input_tokens, output_tokens, total_tokens의 합계를 계산합니다.

    예를 들어, LangChain 또는 OpenAI API 응답 메시지 객체의 토큰 사용 정보를 분석하고자 할 때
    활용할 수 있습니다.

    사용 예:
        >>> from infra.observability.token_usage import TokenUtils
        >>> summary = TokenUtils.get_token_usage_summary(messages)
        >>> print(summary["total_tokens"])

    반환 형식:
        {
            "input_tokens": int,
            "output_tokens": int,
            "total_tokens": int,
        }
    """

    @staticmethod
    def get_token_usage_summary(*, data: List[Any]) -> dict:
        """
        메시지 데이터에서 input/output/total 토큰 사용량을 각각 집계합니다.

        Args:
            data (List[Any]): 각 항목이 usage_metadata를 포함할 수 있는 객체 리스트.

        Returns:
            dict: {
                "input_tokens": int,
                "output_tokens": int,
                "total_tokens": int
            }
        """

        input_tokens = 0
        output_tokens = 0
        total_tokens = 0

        for idx, item in enumerate(data):
            token_usage = getattr(item, "usage_metadata", {})
            in_tok = token_usage.get("input_tokens", 0)
            out_tok = token_usage.get("output_tokens", 0)
            total_tok = token_usage.get("total_tokens", 0)

            logger.debug(
                "Message[%d] → input=%d, output=%d, total=%d",
                idx,
                in_tok,
                out_tok,
                total_tok,
            )

            input_tokens += in_tok
            output_tokens += out_tok
            total_tokens += total_tok

        logger.info(
            "Token usage summary → input: %d, output: %d, total: %d",
            input_tokens,
            output_tokens,
            total_tokens,
        )

        return {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": total_tokens,
        }
