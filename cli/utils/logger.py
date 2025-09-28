"""CLI 전용 로깅 유틸리티 모듈."""

import logging
from infra.logging import get_logger, configure_logging as configure_centralized_logging


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    """로깅을 설정하고 기본 로거를 반환합니다.

    Args:
        level (int, optional): 로깅 레벨. 기본값은 logging.INFO.

    Returns:
        logging.Logger: 설정된 로거 인스턴스.
    """
    # 중앙화된 로깅 설정 사용
    level_name = logging.getLevelName(level)
    configure_centralized_logging(level=level_name)
    return get_logger("cli")
