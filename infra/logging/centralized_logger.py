"""
중앙화된 로깅 설정 모듈

이 모듈은 프로젝트 전체에서 사용할 로깅 설정을 중앙에서 관리합니다.
logging.basicConfig()의 중복 호출을 방지하고 일관된 로깅 포맷을 제공합니다.
"""

import logging
import os
from typing import Optional

# 전역 변수로 로깅 설정 상태 추적
_logging_configured = False

def configure_logging(level: Optional[str] = None, force: bool = False) -> None:
    """
    중앙화된 로깅 설정을 수행합니다.
    
    이 함수는 한 번만 호출되어야 하며, 중복 호출을 방지합니다.
    
    Args:
        level (Optional[str]): 로깅 레벨 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
                              None인 경우 환경변수 LOG_LEVEL 또는 기본값 INFO 사용
        force (bool): 이미 설정된 경우에도 강제로 재설정할지 여부
    """
    global _logging_configured
    
    if _logging_configured and not force:
        return
    
    # 로깅 레벨 결정
    if level is None:
        level = os.getenv("LOG_LEVEL", "INFO").upper()
    
    # 로깅 레벨 유효성 검사
    valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    if level not in valid_levels:
        level = "INFO"
    
    # 중앙화된 로깅 설정
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True  # 기존 핸들러 제거 후 재설정
    )
    
    _logging_configured = True

def get_logger(name: str) -> logging.Logger:
    """
    지정된 이름의 로거를 반환합니다.
    
    로깅이 아직 설정되지 않은 경우 자동으로 기본 설정을 수행합니다.
    
    Args:
        name (str): 로거 이름 (보통 __name__ 사용)
        
    Returns:
        logging.Logger: 설정된 로거 인스턴스
    """
    if not _logging_configured:
        configure_logging()
    
    return logging.getLogger(name)
