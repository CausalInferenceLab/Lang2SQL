"""
서버 상태 확인 및 연결 관련 기능을 제공하는 유틸리티 클래스입니다.

이 모듈은 HTTP 기반의 서버에 대해 다음과 같은 기능을 제공합니다:
- `/health` 엔드포인트를 통한 서버 헬스 체크
- 향후 서버 연결 또는 상태 점검과 관련된 기능 추가 예정

각 기능은 요청 실패, 타임아웃, 연결 오류 등의 다양한 예외 상황을 포괄적으로 처리하며,
로깅을 통해 상세한 실패 원인을 기록하고 결과를 boolean 또는 적절한 형태로 반환합니다.
"""

import logging
from urllib.parse import urljoin

import requests
from infra.logging import get_logger

# 중앙화된 로깅 시스템 사용
logger = get_logger(__name__)


class CheckServer:
    """
    서버의 상태를 확인하거나 연결을 테스트하는 유틸리티 메서드를 제공하는 클래스입니다.

    현재는 GMS 서버의 `/health` 엔드포인트에 대한 헬스 체크 기능을 포함하고 있으며,
    향후에는 다양한 서버 연결 확인 및 상태 점검 기능이 추가될 수 있도록 확장 가능한 구조로 설계되었습니다.
    모든 기능은 네트워크 오류 및 서버 응답 상태에 따라 예외를 로깅하며, 호출자가 결과를 판단할 수 있도록 boolean 값을 반환합니다.
    """

    @staticmethod
    def is_gms_server_healthy(*, url: str) -> bool:
        """
        지정된 GMS 서버의 `/health` 엔드포인트에 요청을 보내 상태를 확인합니다.

        서버가 HTTP 200 응답을 반환하면 True를 반환하며,
        요청 실패, 타임아웃, 연결 오류 등의 예외 발생 시 False를 반환하고,
        로깅을 통해 상세한 에러 정보를 출력합니다.

        Args:
            url (str): 헬스 체크를 수행할 GMS 서버의 기본 URL (예: "http://localhost:8080")

        Returns:
            bool: 서버가 정상적으로 응답하면 True, 예외 발생 시 False
        """

        health_url = urljoin(url, "/health")

        try:
            response = requests.get(
                health_url,
                timeout=3,
            )
            response.raise_for_status()
            logger.info("GMS server is healthy: %s", url)
            return True
        except (
            requests.exceptions.ConnectTimeout,
            requests.exceptions.ReadTimeout,
        ) as e:
            logger.error(
                "Timeout while connecting to GMS server: %s | %s", health_url, e
            )
        except requests.exceptions.ConnectionError as e:
            logger.error("Failed to connect to GMS server: %s | %s", health_url, e)
        except requests.exceptions.HTTPError as e:
            logger.error("GMS server returned HTTP error: %s | %s", health_url, e)
        except requests.exceptions.RequestException as e:
            logger.exception("Unexpected request error to GMS server: %s", health_url)

        return False
