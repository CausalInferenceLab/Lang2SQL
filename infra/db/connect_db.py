"""
ClickHouse 데이터베이스에 연결하고 SQL 쿼리를 실행하여 결과를 pandas DataFrame으로 반환하는 모듈입니다.

구성 요소:
- 환경 변수에서 접속 정보를 불러옵니다.
- ClickHouse에 지연 연결(lazy connection)을 수행합니다.
- SQL 쿼리를 실행하고 결과를 pandas DataFrame으로 반환합니다.
- 연결 실패 및 쿼리 오류에 대해 로깅을 통해 디버깅을 지원합니다.
"""

import logging
import os
from typing import Optional

import pandas as pd
from clickhouse_driver import Client
from infra.logging import get_logger

# 중앙화된 로깅 시스템 사용
logger = get_logger(__name__)


class ConnectDB:
    """
    ClickHouse 데이터베이스에 연결하고 SQL 쿼리를 실행하는 클래스입니다.

    환경 변수에서 접속 정보를 읽어들이며, 실제 연결은 SQL 실행 시점에 수행됩니다.
    연결된 클라이언트를 통해 SQL 쿼리를 실행하고, 결과를 pandas DataFrame으로 반환합니다.
    """

    def __init__(self):
        """
        ConnectDB 클래스의 인스턴스를 초기화합니다.

        환경 변수에서 ClickHouse 접속 정보를 불러오며, 연결은 즉시 수행하지 않습니다.
        """

        self.client: Optional[Client] = None
        self.is_connected: bool = False

        self.host = self._get_env_or_raise("CLICKHOUSE_HOST")
        self.dbname = self._get_env_or_raise("CLICKHOUSE_DATABASE")
        self.user = self._get_env_or_raise("CLICKHOUSE_USER")
        self.password = os.getenv("CLICKHOUSE_PASSWORD", "")
        self.port = int(self._get_env_or_raise("CLICKHOUSE_PORT"))

    def _get_env_or_raise(self, var_name: str) -> str:
        """
        주어진 환경변수를 읽고, 값이 없으면 예외를 발생시킵니다.

        Parameters:
            var_name (str): 환경 변수 이름

        Returns:
            str: 환경 변수의 값

        Raises:
            ValueError: 값이 없을 경우
        """

        value = os.getenv(var_name)
        if not value:
            logger.error("Environment variable '%s' is not set.", var_name)
            raise ValueError(f"Environment variable '{var_name}' is not set.")
        return value

    def connect_to_clickhouse(self) -> None:
        """
        ClickHouse 서버에 연결을 시도합니다.

        연결 성공 시 client가 초기화되며, 실패 시 로그를 남기고 예외를 발생시킵니다.
        """

        try:
            self.client = Client(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.dbname,
            )
            self.is_connected = True
            logger.info("Successfully connected to ClickHouse.")
        except Exception as e:
            self.is_connected = False
            logger.error("Failed to connect to ClickHouse: %s", e)
            raise

    def run_sql(self, sql: str) -> pd.DataFrame:
        """
        주어진 SQL 쿼리를 실행하고 결과를 pandas DataFrame으로 반환합니다.

        연결이 설정되지 않은 경우 자동으로 연결을 시도합니다.

        Parameters:
            sql (str): 실행할 SQL 쿼리 문자열

        Returns:
            pd.DataFrame: 쿼리 결과를 포함한 DataFrame

        Raises:
            Exception: SQL 실행 중 오류가 발생한 경우 예외를 발생시킵니다.
        """

        if not self.is_connected or not self.client:
            logger.warning("ClickHouse client not connected. Attempting to connect...")
            self.connect_to_clickhouse()

        try:
            result = self.client.execute(sql, with_column_types=True)
            rows, columns = result
            column_names = [col[0] for col in columns]
            df = pd.DataFrame(rows, columns=column_names)
            return df
        except Exception as e:
            logger.exception("An error occurred while executing SQL: %s", e)
            raise
