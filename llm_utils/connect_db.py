"""
이 모듈은 ClickHouse 데이터베이스에 연결하고 SQL 쿼리를 실행하여 결과를 pandas DataFrame으로 반환하는 기능을 제공합니다.

구성 요소:
- 환경 변수에서 접속 정보를 불러와 ClickHouse 서버에 연결합니다.
- SQL 쿼리를 실행하고 결과를 pandas DataFrame으로 반환합니다.
- 연결 실패 및 쿼리 오류에 대해 로깅을 통해 디버깅을 지원합니다.
"""

import logging
import os
from typing import Optional

import pandas as pd
from clickhouse_driver import Client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class ConnectDB:
    """
    ClickHouse 데이터베이스에 연결하고 SQL 쿼리를 실행하는 클래스입니다.

    환경 변수에서 접속 정보를 로드하여 ClickHouse 서버에 연결하며,
    SQL 쿼리 실행 결과를 pandas DataFrame으로 반환합니다.
    """

    def __init__(self):
        """
        ConnectDB 클래스의 인스턴스를 초기화합니다.

        환경 변수에서 ClickHouse 접속 정보를 읽고, 즉시 서버에 연결을 시도합니다.
        """

        self.client: Optional[Client] = None
        self.host = os.getenv("CLICKHOUSE_HOST")
        self.dbname = os.getenv("CLICKHOUSE_DATABASE")
        self.user = os.getenv("CLICKHOUSE_USER")
        self.password = os.getenv("CLICKHOUSE_PASSWORD")
        self.port = os.getenv("CLICKHOUSE_PORT")

        self.connect_to_clickhouse()

    def connect_to_clickhouse(self) -> None:
        """
        ClickHouse 서버에 연결을 시도합니다.

        연결에 성공하면 client 객체가 설정되며, 실패 시 예외를 발생시킵니다.
        연결 상태는 로깅을 통해 출력됩니다.
        """

        try:
            self.client = Client(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.dbname,
            )
            logger.info("Successfully connected to ClickHouse.")
        except Exception as e:
            logger.error("Failed to connect to ClickHouse: %s", e)
            raise

    def run_sql(self, sql: str) -> pd.DataFrame:
        """
        SQL 쿼리를 실행하고 결과를 pandas DataFrame으로 반환합니다.
        내부적으로 ClickHouse 클라이언트가 없으면 자동으로 재연결을 시도합니다.

        Parameters:
            sql (str): 실행할 SQL 쿼리 문자열

        Returns:
            pd.DataFrame: 쿼리 실행 결과를 담은 DataFrame 객체

        Raises:
            Exception: SQL 실행 중 오류 발생 시 예외를 발생시킵니다.
        """

        if not self.client:
            logger.warning(
                "ClickHouse client is not initialized. Attempting to reconnect..."
            )
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
