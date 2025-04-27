import os
from typing import Union

import pandas as pd
from clickhouse_driver import Client
from dotenv import load_dotenv

# 환경변수
load_dotenv()


class ConnectDB:
    def __init__(self):
        self.client = None
        self.host = os.getenv("CLICKHOUSE_HOST")
        self.dbname = os.getenv("CLICKHOUSE_DATABASE")
        self.user = os.getenv("CLICKHOUSE_USER")
        self.password = os.getenv("CLICKHOUSE_PASSWORD")
        self.port = os.getenv("CLICKHOUSE_PORT")

    def connect_to_clickhouse(self):

        # ClickHouse 서버 정보
        self.client = Client(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.dbname,  # 예: '127.0.0.1'  # 기본 TCP 포트
        )

    def run_sql(self, sql: str) -> Union[pd.DataFrame, None]:
        if self.client:
            try:
                result = self.client.execute(sql, with_column_types=True)
                # 결과와 컬럼 정보 분리
                rows, columns = result
                column_names = [col[0] for col in columns]

                # Create a pandas dataframe from the results
                df = pd.DataFrame(rows, columns=column_names)
                return df

            except Exception as e:
                raise e
