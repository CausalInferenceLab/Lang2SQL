from .base_connector import BaseConnector
from clickhouse_driver import Client
import pandas as pd
from db_utils import DBConfig, logger


class ClickHouseConnector(BaseConnector):
    """
    Connect to ClickHouse and execute SQL queries.
    """

    client = None

    def __init__(self, config: DBConfig):
        """
        Initialize the ClickHouseConnector with connection parameters.

        Parameters:
            config (DBConfig): Configuration object containing connection parameters.
        """
        self.host = config["host"]
        self.port = config["port"]
        self.user = config["user"]
        self.password = config["password"]
        self.database = config["database"]
        self.connect()

    def connect(self) -> None:
        """
        Establish a connection to the ClickHouse server.
        """
        try:
            self.client = Client(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
            )
            logger.info("Successfully connected to ClickHouse.")
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise

    def run_sql(self, sql: str) -> pd.DataFrame:
        """
        Execute a SQL query and return the result as a pandas DataFrame.

        Parameters:
            sql (str): SQL query string to be executed.

        Returns:
            pd.DataFrame: Result of the SQL query as a pandas DataFrame.
        """
        if self.client is None:
            self.connect()

        try:
            result = self.client.query_dataframe(sql)
            return result
        except Exception as e:
            logger.error(f"Failed to execute SQL query: {e}")
            raise

    def close(self) -> None:
        """
        Close the connection to the ClickHouse server.
        """
        if self.client:
            self.client.disconnect()
            logger.error("Connection to ClickHouse closed.")
        self.client = None
