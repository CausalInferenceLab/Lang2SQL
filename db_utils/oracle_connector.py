import oracledb
import pandas as pd
from .base_connector import BaseConnector
from .config import DBConfig
from .logger import logger


class OracleConnector(BaseConnector):
    """
    Connect to Oracle database and execute SQL queries.
    """

    connection = None

    def __init__(self, config: DBConfig):
        """
        Initialize the OracleConnector with connection parameters.
        Parameters:
            config (DBConfig): Configuration object containing connection parameters.
        """
        self.host = config["host"]
        self.port = config["port"]
        self.user = config["user"]
        self.password = config["password"]
        self.service_name = config.get("extra").get("service_name", "orcl")
        self.connect()

    def connect(self) -> None:
        """
        Establish a connection to the Oracle server.
        """
        try:
            self.connection = oracledb.connect(
                user=self.user,
                password=self.password,
                dsn=f"{self.host}:{self.port}/{self.service_name}",
            )
            logger.info("Successfully connected to Oracle.")
        except Exception as e:
            logger.error(f"Failed to connect to Oracle: {e}")
            raise

    def run_sql(self, sql: str) -> pd.DataFrame:
        """
        Execute a SQL query and return the result as a pandas DataFrame.
        Parameters:
            sql (str): SQL query string to be executed.
        Returns:
            pd.DataFrame: Result of the SQL query as a pandas DataFrame.
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)
        except Exception as e:
            logger.error(f"Failed to execute SQL query: {e}")
            raise
        finally:
            cursor.close()

    def close(self) -> None:
        if self.connection:
            self.connection.close()
            logger.error("Connection to Oracle closed.")
        self.connection = None
