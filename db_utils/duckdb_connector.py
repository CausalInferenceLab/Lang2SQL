import duckdb
import pandas as pd
from .base_connector import BaseConnector
from .config import DBConfig
from .logger import logger


class DuckDBConnector(BaseConnector):
    """
    Connect to DuckDB and execute SQL queries.
    """

    connection = None

    def __init__(self, config: DBConfig):
        """
        Initialize the DuckDBConnector with connection parameters.

        Parameters:
            config (DBConfig): Configuration object containing connection parameters.
                               Uses config['path'] as the file path or ':memory:'.
        """
        self.database = config.get("path", ":memory:")
        self.connect()

    def connect(self) -> None:
        """
        Establish a connection to the DuckDB database.
        """
        try:
            self.connection = duckdb.connect(database=self.database)
            logger.info("Successfully connected to DuckDB.")
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
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
            return self.connection.execute(sql).fetchdf()
        except Exception as e:
            logger.error(f"Failed to execute SQL query: {e}")
            raise

    def close(self) -> None:
        """
        Close the connection to the DuckDB database.
        """
        if self.connection:
            self.connection.close()
            logger.error("Connection to DuckDB closed.")
        self.connection = None
