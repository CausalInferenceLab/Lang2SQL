import sqlite3
import pandas as pd
from .base_connector import BaseConnector
from .config import DBConfig
from .logger import logger


class SQLiteConnector(BaseConnector):
    """
    Connect to SQLite and execute SQL queries.
    """

    connection = None

    def __init__(self, config: DBConfig):
        """
        Initialize the SQLiteConnector with connection parameters.

        Parameters:
            config (DBConfig): Configuration object containing connection parameters.
                               Uses config["database"] as the SQLite file path.
                               If None or ":memory:", creates an in-memory database.
        """
        self.database = config.get("path", ":memory:")
        self.connect()

    def connect(self) -> None:
        """
        Establish a connection to the SQLite database.
        """
        try:
            self.connection = sqlite3.connect(self.database)
            logger.info(f"Successfully connected to SQLite ({self.database}).")
        except Exception as e:
            logger.error(f"Failed to connect to SQLite: {e}")
            raise

    def run_sql(self, sql: str) -> pd.DataFrame:
        """
        Execute a SQL query and return the result as a pandas DataFrame.

        Parameters:
            sql (str): SQL query string to be executed.

        Returns:
            pd.DataFrame: Result of the SQL query as a pandas DataFrame.
        """
        if self.connection is None:
            self.connect()

        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)
        except Exception as e:
            logger.error(f"Failed to execute SQL query: {e}")
            raise
        finally:
            cursor.close()

    def close(self) -> None:
        """
        Close the connection to the SQLite database.
        """
        if self.connection:
            self.connection.close()
            logger.info("Connection to SQLite closed.")
        self.connection = None
