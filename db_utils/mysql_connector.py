import mysql.connector
import pandas as pd
from .base_connector import BaseConnector
from .config import DBConfig
from .logger import logger


class MySQLConnector(BaseConnector):
    """
    Connect to MySQL and execute SQL queries.
    """

    connection = None

    def __init__(self, config: DBConfig):
        """
        Initialize the MySQLConnector with connection parameters.

        Parameters:
            config (DBConfig): Configuration object containing connection parameters.
        """
        self.host = config["host"]
        self.port = config.get("port", 3306)
        self.user = config["user"]
        self.password = config["password"]
        self.database = config["database"]
        self.connect()

    def connect(self) -> None:
        """
        Establish a connection to the MySQL server.
        """
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
            )
            logger.info("Successfully connected to MySQL.")
        except Exception as e:
            logger.error(f"Failed to connect to MySQL: {e}")
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
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)
        except Exception as e:
            logger.error(f"Failed to execute SQL query: {e}")
            raise
        finally:
            cursor.close()

    def close(self) -> None:
        """
        Close the connection to the MySQL server.
        """
        if self.connection:
            self.connection.close()
            logger.info("Connection to MySQL closed.")
        self.connection = None
