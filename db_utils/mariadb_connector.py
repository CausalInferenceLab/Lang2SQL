import mysql.connector
import pandas as pd
from .base_connector import BaseConnector
from .config import DBConfig
from .logger import logger


class MariaDBConnector(BaseConnector):
    """
    Connect to MariaDB and execute SQL queries.
    This class uses **mysql-connector-python** to connect to the MariaDB server.
    """

    connection = None

    def __init__(self, config: DBConfig):
        """
        Initialize the MariaDBConnector with connection parameters.

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
        Establish a connection to the MariaDB server using mysql-connector-python.
        """
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
            )
            logger.info("Successfully connected to MariaDB.")
        except Exception as e:
            logger.error(f"Failed to connect to MariaDB: {e}")
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
            print(f"Failed to execute SQL query: {e}")
            raise
        finally:
            cursor.close()

    def close(self) -> None:
        """
        Close the connection to the MariaDB server.
        """
        if self.connection:
            self.connection.close()
            print("Connection to MariaDB closed.")
        self.connection = None
