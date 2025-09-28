from .base_connector import BaseConnector
from snowflake import connector
import pandas as pd
from .config import DBConfig
from .logger import logger


class SnowflakeConnector(BaseConnector):
    """
    Connect to Snowflake database and execute SQL queries.
    """

    connection = None

    def __init__(self, config: DBConfig):
        """
        Initialize the SnowflakeConnector with connection parameters.

        Parameters:
            config (DBConfig): Configuration object containing connection parameters.
                                 Required: user, password, extra.account
                                 Optional: extra.warehouse, database, extra.schema
        """
        self.user = config["user"]
        self.password = config["password"]
        self.account = config["extra"]["account"]
        self.warehouse = config.get("extra", {}).get("warehouse")
        self.database = config.get("database")
        self.schema = config.get("extra", {}).get("schema")
        self.connect()

    def connect(self) -> None:
        """
        Establish a connection to the Snowflake server.
        """
        try:
            self.connection = connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
            )
            logger.info("Successfully connected to Snowflake.")
            self.cursor = self.connection.cursor()
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
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

        cursor = self.connection.cursor()

        try:
            self.cursor.execute(sql)
            columns = [col[0] for col in self.cursor.description]
            data = self.cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
        except Exception as e:
            logger.error(f"Failed to execute SQL query: {e}")
            raise
        finally:
            cursor.close()

    def close(self) -> None:
        """
        Close the connection to the Snowflake server.
        """
        if self.connection:
            self.connection.close()
            logger.info("Connection to Snowflake closed.")
        self.connection = None
