from databricks import sql
import pandas as pd
from .base_connector import BaseConnector
from .config import DBConfig
from .logger import logger


class DatabricksConnector(BaseConnector):
    """
    Connect to Databricks SQL Warehouse and execute queries.
    """

    connection = None

    def __init__(self, config: DBConfig):
        """
        Initialize the DatabricksConnector with connection parameters.

        Parameters:
            config (DBConfig): Configuration object containing connection parameters.
                                 Required keys: host, extra.http_path, extra.access_token
                                 Optional keys: extra.catalog, extra.schema
        """
        self.server_hostname = config["host"]
        self.http_path = config["extra"]["http_path"]
        self.access_token = config["extra"]["access_token"]
        self.catalog = config.get("extra", {}).get("catalog")
        self.schema = config.get("extra", {}).get("schema")
        self.connect()

    def connect(self) -> None:
        """
        Establish a connection to the Databricks SQL endpoint.
        """
        try:
            self.connection = sql.connect(
                server_hostname=self.server_hostname,
                http_path=self.http_path,
                access_token=self.access_token,
                catalog=self.catalog,
                schema=self.schema,
            )
            logger.info("Successfully connected to Databricks.")
        except Exception as e:
            logger.error(f"Failed to connect to Databricks: {e}")
            raise

    def run_sql(self, sql: str) -> pd.DataFrame:
        """
        Execute a SQL query and return result as pandas DataFrame.

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
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)
        except Exception as e:
            logger.error(f"Failed to execute SQL query: {e}")
            raise
        finally:
            cursor.close()

    def close(self) -> None:
        """
        Close the Databricks connection.
        """
        if self.connection:
            self.connection.close()
            logger.error("Connection to Databricks closed.")
        self.connection = None
