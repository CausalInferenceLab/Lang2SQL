from abc import ABC, abstractmethod
import pandas as pd


class BaseConnector(ABC):
    """
    Abstract base class for database connectors.
    """

    @abstractmethod
    def connect(self):
        """
        Initialize the database connection.
        """
        pass

    @abstractmethod
    def run_sql(self, sql: str) -> pd.DataFrame:
        """
        Returns the result of the SQL query as a pandas DataFrame.

        Parameters:
            sql (str): SQL query string to be executed.

        Returns:
            pd.DataFrame: Result of the SQL query as a pandas DataFrame.
        """
        pass
