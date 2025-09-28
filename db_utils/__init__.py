from typing import Optional
import os
from .config import DBConfig
from .logger import logger

from .base_connector import BaseConnector

from .clickhouse_connector import ClickHouseConnector
from .postgres_connector import PostgresConnector
from .mysql_connector import MySQLConnector
from .mariadb_connector import MariaDBConnector
from .oracle_connector import OracleConnector
from .duckdb_connector import DuckDBConnector
from .databricks_connector import DatabricksConnector
from .snowflake_connector import SnowflakeConnector

env_path = os.path.join(os.getcwd(), ".env")


def get_db_connector(db_type: Optional[str] = None, config: Optional[DBConfig] = None):
    """
    Return the appropriate DB connector instance.
    - If db_type is not provided, loads from environment variable DB_TYPE
    - If config is not provided, loads from environment using db_type

    Parameters:
        db_type (Optional[str]): Database type (e.g., 'postgresql', 'mysql')
        config (Optional[DBConfig]): Connection config

    Returns:
        BaseConnector: Initialized DB connector instance

    Raises:
        ValueError: If type/config is missing or invalid
    """
    if db_type is None:
        db_type = os.getenv("DB_TYPE")
        if not db_type:
            raise ValueError(
                "DB type must be provided or set in environment as DB_TYPE."
            )

    db_type = db_type.lower()

    if config is None:
        config = load_config_from_env(db_type.upper())

        connector_map = {
            "clickhouse": ClickHouseConnector,
            "postgresql": PostgresConnector,
            "mysql": MySQLConnector,
            "mariadb": MariaDBConnector,
            "oracle": OracleConnector,
            "duckdb": DuckDBConnector,
            "databricks": DatabricksConnector,
            "snowflake": SnowflakeConnector,
        }

    if db_type not in connector_map:
        logger.error(f"Unsupported DB type: {db_type}")
        raise ValueError(f"Unsupported DB type: {db_type}")

    required_fields = {
        "oracle": ["extra.service_name"],
        "databricks": ["extra.http_path", "extra.access_token"],
        "snowflake": ["extra.account"],
    }

    missing = []
    for path in required_fields.get(db_type, []):
        cur = config
        for key in path.split("."):
            cur = cur.get(key) if isinstance(cur, dict) else None
            if cur is None:
                missing.append(path)
                break

    if missing:
        logger.error(f"Missing required fields for {db_type}: {', '.join(missing)}")
        raise ValueError(f"Missing required fields for {db_type}: {', '.join(missing)}")

    return connector_map[db_type](config)


def load_config_from_env(prefix: str) -> DBConfig:
    """
    Load DBConfig from environment variables with a given prefix.
    Standard keys are extracted, all other prefixed keys go to 'extra'.

    Example:
        If prefix = 'SNOWFLAKE', loads:
        - SNOWFLAKE_HOST
        - SNOWFLAKE_USER
        - SNOWFLAKE_PASSWORD
        - SNOWFLAKE_PORT
        - SNOWFLAKE_DATABASE
        Other keys like SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE -> extra
    """
    base_keys = {"HOST", "PORT", "USER", "PASSWORD", "DATABASE"}

    # Extract standard values
    config = {
        "host": os.getenv(f"{prefix}_HOST"),
        "port": (
            int(os.getenv(f"{prefix}_PORT")) if os.getenv(f"{prefix}_PORT") else None
        ),
        "user": os.getenv(f"{prefix}_USER"),
        "password": os.getenv(f"{prefix}_PASSWORD"),
        "database": os.getenv(f"{prefix}_DATABASE"),
    }

    # Auto-detect extra keys
    extra = {}
    for key, value in os.environ.items():
        if key.startswith(f"{prefix}_"):
            suffix = key[len(f"{prefix}_") :]
            if suffix.upper() not in base_keys:
                extra[suffix.lower()] = value

    if extra:
        config["extra"] = extra

    return DBConfig(**config)
