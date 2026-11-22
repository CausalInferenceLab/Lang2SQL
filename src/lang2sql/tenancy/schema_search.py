"""Configure optional schema retrieval against the actual bound explorer."""

import hashlib
import json
import os
from pathlib import Path

from sqlalchemy.engine import make_url

from ..adapters.db.d1_explorer import D1Explorer
from ..adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer
from ..adapters.storage.qdrant_schema import QdrantSchema
from ..core.ports.explorer import ExplorerPort
from ..harness.tool_registry import ToolRegistry
from ..tools.search_schema import SearchSchema


def schema_index(explorer: ExplorerPort | None) -> QdrantSchema | None:
    url = os.getenv("LANG2SQL_QDRANT_URL")
    if not url:
        return None
    if isinstance(explorer, SqlAlchemyExplorer):
        connection = make_url(explorer.url)
        if connection.get_backend_name() == "sqlite":
            if (
                connection.database in (None, "", ":memory:", "file::memory:")
                or connection.query.get("mode") == "memory"
            ):
                return None
            connection = connection.set(
                database=str(Path(connection.database).resolve())
            )
        source = [connection.render_as_string(hide_password=False), explorer.schema]
    elif isinstance(explorer, D1Explorer):
        source = ["d1", explorer.account_id, explorer.database_id]
    else:
        return None
    endpoint = os.environ.get("LANG2SQL_EMBEDDING_URL")
    model = os.environ.get("LANG2SQL_EMBEDDING_MODEL")
    if not endpoint or not model:
        raise ValueError(
            "Set LANG2SQL_EMBEDDING_URL and LANG2SQL_EMBEDDING_MODEL for schema search"
        )
    return QdrantSchema(
        url.rstrip("/"),
        endpoint,
        model,
        os.getenv("LANG2SQL_QDRANT_API_KEY", ""),
        os.getenv("LANG2SQL_EMBEDDING_API_KEY", ""),
        hashlib.sha256(json.dumps(source).encode()).hexdigest(),
    )


def register_schema_search(
    registry: ToolRegistry, explorer: ExplorerPort | None
) -> None:
    index = schema_index(explorer)
    if index:
        registry.register(SearchSchema(index))
