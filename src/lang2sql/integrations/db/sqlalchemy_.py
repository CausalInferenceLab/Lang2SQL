from __future__ import annotations

from typing import Any

from ...core.exceptions import IntegrationMissingError

try:
    from sqlalchemy import create_engine, text as sa_text
    from sqlalchemy.engine import Engine
except ImportError:
    create_engine = None  # type: ignore[assignment]
    sa_text = None  # type: ignore[assignment]
    Engine = None  # type: ignore[assignment,misc]


class SQLAlchemyDB:
    """DBPort implementation backed by SQLAlchemy 2.x."""

    def __init__(self, url: str) -> None:
        if create_engine is None:
            raise IntegrationMissingError(
                "sqlalchemy", extra="sqlalchemy", hint="pip install sqlalchemy"
            )
        self._engine: Engine = create_engine(url)

    def execute(self, sql: str) -> list[dict[str, Any]]:
        with self._engine.connect() as conn:
            result = conn.execute(sa_text(sql))
            return [dict(row._mapping) for row in result]
