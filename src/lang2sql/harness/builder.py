"""Convenience builder to wire up the full harness from env / config."""

from __future__ import annotations

from typing import Any

from ..factory import build_explorer_from_url, build_llm_from_env, build_tool_call_llm_from_env
from ..semantic.layer import SemanticLayer
from ..semantic.sql_composer import SQLComposer
from ..semantic.store import load_layer
from ..tools import build_default_tools
from .session import Session
from .tool import ToolRegistry


def build_harness(
    *,
    db_url: str,
    db_dialect: str = "sqlite",
    semantic_path: str | None = None,
    db_schema: str | None = None,
) -> dict[str, Any]:
    """Build a complete harness environment from a DB URL.

    Returns a dict with keys: ``llm``, ``tools``, ``session``, ``registry``.

    Usage::

        env = build_harness(db_url="sqlite:///sample.db")
        async for event in agent_loop("question", **env):
            ...
    """
    # LLM with tool-calling
    llm = build_tool_call_llm_from_env()

    # Plain LLM for explain_query
    plain_llm = build_llm_from_env()

    # DB explorer
    explorer = build_explorer_from_url(db_url, schema=db_schema)

    # Semantic layer
    layer = SemanticLayer()
    if semantic_path:
        try:
            layer = load_layer(semantic_path)
        except FileNotFoundError:
            pass

    # SQL composer
    composer = SQLComposer(layer, dialect=db_dialect)

    # Build tools
    tools_list = build_default_tools(
        explorer=explorer,
        layer=layer,
        composer=composer,
        llm=plain_llm,
    )

    # Registry
    registry = ToolRegistry()
    for tool in tools_list:
        registry.register(tool)

    # Session
    session = Session(
        db_url=db_url,
        db_dialect=db_dialect,
        semantic_layer=layer.to_dict() if not layer.is_empty() else {},
    )

    return {
        "llm": llm,
        "tools": registry,
        "session": session,
    }
