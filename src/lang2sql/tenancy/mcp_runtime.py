"""Composition root for the Codex/MCP path. No inner LLM or canned DB fallback."""

from __future__ import annotations

from ..adapters.db.factory import build_explorer
from ..adapters.llm.fake import FakeLLM
from ..adapters.storage.sqlite_store import SqliteStore
from ..core.identity import Identity
from ..frontends.mcp.server import McpSettings, ToolGateway
from ..harness.context import HarnessContext
from ..harness.session import Session
from ..harness.tool_registry import ToolRegistry
from ..safety.pipeline import SafetyPipeline
from ..tools import AskUser, ExploreSchema, RunSQL


async def build_gateway(settings: McpSettings) -> ToolGateway:
    identity = Identity(
        user_id=settings.user_id, guild_id=settings.org_id, channel_id=settings.team_id
    )
    store = SqliteStore(settings.data_path)
    ctx = HarnessContext(
        identity=identity,
        llm=FakeLLM(),
        session=Session(identity),
        tools=ToolRegistry([ExploreSchema(), RunSQL(), AskUser()]),
        explorer=build_explorer(settings.db_url, schema=settings.schema),
        safety=SafetyPipeline(),
        audit=store,
        store=store,
    )
    # The LLM field satisfies the legacy context shape only; MCP never calls it.
    return ToolGateway(ctx)
