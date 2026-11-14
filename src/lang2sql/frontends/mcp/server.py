"""One-request stdio MCP server; identity and credentials are fixed at startup."""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from uuid import uuid4

from ...core.ports.audit import AuditEvent
from ...core.types import ToolResult, ToolSpec
from ...harness.context import HarnessContext
from ...harness.system_prompt import build_system_prompt

logger = logging.getLogger(__name__)
ENV_KEYS = (
    "LANG2SQL_DB_URL",
    "LANG2SQL_DB_SCHEMA",
    "LANG2SQL_DATA_PATH",
    "LANG2SQL_USER_ID",
    "LANG2SQL_ORG_ID",
    "LANG2SQL_TEAM_ID",
)


@dataclass(frozen=True)
class McpSettings:
    db_url: str
    schema: str | None
    data_path: str
    user_id: str
    org_id: str
    team_id: str

    @classmethod
    def from_env(cls) -> McpSettings:
        required = (
            "LANG2SQL_DB_URL",
            "LANG2SQL_USER_ID",
            "LANG2SQL_ORG_ID",
            "LANG2SQL_TEAM_ID",
        )
        missing = [key for key in required if not os.environ.get(key)]
        if missing:
            raise ValueError("Missing MCP settings: " + ", ".join(missing))
        return cls(
            os.environ[required[0]],
            os.environ.get("LANG2SQL_DB_SCHEMA") or None,
            os.environ.get("LANG2SQL_DATA_PATH") or ":memory:",
            os.environ[required[1]],
            os.environ[required[2]],
            os.environ[required[3]],
        )


class ToolGateway:
    """Reuse the tool registry and safety gate without invoking an inner LLM loop."""

    def __init__(self, ctx: HarnessContext):
        self.ctx = ctx
        self.request_id = uuid4().hex
        self._lock = asyncio.Lock()
        self._context_ready = False
        self._stopped: ToolResult | None = None

    def specs(self) -> list[ToolSpec]:
        return [
            ToolSpec(
                "get_context",
                "Read the current identity, business definitions, and SQL dialect before querying.",
                {"type": "object", "properties": {}, "additionalProperties": False},
            ),
            *self.ctx.tools.specs(),
        ]

    async def call(self, name: str, args: dict) -> ToolResult:
        # Serialize the state check with the DB call: a concurrent request cannot
        # pass the latch after ask_user or an access denial has stopped the turn.
        async with self._lock:
            call_id = uuid4().hex
            logger.info("mcp call request=%s tool=%s", self.request_id, name)
            if self._stopped is not None:
                result = ToolResult(
                    call_id, self._stopped.content, is_error=True, stop_turn=True
                )
            elif name == "get_context":
                if args:
                    result = ToolResult(
                        call_id, "get_context takes no arguments", is_error=True
                    )
                else:
                    prompt = await build_system_prompt(self.ctx)
                    result = ToolResult(
                        call_id,
                        f"User: {self.ctx.identity.user_id}\nTeam: {self.ctx.identity.channel_id}\n{prompt}",
                    )
                    self._context_ready = True
            elif name == "run_sql" and not self._context_ready:
                result = ToolResult(
                    call_id, "Call get_context before run_sql.", is_error=True
                )
            else:
                result = await self.ctx.tools.dispatch(name, args, self.ctx, call_id)
            if result.stop_turn:
                self._stopped = result
            if self.ctx.audit is not None:
                await self.ctx.audit.record(
                    AuditEvent(
                        actor=self.ctx.identity.user_id,
                        action="mcp_tool",
                        scope=self.ctx.identity.session_key(),
                        detail={
                            "request_id": self.request_id,
                            "tool": name,
                            "arguments": args,
                            "content": result.content,
                            "is_error": result.is_error,
                            "stop_turn": result.stop_turn,
                        },
                    )
                )
            logger.info(
                "mcp result request=%s tool=%s error=%s stopped=%s",
                self.request_id,
                name,
                result.is_error,
                result.stop_turn,
            )
            return result


def create_server(gateway: ToolGateway):
    # Optional dependency: ordinary CLI/Discord imports do not require MCP.
    from mcp.server import Server
    from mcp.types import CallToolResult, TextContent, Tool, ToolAnnotations

    server = Server(
        "lang2sql",
        instructions="Call get_context first. Use only the current team's definitions. If stop_turn is true, present that message and end the request. Do not call more tools or change identity.",
    )

    @server.list_tools()
    async def list_tools():
        return [
            Tool(
                name=s.name,
                description=s.description,
                inputSchema=s.parameters,
                annotations=ToolAnnotations(
                    readOnlyHint=True, destructiveHint=False, openWorldHint=False
                ),
            )
            for s in gateway.specs()
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict):
        result = await gateway.call(name, arguments)
        return CallToolResult(
            content=[TextContent(type="text", text=result.content)],
            structuredContent={"text": result.content, "stop_turn": result.stop_turn},
            isError=result.is_error,
        )

    return server


async def serve(settings: McpSettings) -> None:
    from mcp.server.stdio import stdio_server
    from ...tenancy.mcp_runtime import build_gateway

    gateway = await build_gateway(settings)
    server = create_server(gateway)
    try:
        async with stdio_server() as (read, write):
            await server.run(read, write, server.create_initialization_options())
    finally:
        if gateway.ctx.store is not None:
            gateway.ctx.store.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    asyncio.run(serve(McpSettings.from_env()))


if __name__ == "__main__":
    main()
