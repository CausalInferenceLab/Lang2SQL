"""CLI command: lang2sql agent — one-shot agentic query."""

from __future__ import annotations

import asyncio
import json

import click


@click.command("agent")
@click.argument("question")
@click.option("--db", "db_url", required=True, help="Database URL (e.g., sqlite:///data.db)")
@click.option("--dialect", default="sqlite", help="SQL dialect (sqlite, postgresql, mysql, bigquery, duckdb)")
@click.option("--semantic", "semantic_path", default=None, help="Path to semantic layer JSON file")
@click.option("--max-turns", default=15, help="Maximum agent loop turns")
@click.option("--json-output", is_flag=True, help="Output as JSON instead of human-readable text")
def agent_command(
    question: str,
    db_url: str,
    dialect: str,
    semantic_path: str | None,
    max_turns: int,
    json_output: bool,
) -> None:
    """Run an agentic query against your database.

    The agent will explore the schema, use the semantic layer,
    generate SQL, execute it, and present results — all automatically.
    It will ask clarifying questions if needed.

    \b
    Examples:
        lang2sql agent "지난달 매출" --db sqlite:///sales.db
        lang2sql agent "Top 10 customers" --db postgresql://user:pw@host/db --dialect postgresql
    """
    asyncio.run(_run_agent(question, db_url, dialect, semantic_path, max_turns, json_output))


async def _run_agent(
    question: str,
    db_url: str,
    dialect: str,
    semantic_path: str | None,
    max_turns: int,
    json_output: bool,
) -> None:
    from lang2sql.harness.builder import build_harness
    from lang2sql.harness.loop import agent_loop
    from lang2sql.harness.types import (
        AssistantEvent,
        DataEvent,
        ErrorEvent,
        ToolCallEvent,
        ToolResultEvent,
        UserPromptEvent,
        VizEvent,
    )

    try:
        env = build_harness(db_url=db_url, db_dialect=dialect, semantic_path=semantic_path)
    except Exception as exc:
        click.secho(f"Error initializing harness: {exc}", fg="red")
        raise SystemExit(1) from exc

    results: list[dict] = []

    gen = agent_loop(question, max_turns=max_turns, **env)
    try:
        event = await gen.__anext__()
        while True:
            reply = None

            if isinstance(event, ToolCallEvent):
                if not json_output:
                    click.secho(f"  🔧 {event.tool_call.name}({json.dumps(event.tool_call.arguments, ensure_ascii=False)[:80]})", fg="cyan", dim=True)

            elif isinstance(event, ToolResultEvent):
                if event.result.is_error and not json_output:
                    click.secho(f"  ❌ {event.result.content[:120]}", fg="red")

            elif isinstance(event, AssistantEvent):
                if json_output:
                    results.append({"type": "assistant", "content": event.content})
                else:
                    click.echo(f"\n{event.content}")

            elif isinstance(event, DataEvent):
                if json_output:
                    results.append({"type": "data", "sql": event.sql, "rows": event.rows[:20], "row_count": event.row_count})
                else:
                    click.secho(f"\n  SQL: {event.sql[:200]}", fg="green")
                    click.echo(f"  Rows: {event.row_count}")

            elif isinstance(event, UserPromptEvent):
                # In CLI mode, prompt the user
                reply = click.prompt(f"\n❓ {event.question}")

            elif isinstance(event, ErrorEvent):
                if json_output:
                    results.append({"type": "error", "error": event.error})
                else:
                    click.secho(f"\n❌ {event.error}", fg="red")

            event = await gen.asend(reply)

    except StopAsyncIteration:
        pass

    if json_output:
        click.echo(json.dumps(results, ensure_ascii=False, indent=2))
