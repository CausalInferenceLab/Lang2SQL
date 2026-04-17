"""CLI command: lang2sql tui — launch the terminal UI."""

from __future__ import annotations

import click


@click.command("tui")
@click.option("--db", "db_url", default="", help="Database URL (e.g., sqlite:///data.db)")
@click.option("--dialect", default="sqlite", help="SQL dialect")
@click.option("--llm", "llm_provider", default="anthropic", help="LLM provider (anthropic, openai)")
def tui_command(db_url: str, dialect: str, llm_provider: str) -> None:
    """Launch the interactive terminal UI (OpenCode-style).

    \b
    Examples:
        lang2sql tui
        lang2sql tui --db sqlite:///sales.db
        lang2sql tui --db postgresql://user:pw@host/db --dialect postgresql --llm openai
    """
    from lang2sql.tui.app import run_tui

    run_tui(db_url=db_url, db_dialect=dialect, llm_provider=llm_provider)
