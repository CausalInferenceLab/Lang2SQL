"""Main Textual TUI application for lang2sql — OpenCode-style interface."""

from __future__ import annotations

import json
from typing import Any

try:
    from textual.app import App, ComposeResult
    from textual.containers import Horizontal, Vertical
    from textual.widgets import Footer, Header, Static
    from textual.binding import Binding

    _HAS_TEXTUAL = True
except ImportError:
    _HAS_TEXTUAL = False

from ..core.exceptions import IntegrationMissingError


def _check_textual() -> None:
    if not _HAS_TEXTUAL:
        raise IntegrationMissingError(
            "textual",
            hint="pip install 'lang2sql[tui]'  # or: pip install textual",
        )


if _HAS_TEXTUAL:
    from .widgets import (
        ConversationLog,
        InputBar,
        PlanView,
        SemanticBrowser,
        StatusBar,
        render_chart_text,
    )
    from ..harness.builder import build_harness
    from ..harness.loop import agent_loop
    from ..harness.types import (
        AssistantEvent,
        DataEvent,
        ErrorEvent,
        PlanApprovalEvent,
        ToolCallEvent,
        ToolResultEvent,
        UserPromptEvent,
        VizEvent,
    )
    from ..viz.rich_table import render_table

    class Lang2SQLTUI(App):
        """Interactive data agent terminal interface."""

        TITLE = "lang2sql v3"
        CSS = """
        #sidebar {
            width: 30;
            dock: left;
            background: $surface;
            border-right: solid $primary;
            overflow-y: auto;
        }
        #conversation {
            height: 1fr;
            overflow-y: auto;
            padding: 0 1;
        }
        #input-bar {
            dock: bottom;
            height: 3;
            padding: 0 1;
        }
        #status {
            dock: bottom;
            height: 1;
            background: $primary;
            color: $text;
            padding: 0 1;
        }
        #plan-view {
            display: none;
            height: auto;
            max-height: 15;
            padding: 1;
            background: $warning 10%;
            border: solid $warning;
        }
        .title-label {
            text-style: bold;
            padding: 0 1;
            color: $accent;
        }
        """

        BINDINGS = [
            Binding("ctrl+q", "quit", "Quit"),
            Binding("ctrl+l", "clear_log", "Clear"),
            Binding("escape", "focus_input", "Focus input"),
        ]

        def __init__(
            self,
            db_url: str = "",
            db_dialect: str = "sqlite",
            llm_provider: str = "anthropic",
            semantic_path: str | None = None,
            **kwargs,
        ):
            super().__init__(**kwargs)
            self._db_url = db_url
            self._db_dialect = db_dialect
            self._llm_provider = llm_provider
            self._semantic_path = semantic_path
            # Harness components (initialized on setup)
            self._session = None
            self._registry = None
            self._llm = None
            # Agent loop state
            self._pending_gen = None
            self._waiting_for: str | None = None
            self._last_tool_name: str = ""
            self._setup_mode: bool = False

        # ── Layout ────────────────────────────────────────────────────

        def compose(self) -> ComposeResult:
            yield Header()

            with Horizontal():
                # Left sidebar: schema + semantic browser
                with Vertical(id="sidebar"):
                    yield Static("Schema Browser", classes="title-label")
                    yield SemanticBrowser("Schema", id="schema-tree")

                # Right: conversation + plan + status + input
                with Vertical():
                    yield ConversationLog(
                        id="conversation", highlight=True, markup=True,
                    )
                    yield PlanView(id="plan-view")
                    yield StatusBar(id="status")
                    yield InputBar(
                        placeholder=(
                            "Ask about your data... "
                            "(/setup, /schema, /export, /clear)"
                        ),
                        id="input-bar",
                    )

            yield Footer()

        # ── Lifecycle ─────────────────────────────────────────────────

        async def on_mount(self) -> None:
            """Initialize harness components if db_url provided."""
            conv = self.query_one(ConversationLog)
            conv.write("[bold green]lang2sql v3[/] — Interactive Data Agent")
            conv.write("")

            if self._db_url:
                await self._init_harness(self._db_url)
            else:
                conv.write("[yellow]No database connected.[/]")
                conv.write(
                    "[dim]/setup <db_url> to connect "
                    "(e.g. /setup sqlite:///data.db)[/]"
                )

            self.query_one(InputBar).focus()
            self._update_status()

        # ── Harness setup ─────────────────────────────────────────────

        async def _init_harness(self, db_url: str) -> None:
            """Build harness from a database URL."""
            conv = self.query_one(ConversationLog)
            conv.write(f"[dim]Connecting to {db_url}...[/]")

            try:
                env = build_harness(
                    db_url=db_url,
                    db_dialect=self._db_dialect,
                    semantic_path=self._semantic_path,
                )
                self._llm = env["llm"]
                self._registry = env["tools"]
                self._session = env["session"]
                self._db_url = db_url

                conv.write(f"[green]Connected ({self._db_dialect})[/]")
                conv.write(f"[dim]LLM: {self._llm_provider}[/]")
                conv.write("")
                conv.write(
                    "Ready. Ask a question or use /schema to browse tables."
                )

                # Populate sidebar
                browser = self.query_one(SemanticBrowser)
                browser.populate(
                    self._session.semantic_layer,
                    self._session.schema_cache,
                )
                self._update_status()

            except Exception as exc:
                conv.add_error(f"Connection failed: {exc}")

        def _update_status(self) -> None:
            status = self.query_one(StatusBar)
            mode = "query" if self._session else "setup"
            db = self._db_dialect if self._db_url else "none"
            status.set_status(db_dialect=db, llm=self._llm_provider, mode=mode)

        # ── Input handling ────────────────────────────────────────────

        async def on_input_submitted(self, event) -> None:
            """Handle user input — route to setup, pending, command, or agent."""
            query = event.value.strip()
            if not query:
                return

            input_bar = self.query_one(InputBar)
            input_bar.value = ""
            conv = self.query_one(ConversationLog)

            # Setup mode: treat input as DB URL
            if self._setup_mode:
                self._setup_mode = False
                input_bar.placeholder = "Ask about your data..."
                await self._init_harness(query)
                return

            # Resume pending interactive event
            if self._pending_gen is not None:
                await self._resume_pending(query)
                return

            # Slash commands
            if query.startswith("/"):
                await self._handle_command(query, conv)
                return

            # Regular query → agent loop
            if self._session is None:
                conv.write(
                    "[yellow]Not connected. Use /setup first.[/]"
                )
                return

            conv.add_user_message(query)
            conv.add_thinking()

            gen = agent_loop(
                query,
                llm=self._llm,
                tools=self._registry,
                session=self._session,
            )
            await self._consume_events(gen)

        # ── Agent loop event consumption ──────────────────────────────

        async def _resume_pending(self, user_input: str) -> None:
            """Resume a paused agent loop with user's response."""
            gen = self._pending_gen
            self._pending_gen = None
            conv = self.query_one(ConversationLog)

            if self._waiting_for == "plan_approval":
                approved = user_input.lower().startswith("y")
                # bool() in agent_loop: non-empty string = True, "" = False
                reply: Any = "approved" if approved else ""
                conv.write(f"[dim]{'Approved' if approved else 'Rejected'}[/]")
                self.query_one(PlanView).hide()
            else:
                reply = user_input
                conv.add_user_message(user_input)

            self._waiting_for = None
            self.query_one(InputBar).placeholder = "Ask about your data..."
            await self._consume_events(gen, initial_send=reply)

        async def _consume_events(
            self,
            gen,
            initial_send=None,
        ) -> None:
            """Iterate the async agent generator, dispatching events to widgets.

            For interactive events (UserPromptEvent, PlanApprovalEvent) the
            generator is stored in ``_pending_gen`` and we return — the loop
            resumes when the user submits their response.
            """
            conv = self.query_one(ConversationLog)

            try:
                if initial_send is not None:
                    event = await gen.asend(initial_send)
                else:
                    event = await gen.__anext__()

                while True:
                    reply = None

                    if isinstance(event, ToolCallEvent):
                        tc = event.tool_call
                        args = json.dumps(
                            tc.arguments, ensure_ascii=False,
                        )
                        conv.add_tool_call(tc.name, args)
                        self._last_tool_name = tc.name

                    elif isinstance(event, ToolResultEvent):
                        r = event.result
                        conv.add_tool_result(r.content, r.is_error)
                        # Visualize tool returns a spec in data
                        if (
                            self._last_tool_name == "visualize"
                            and r.data
                        ):
                            self._render_viz_from_spec(r.data)

                    elif isinstance(event, AssistantEvent):
                        conv.add_assistant_message(event.content)

                    elif isinstance(event, DataEvent):
                        conv.add_sql(event.sql)
                        self._render_data(event)

                    elif isinstance(event, UserPromptEvent):
                        conv.add_question(event.question)
                        if event.options:
                            opts = " / ".join(event.options)
                            conv.write(f"  [dim]Options: {opts}[/]")
                        self._pending_gen = gen
                        self._waiting_for = "user_prompt"
                        bar = self.query_one(InputBar)
                        bar.placeholder = event.question
                        bar.focus()
                        return

                    elif isinstance(event, PlanApprovalEvent):
                        conv.add_plan(event.plan)
                        if event.steps:
                            for i, step in enumerate(event.steps, 1):
                                conv.write(f"  {i}. {step}")
                        self.query_one(PlanView).show_plan(event.plan)
                        self._pending_gen = gen
                        self._waiting_for = "plan_approval"
                        bar = self.query_one(InputBar)
                        bar.placeholder = "Y to approve / N to reject"
                        bar.focus()
                        return

                    elif isinstance(event, VizEvent):
                        text = render_chart_text(
                            event.data,
                            event.chart_type,
                            event.title,
                            event.columns.get("x", ""),
                            event.columns.get("y", ""),
                        )
                        conv.add_chart(text)

                    elif isinstance(event, ErrorEvent):
                        conv.add_error(event.error)

                    event = await gen.asend(reply)

            except StopAsyncIteration:
                pass
            except Exception as exc:
                conv.add_error(f"Agent error: {exc}")

            # Restore default placeholder
            bar = self.query_one(InputBar)
            bar.placeholder = "Ask about your data..."
            bar.focus()

        # ── Data & chart rendering ────────────────────────────────────

        def _render_data(self, event: DataEvent) -> None:
            """Render a DataEvent as a table in the conversation."""
            conv = self.query_one(ConversationLog)
            if event.rows:
                table_text = render_table(event.rows, max_rows=20)
                conv.write(table_text)
                conv.add_data_summary(event.row_count, event.truncated)

        def _render_viz_from_spec(self, spec: dict[str, Any]) -> None:
            """Render a chart from a visualize tool result spec."""
            conv = self.query_one(ConversationLog)
            data = self._session.last_result if self._session else []
            if not data:
                conv.write("[dim](no data to visualize)[/]")
                return
            text = render_chart_text(
                data,
                spec.get("chart_type", "auto"),
                spec.get("title", ""),
                spec.get("x_column", ""),
                spec.get("y_column", ""),
            )
            conv.add_chart(text)

        # ── Slash commands ────────────────────────────────────────────

        async def _handle_command(
            self,
            cmd: str,
            conv: ConversationLog,
        ) -> None:
            """Handle slash commands."""
            parts = cmd.split(maxsplit=1)
            command = parts[0].lower()

            if command == "/clear":
                conv.clear()
                conv.write("[dim]Conversation cleared.[/]")

            elif command == "/setup":
                if len(parts) > 1:
                    await self._init_harness(parts[1].strip())
                else:
                    conv.write("\n[bold yellow]Setup Mode[/]")
                    conv.write(
                        "[dim]Enter your DB URL "
                        "(e.g. sqlite:///data.db)[/]"
                    )
                    self._setup_mode = True
                    self.query_one(InputBar).placeholder = "DB URL..."

            elif command == "/schema":
                if not self._session:
                    conv.write("[yellow]Connect first with /setup.[/]")
                    return
                table = parts[1].strip() if len(parts) > 1 else ""
                if table and table in self._session.schema_cache:
                    conv.write(f"\n[bold]{table}:[/]")
                    conv.write(self._session.schema_cache[table])
                elif self._session.schema_cache:
                    conv.write("\n[bold]Tables:[/]")
                    for t in sorted(self._session.schema_cache):
                        conv.write(f"  {t}")
                else:
                    conv.write(
                        "[dim]Schema cache empty. "
                        "Ask a question to discover tables.[/]"
                    )

            elif command == "/export":
                if not self._session or not self._session.last_result:
                    conv.write("[yellow]No data to export.[/]")
                    return
                fmt = parts[1].strip() if len(parts) > 1 else "json"
                self._export_data(fmt, conv)

            else:
                conv.write(f"[red]Unknown command: {command}[/]")

        def _export_data(self, fmt: str, conv: ConversationLog) -> None:
            """Export last query result to a file."""
            data = self._session.last_result
            if fmt == "json":
                path = "export.json"
                with open(path, "w") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            elif fmt == "csv":
                import csv

                path = "export.csv"
                if not data:
                    conv.write("[dim]No data to export.[/]")
                    return
                with open(path, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=data[0].keys())
                    writer.writeheader()
                    writer.writerows(data)
            else:
                conv.write(
                    f"[red]Unsupported format: {fmt}. Use json or csv.[/]"
                )
                return
            conv.write(f"[green]Exported {len(data)} rows to {path}[/]")

        # ── Actions ───────────────────────────────────────────────────

        def action_clear_log(self) -> None:
            self.query_one(ConversationLog).clear()

        def action_focus_input(self) -> None:
            self.query_one(InputBar).focus()


def run_tui(
    db_url: str = "",
    db_dialect: str = "sqlite",
    llm_provider: str = "anthropic",
    semantic_path: str | None = None,
) -> None:
    """Launch the lang2sql terminal UI."""
    _check_textual()
    app = Lang2SQLTUI(
        db_url=db_url,
        db_dialect=db_dialect,
        llm_provider=llm_provider,
        semantic_path=semantic_path,
    )
    app.run()
