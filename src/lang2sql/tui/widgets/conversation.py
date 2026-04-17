"""RichLog-based conversation display widget."""

from __future__ import annotations

try:
    from textual.widgets import RichLog

    _HAS_TEXTUAL = True
except ImportError:
    _HAS_TEXTUAL = False

if _HAS_TEXTUAL:

    class ConversationLog(RichLog):
        """Displays the agent conversation with rich formatting."""

        def add_user_message(self, text: str) -> None:
            self.write(f"\n[bold cyan]User:[/] {text}")

        def add_assistant_message(self, text: str) -> None:
            self.write(f"\n[bold green]Agent:[/] {text}")

        def add_tool_call(self, name: str, args_preview: str) -> None:
            self.write(f"  [dim]🔧 {name}[/][dim]({args_preview[:60]})[/]")

        def add_tool_result(self, content: str, is_error: bool = False) -> None:
            if is_error:
                self.write(f"  [red]❌ {content[:120]}[/]")

        def add_sql(self, sql: str) -> None:
            self.write(f"\n[bold]SQL:[/]\n{sql}")

        def add_chart(self, chart_text: str) -> None:
            self.write(f"\n{chart_text}")

        def add_plan(self, plan: str) -> None:
            self.write(f"\n[bold yellow]Plan:[/]\n{plan}")

        def add_question(self, question: str) -> None:
            self.write(f"\n[bold yellow]? {question}[/]")

        def add_error(self, error: str) -> None:
            self.write(f"\n[bold red]Error:[/] {error}")

        def add_thinking(self) -> None:
            self.write("[dim]thinking...[/]")

        def add_data_summary(self, row_count: int, truncated: bool) -> None:
            msg = f"  [dim]{row_count} rows returned"
            if truncated:
                msg += " (truncated)"
            msg += "[/]"
            self.write(msg)
