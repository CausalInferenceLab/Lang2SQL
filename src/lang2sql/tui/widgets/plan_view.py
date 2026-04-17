"""Plan approval widget."""

from __future__ import annotations

try:
    from textual.widgets import Static

    _HAS_TEXTUAL = True
except ImportError:
    _HAS_TEXTUAL = False

if _HAS_TEXTUAL:

    class PlanView(Static):
        """Shows analysis plan and approval prompt.

        Used when the agent calls show_plan — displays the plan
        and waits for user approval via the input bar.
        """

        def show_plan(self, plan_text: str) -> None:
            self.update(
                f"[bold yellow]Plan:[/]\n{plan_text}\n\n"
                "[dim]Press Y to approve, N to reject[/]"
            )
            self.display = True

        def hide(self) -> None:
            self.display = False
            self.update("")
