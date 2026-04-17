"""Bottom status bar widget."""

from __future__ import annotations

try:
    from textual.widgets import Static

    _HAS_TEXTUAL = True
except ImportError:
    _HAS_TEXTUAL = False

if _HAS_TEXTUAL:

    class StatusBar(Static):
        """Bottom status bar showing connection info and mode."""

        def set_status(
            self,
            db_dialect: str = "none",
            llm: str = "none",
            mode: str = "query",
        ) -> None:
            self.update(f" db:{db_dialect} | llm:{llm} | {mode} mode")
