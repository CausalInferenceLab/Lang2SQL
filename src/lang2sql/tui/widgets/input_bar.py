"""Enhanced Input widget with command history."""

from __future__ import annotations

try:
    from textual.widgets import Input

    _HAS_TEXTUAL = True
except ImportError:
    _HAS_TEXTUAL = False

if _HAS_TEXTUAL:

    class InputBar(Input):
        """Input bar with command history (up/down arrows)."""

        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)
            self._history: list[str] = []
            self._history_index: int = -1

        def on_input_submitted(self, event: Input.Submitted) -> None:
            if event.value.strip():
                self._history.append(event.value)
                self._history_index = -1

        def on_key(self, event) -> None:
            if event.key == "up" and self._history:
                if self._history_index == -1:
                    self._history_index = len(self._history) - 1
                elif self._history_index > 0:
                    self._history_index -= 1
                self.value = self._history[self._history_index]
                event.prevent_default()
            elif event.key == "down" and self._history:
                if self._history_index < len(self._history) - 1:
                    self._history_index += 1
                    self.value = self._history[self._history_index]
                else:
                    self._history_index = -1
                    self.value = ""
                event.prevent_default()
