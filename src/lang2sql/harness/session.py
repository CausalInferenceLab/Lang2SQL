"""Session — the persistable state of one conversation.

Holds the transcript plus a scratch of facts recalled for the current turn.
Persisted via :class:`SessionStorePort` keyed by ``Identity.session_key`` so a
thread picks up where it left off (tiebreaker #4).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ..core.identity import Identity
from ..core.types import Message


@dataclass
class Session:
    identity: Identity
    transcript: list[Message] = field(default_factory=list)

    def add(self, message: Message) -> None:
        self.transcript.append(message)

    def history(self) -> list[Message]:
        return list(self.transcript)

    def reset(self) -> None:
        self.transcript.clear()

    def compress(self) -> None:
        """Remove tool call/result messages to prevent context pollution across turns."""
        from ..core.types import Role

        cleaned: list[Message] = []
        for msg in self.transcript:
            if msg.role == Role.TOOL:
                continue
            if msg.role == Role.ASSISTANT and msg.tool_calls:
                if (
                    msg.content
                ):  # skip if no text content — empty assistant messages confuse OpenAI
                    cleaned.append(Message(role=Role.ASSISTANT, content=msg.content))
            else:
                cleaned.append(msg)
        self.transcript = cleaned
