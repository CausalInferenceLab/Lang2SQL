"""Frontend port — the Phase boundary (★ multi-interface).

Discord (Phase 1), Slack (Phase 2), Web (Phase 3) and the dev CLI all implement
this. The harness never imports a frontend; frontends drive the harness. Adding
Slack later is "implement this Protocol" with zero core changes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from ..identity import Identity


@dataclass
class InboundMessage:
    """Normalised user input from any frontend."""

    identity: Identity
    text: str
    attachments: list[bytes] = field(default_factory=list)
    attachment_names: list[str] = field(default_factory=list)


@dataclass
class OutboundMessage:
    """Normalised agent output a frontend renders natively."""

    text: str
    file_bytes: bytes | None = None  # e.g. CSV when result > 50 rows
    file_name: str | None = None


@runtime_checkable
class FrontendPort(Protocol):
    """Translate between a chat platform and the harness."""

    async def receive(self) -> InboundMessage:
        """Block until the next user message arrives."""
        ...

    async def send(self, identity: Identity, message: OutboundMessage) -> None:
        """Deliver agent output back to the originating conversation."""
        ...
