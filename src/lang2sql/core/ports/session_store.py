"""Session store port — persist/restore a conversation by session key.

This is what makes the bot *not* stateless (tiebreaker #4, context
preservation). V1 persists to SQLite keyed by :meth:`Identity.session_key`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from ...harness.session import Session


@runtime_checkable
class SessionStorePort(Protocol):
    async def load(self, key: str) -> "Session | None":
        """Restore a saved session, or ``None`` for a fresh conversation."""
        ...

    async def save(self, key: str, session: "Session") -> None: ...
