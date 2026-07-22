"""Discord frontend (Phase 1) — the V1 chat interface over the harness.

``bot.py`` is the only module that imports discord.py; ``session_router``,
``render``, and ``commands`` are pure and unit-testable. Importing this package
does not import discord.py — ``bot`` is loaded lazily on demand — so the pure
layer (and its tests) stay free of the gateway dependency.
"""

from __future__ import annotations

from .commands import CommandHandlers
from .render import MAX_INLINE_ROWS, render_answer, sanitize_discord_text
from .session_router import (
    InteractionContext,
    is_channel,
    is_dm,
    is_thread,
    to_identity,
)

__all__ = [
    "CommandHandlers",
    "render_answer",
    "MAX_INLINE_ROWS",
    "sanitize_discord_text",
    "InteractionContext",
    "to_identity",
    "is_dm",
    "is_thread",
    "is_channel",
]
