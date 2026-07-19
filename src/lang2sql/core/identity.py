"""Who is asking, and in which semantic scope.

Federation (★④) resolves a metric definition by walking *most specific →
least specific* scope. :class:`Identity` is the frontend-agnostic carrier of
that scope chain: Discord fills it from guild/channel/thread IDs, the CLI from
flags, a future Slack adapter from workspace/channel — but the harness only
ever sees this shape.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ScopeLevel(str, Enum):
    """Federation scope levels, ordered narrow → wide.

    ``THREAD`` overrides ``CHANNEL`` overrides ``GUILD``; ``BUILTIN`` is the
    empty system default that every guild inherits from.
    """

    THREAD = "thread"
    CHANNEL = "team"
    GUILD = "org"
    BUILTIN = "builtin"


@dataclass(frozen=True)
class Scope:
    """A single addressable point in the federation tree."""

    level: ScopeLevel
    key: str  # e.g. guild id, channel id, thread id; "" for BUILTIN

    def __str__(self) -> str:
        return f"{self.level.value}:{self.key}" if self.key else self.level.value


@dataclass(frozen=True)
class Identity:
    """Frontend-agnostic identity + scope coordinates for one request.

    ``guild_id``/``channel_id``/``thread_id`` are optional so the same type
    serves a DM (guild only), a channel mention, or a thread reply.
    """

    user_id: str
    guild_id: str | None = None
    channel_id: str | None = None
    thread_id: str | None = None
    is_admin: bool = False

    def session_key(self) -> str:
        """Stable key for persisting/looking up this conversation's session.

        The thread (or channel, or DM) is the unit of conversation, so the key
        is the narrowest container that exists.
        """
        if self.thread_id:
            return f"thread:{self.thread_id}"
        if self.channel_id:
            return f"channel:{self.channel_id}"
        if self.guild_id:
            return f"guild:{self.guild_id}:{self.user_id}"
        return f"dm:{self.user_id}"

    def scope_chain(self) -> list[Scope]:
        """Scopes from most specific to least, for federation resolution.

        ``define_metric`` writes to ``scope_chain()[0]`` by default; lookup
        reads down the chain and stops at the first definition found.
        """
        chain: list[Scope] = []
        if self.thread_id:
            chain.append(Scope(ScopeLevel.THREAD, self.thread_id))
        if self.channel_id:
            chain.append(Scope(ScopeLevel.CHANNEL, self.channel_id))
        if self.guild_id:
            chain.append(Scope(ScopeLevel.GUILD, self.guild_id))
        chain.append(Scope(ScopeLevel.BUILTIN, ""))
        return chain

    @property
    def kv_scope(self) -> str:
        """KV store namespace key for this identity's guild (or DM fallback)."""
        return self.guild_id or f"dm:{self.user_id}"

    @property
    def effective_channel_id(self) -> str:
        """Channel entity for KV writes/lookups — always the parent channel, never thread_id.

        Threads inherit channel-layer terms from their parent channel.
        Thread-specific scoping would require a separate 'thread' layer (future).
        """
        return self.channel_id or ""

    def default_write_scope(self) -> Scope:
        """Where a new definition lands when the user gives no ``--scope``.

        Per v4.1 §3.5 the default is the current channel; a DM falls back to a
        per-user pseudo-scope so personal definitions don't leak.
        """
        if self.channel_id:
            return Scope(ScopeLevel.CHANNEL, self.channel_id)
        if self.guild_id:
            return Scope(ScopeLevel.GUILD, self.guild_id)
        return Scope(ScopeLevel.CHANNEL, f"dm:{self.user_id}")
