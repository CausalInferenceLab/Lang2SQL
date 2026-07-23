"""InMemoryStore — the V1 Store axis (★②).

Facts live in a process-local dict keyed by owner. No persistence: a restart
forgets everything. v1.5 swaps this for a SQLite-backed store implementing the
same :class:`StorePort`, with zero changes elsewhere.
"""

from __future__ import annotations

from ...core.ports.memory import Fact


class InMemoryStore:
    """``StorePort`` over a ``dict[owner, list[Fact]]``."""

    def __init__(self) -> None:
        self._facts: dict[str, list[Fact]] = {}

    async def add(self, fact: Fact) -> None:
        self._facts.setdefault(fact.owner, []).append(fact)

    async def all(self, owner: str) -> list[Fact]:
        return list(self._facts.get(owner, ()))
