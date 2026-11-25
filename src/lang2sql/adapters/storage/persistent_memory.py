"""Persist personal notes within an operator-bound organization and team."""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict

from ...core.ports.memory import Fact
from .sqlite_store import SqliteStore


class PersistentMemoryStore:
    def __init__(self, store: SqliteStore, org: str | None, team: str | None):
        self._store = store
        self._scope = "memory:" + json.dumps([org, team], ensure_ascii=False)

    async def add(self, fact: Fact) -> None:
        await asyncio.to_thread(
            self._store.kv_set,
            self._scope,
            fact.id,
            json.dumps(asdict(fact), ensure_ascii=False),
        )

    async def all(self, owner: str) -> list[Fact]:
        entries = await asyncio.to_thread(self._store.kv_list_prefix, self._scope, "")
        facts = [Fact(**json.loads(value)) for _, value in entries]
        return sorted(
            (fact for fact in facts if fact.owner == owner), key=lambda f: (f.ts, f.id)
        )
