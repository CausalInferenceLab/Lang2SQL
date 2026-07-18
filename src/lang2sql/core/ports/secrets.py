"""Secrets port — per-scope encrypted credential storage.

Holds DB connection strings / API keys a guild registers via ``/connect``. V1
backs it with an encrypted SQLite store; the boundary lets a KMS-backed impl
drop in later without touching callers.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class SecretsPort(Protocol):
    async def get(self, scope: str, key: str) -> str | None:
        """Decrypt and return one secret, or ``None`` if unset."""
        ...

    async def set(self, scope: str, key: str, value: str) -> None:
        """Encrypt and persist one secret under ``scope``."""
        ...

    async def delete(self, scope: str, key: str) -> None: ...
