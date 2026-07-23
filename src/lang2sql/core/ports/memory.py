"""Memory ports — Hermes memory split into 3 independent axes (★②).

Store (where), Recall (what to fetch), Extractor (how new facts are made) each
evolve on their own. V1 = in-memory dict + inject-all + manual ``/remember``;
v1.5 swaps Store→SQLite or Recall→keyword as a single adapter add.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Sequence, runtime_checkable

from ..types import Message


@dataclass
class Fact:
    """A remembered statement, scoped to a user/conversation."""

    id: str
    owner: str  # user_id or scope key
    text: str
    source: str = "manual"  # "manual" (/remember) | "auto" (v1.5 extractor)
    ts: float = 0.0


@runtime_checkable
class StorePort(Protocol):
    """Where facts live. Axis 1."""

    async def add(self, fact: Fact) -> None: ...

    async def all(self, owner: str) -> list[Fact]: ...


@runtime_checkable
class RecallPort(Protocol):
    """Which facts to surface for the current question. Axis 2.

    V1 returns everything; v1.5 filters by keyword, v2 by vector similarity.
    """

    async def recall(self, owner: str, query: str, store: StorePort) -> list[Fact]: ...


@runtime_checkable
class ExtractorPort(Protocol):
    """How new facts get created. Axis 3.

    V1 only the explicit ``/remember`` path produces facts (so ``extract``
    yields nothing); v1.5 mines the transcript with an LLM.
    """

    async def extract(
        self, owner: str, transcript: Sequence[Message]
    ) -> list[Fact]: ...
