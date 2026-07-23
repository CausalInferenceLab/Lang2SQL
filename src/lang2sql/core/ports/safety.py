"""Safety port — the ★① pipeline that gates every SQL execution.

Airport-security model: SQL passes a *line* of layers; each returns pass /
block / needs-confirmation / rewrite. New checks (v1.5 AST validation, function
blocklist, metadata enrichment) are "one class + slot it in the line" with zero
``run_sql`` changes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Protocol, Sequence, runtime_checkable


class Verdict(str, Enum):
    PASS = "pass"
    BLOCK = "block"
    CONFIRM = "confirm"  # ask the user before proceeding
    REWRITE = "rewrite"  # layer rewrote the SQL (e.g. attach LIMIT)


@dataclass
class SafetyDecision:
    verdict: Verdict
    sql: str  # possibly rewritten
    reason: str = ""
    layer: str = ""  # which layer decided
    confirm_prompt: str = ""  # populated when verdict is CONFIRM


@dataclass
class SafetyContext:
    """Knobs a layer reads (timeout, row cap). Grows over versions."""

    timeout_seconds: float = 30.0
    row_limit: int = 1000
    extras: dict = field(default_factory=dict)


@runtime_checkable
class SafetyLayerPort(Protocol):
    """One check in the line. Pure: SQL in, decision out."""

    @property
    def name(self) -> str: ...

    def check(self, sql: str, ctx: SafetyContext) -> SafetyDecision: ...


@runtime_checkable
class SafetyPipelinePort(Protocol):
    """Runs layers in order; first non-PASS short-circuits."""

    def evaluate(self, sql: str, ctx: SafetyContext) -> SafetyDecision: ...

    @property
    def layers(self) -> Sequence[SafetyLayerPort]: ...
