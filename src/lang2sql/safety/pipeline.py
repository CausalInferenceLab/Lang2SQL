"""Safety pipeline — runs layers in order, first non-PASS short-circuits.

Airport-security model: SQL walks a *line* of layers. The first layer that
does not return PASS decides the outcome and the rest are skipped. If every
layer PASSes, the pipeline returns a PASS carrying the (possibly rewritten)
SQL forward — so a layer that REWRITEs is itself a non-PASS short-circuit in
V1, while accumulated rewrites only matter once we have multiple rewriting
layers (V1.5+).
"""

from __future__ import annotations

from typing import Sequence

from ..core.ports.safety import (
    SafetyContext,
    SafetyDecision,
    SafetyLayerPort,
    Verdict,
)
from .layers import RowLimitLayer, TimeoutLayer, WhitelistLayer


def _default_layers(allow_write: bool = False) -> list[SafetyLayerPort]:
    # Whitelist (reject) → RowLimit (rewrite) → Timeout (exec config).
    return [WhitelistLayer(allow_write), RowLimitLayer(), TimeoutLayer()]


class SafetyPipeline:
    """Ordered safety layers. Implements ``SafetyPipelinePort``."""

    def __init__(
        self,
        layers: Sequence[SafetyLayerPort] | None = None,
        *,
        allow_write: bool = False,
    ) -> None:
        self._layers: list[SafetyLayerPort] = (
            list(layers) if layers is not None else _default_layers(allow_write)
        )

    @property
    def layers(self) -> Sequence[SafetyLayerPort]:
        return self._layers

    def evaluate(self, sql: str, ctx: SafetyContext) -> SafetyDecision:
        current = sql
        for layer in self._layers:
            decision = layer.check(current, ctx)
            if decision.verdict is not Verdict.PASS:
                return decision
            # Carry any rewritten SQL forward to the next layer.
            current = decision.sql
        return SafetyDecision(verdict=Verdict.PASS, sql=current, layer="pipeline")
