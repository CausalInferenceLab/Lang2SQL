"""Persistence helpers for SemanticLayer."""

from __future__ import annotations

import json
from pathlib import Path

from .layer import SemanticLayer


def save_layer(layer: SemanticLayer, path: str) -> None:
    """Save semantic layer to JSON file."""
    Path(path).write_text(
        json.dumps(layer.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_layer(path: str) -> SemanticLayer:
    """Load semantic layer from JSON file."""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return SemanticLayer.from_dict(data)
