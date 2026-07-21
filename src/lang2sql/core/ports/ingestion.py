"""Ingestion ports — document → semantic candidates (★③).

A Source × Extractor matrix: one new Source automatically pairs with every
Extractor. V1 = file upload × LLM extraction; v1.5 adds URL source + DDL
parser, etc. Extracted candidates are shown for user confirm before landing in
the semantic layer (documents are the source of truth).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol, runtime_checkable


@dataclass
class Document:
    """Raw material pulled from some source."""

    name: str
    text: str
    source_id: str = ""  # preserved on resulting semantic entries


class CandidateKind(str, Enum):
    METRIC = "metric"
    DIMENSION = "dimension"
    RULE = "rule"


@dataclass
class SemanticCandidate:
    """A proposed definition awaiting user confirmation."""

    kind: CandidateKind
    name: str
    definition: str
    applies_to: str = ""
    source_id: str = ""


@runtime_checkable
class SourcePort(Protocol):
    """Where a document comes from (file/URL/Notion/…). Axis 1."""

    async def fetch(self, ref: str, blob: bytes | None = None) -> Document: ...


@runtime_checkable
class DocExtractorPort(Protocol):
    """How definitions are pulled from a document (LLM/DDL/…). Axis 2."""

    async def extract(self, doc: Document) -> list[SemanticCandidate]: ...
