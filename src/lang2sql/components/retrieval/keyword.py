from __future__ import annotations

from typing import Optional

from ...core.base import BaseComponent
from ...core.catalog import CatalogEntry
from ...core.hooks import TraceHook
from ._bm25 import _BM25Index

_DEFAULT_INDEX_FIELDS = ["name", "description", "columns"]


class KeywordRetriever(BaseComponent):
    """
    BM25-based keyword retriever over a table catalog.

    Indexes catalog entries at init time (in-memory).
    On each call, scores entries against the given query and returns
    the top-N matches as a ranked list.

    Args:
        catalog:       List of table dicts. Each dict should have at minimum
                       ``name`` (str) and ``description`` (str).
                       Optional keys: ``columns`` (dict[str, str]), ``meta`` (dict).
        top_n:         Maximum number of results to return. Defaults to 5.
        index_fields:  Fields to index. Defaults to ["name", "description", "columns"].
                       Pass a custom list to replace the default (complete override).
        name:          Component name for tracing. Defaults to "KeywordRetriever".
        hook:          Optional TraceHook for observability.

    Example::

        retriever = KeywordRetriever(catalog=[
            {"name": "orders", "description": "주문 정보 테이블"},
        ])
        results = retriever("주문 조회")
        print(results)  # [{"name": "orders", ...}]
    """

    def __init__(
        self,
        *,
        catalog: list[dict],
        top_n: int = 5,
        index_fields: Optional[list[str]] = None,
        name: Optional[str] = None,
        hook: Optional[TraceHook] = None,
    ) -> None:
        super().__init__(name=name or "KeywordRetriever", hook=hook)
        self._catalog = catalog
        self._top_n = top_n
        self._index_fields = (
            index_fields if index_fields is not None else _DEFAULT_INDEX_FIELDS
        )
        self._index = _BM25Index(catalog, self._index_fields)

    def run(self, query: str) -> list[CatalogEntry]:
        """
        Search the catalog with BM25 and return top-N matching entries.

        Args:
            query: Natural language search query.

        Returns:
            Ranked list of matching catalog entries (BM25 score descending).
            Empty list if no match or catalog is empty.
        """
        if not self._catalog:
            return []

        scores = self._index.score(query)

        # Pair each catalog entry with its score, sort descending
        ranked = sorted(
            zip(scores, self._catalog),
            key=lambda x: x[0],
            reverse=True,
        )

        # Return up to top_n entries that have a positive score
        return [entry for score, entry in ranked[: self._top_n] if score > 0.0]
