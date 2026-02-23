from __future__ import annotations

from typing import TypedDict


class CatalogEntry(TypedDict, total=False):
    name: str                
    description: str
    columns: dict[str, str]  
