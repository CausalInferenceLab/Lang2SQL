"""Rebuild a scoped table index in a staging collection, then publish its alias."""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from uuid import uuid4

import aiohttp

from ...core.ports.explorer import ExplorerPort

logger = logging.getLogger(__name__)
_BATCH = 32


@dataclass(frozen=True)
class QdrantSchema:
    url: str
    embedding_url: str
    model: str
    api_key: str = field(repr=False)
    embedding_key: str = field(repr=False)
    connection: str

    def alias(self, scope: str) -> str:
        key = json.dumps([scope, self.connection, self.embedding_url, self.model])
        return "lang2sql_schema_" + hashlib.sha256(key.encode()).hexdigest()

    async def _request(self, session, method, path, body=None):
        headers = {"api-key": self.api_key} if self.api_key else {}
        async with session.request(
            method, self.url + path, json=body, headers=headers, raise_for_status=True
        ) as response:
            return (await response.json())["result"]

    async def _embed(self, session, texts):
        headers = (
            {"Authorization": f"Bearer {self.embedding_key}"}
            if self.embedding_key
            else {}
        )
        async with session.post(
            self.embedding_url,
            headers=headers,
            json={"model": self.model, "input": texts},
            raise_for_status=True,
        ) as response:
            rows = sorted((await response.json())["data"], key=lambda r: r["index"])
        if [row["index"] for row in rows] != list(range(len(texts))):
            raise ValueError("Embedding service returned incomplete batch")
        return [row["embedding"] for row in rows]

    async def refresh(self, scope: str, explorer: ExplorerPort) -> int:
        alias = self.alias(scope)
        staging = alias + "_" + uuid4().hex
        tables = await explorer.list_tables()
        logger.info(
            "schema_index_start alias=%s collection=%s tables=%s",
            alias,
            staging,
            len(tables),
        )
        async with aiohttp.ClientSession() as session:
            # One probe also gives empty databases a valid empty index.
            dimension = len((await self._embed(session, ["table schema"]))[0])
            await self._request(
                session,
                "PUT",
                "/collections/" + staging,
                {"vectors": {"size": dimension, "distance": "Cosine"}},
            )
            try:
                for start in range(0, len(tables), _BATCH):
                    payloads = []
                    for ref in tables[start : start + _BATCH]:
                        table = await explorer.describe_table(ref.name)
                        columns = [
                            f"{c.name} {c.type} {c.description}".strip()
                            for c in table.columns
                        ]
                        description = "\n".join(
                            filter(
                                None,
                                [
                                    ref.qualified,
                                    table.description or ref.description,
                                    *columns,
                                ],
                            )
                        )
                        payloads.append({"table": ref.name, "description": description})
                    vectors = await self._embed(
                        session, [p["description"] for p in payloads]
                    )
                    points = [
                        {"id": start + i, "vector": vector, "payload": payload}
                        for i, (payload, vector) in enumerate(zip(payloads, vectors))
                    ]
                    await self._request(
                        session,
                        "PUT",
                        f"/collections/{staging}/points?wait=true",
                        {"points": points},
                    )
                aliases = (await self._request(session, "GET", "/aliases"))["aliases"]
                previous = next(
                    (a["collection_name"] for a in aliases if a["alias_name"] == alias),
                    None,
                )
            except Exception:
                logger.exception("schema_index_build_failed collection=%s", staging)
                try:
                    await self._request(session, "DELETE", "/collections/" + staging)
                except (aiohttp.ClientError, TimeoutError) as cleanup_error:
                    logger.warning(
                        "schema_index_cleanup collection=%s error=%s",
                        staging,
                        cleanup_error,
                    )
                raise

            actions = [{"delete_alias": {"alias_name": alias}}] if previous else []
            actions.append(
                {"create_alias": {"alias_name": alias, "collection_name": staging}}
            )
            # A lost response may mean publication succeeded. Never discard staging here.
            await self._request(
                session, "POST", "/collections/aliases", {"actions": actions}
            )
            if previous and previous.startswith(alias + "_"):
                try:
                    await self._request(session, "DELETE", "/collections/" + previous)
                except (aiohttp.ClientError, TimeoutError) as exc:
                    logger.warning(
                        "schema_index_cleanup collection=%s error=%s", previous, exc
                    )
        logger.info("schema_index_published alias=%s tables=%s", alias, len(tables))
        return len(tables)

    async def search(self, scope: str, query: str):
        alias = self.alias(scope)
        async with aiohttp.ClientSession() as session:
            vector = (await self._embed(session, [query]))[0]
            try:
                result = await self._request(
                    session,
                    "POST",
                    f"/collections/{alias}/points/query",
                    {"query": vector, "limit": 5, "with_payload": True},
                )
            except aiohttp.ClientResponseError as exc:
                if exc.status != 404:
                    raise
                raise ValueError(
                    "Schema index missing. Run lang2sql-tools index-schema for this organization and DB."
                ) from exc
        logger.info("schema_search alias=%s hits=%s", alias, len(result["points"]))
        return [p["payload"] for p in result["points"]]
