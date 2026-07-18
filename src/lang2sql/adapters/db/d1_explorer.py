"""Cloudflare D1 explorer — read-only introspection over the D1 HTTP API.

D1 is SQLite that lives on Cloudflare's edge and is only reachable from a Worker
or over the REST query endpoint. A Python process (our Discord bot) uses the
**HTTP Query API**:

    POST /client/v4/accounts/{account_id}/d1/database/{database_id}/query
    Authorization: Bearer <token>
    {"sql": "...", "params": [...]}

Since D1 *is* SQLite, schema introspection uses ``sqlite_master`` / ``PRAGMA``.
The HTTP call is injectable (``transport``) so the adapter is unit-testable with
no network.
"""

from __future__ import annotations

import asyncio
import json
import os
import urllib.request
from typing import Any, Callable

from ...core.ports.explorer import Column, Table

_API_ROOT = "https://api.cloudflare.com/client/v4"

# A transport takes (sql, params) and returns the parsed D1 JSON response.
Transport = Callable[[str, list], dict]


class D1Explorer:
    """ExplorerPort backed by Cloudflare D1's HTTP query API."""

    def __init__(
        self,
        account_id: str,
        database_id: str,
        token: str | None = None,
        *,
        transport: Transport | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.account_id = account_id
        self.database_id = database_id
        self._token = (
            token if token is not None else os.environ.get("CLOUDFLARE_API_TOKEN")
        )
        self._timeout = timeout
        self._transport = transport or self._http_transport

    # --- ExplorerPort ----------------------------------------------------

    async def list_tables(self) -> list[Table]:
        rows = await self._query(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%' ORDER BY name"
        )
        return [Table(name=r["name"], schema="") for r in rows]

    async def describe_table(self, name: str) -> Table:
        rows = await self._query(f"PRAGMA table_info({_ident(name)})")
        cols = [
            Column(
                name=r["name"], type=r["type"] or "", nullable=not bool(r["notnull"])
            )
            for r in rows
        ]
        return Table(name=name, schema="", columns=cols)

    async def sample_rows(self, name: str, limit: int = 5) -> list[dict]:
        return await self._query(f"SELECT * FROM {_ident(name)} LIMIT {int(limit)}")

    async def execute(self, sql: str, limit: int = 1000) -> list[dict]:
        rows = await self._query(sql)
        return rows[: int(limit)]

    # --- internals -------------------------------------------------------

    async def _query(self, sql: str, params: list | None = None) -> list[dict]:
        resp = await asyncio.to_thread(self._transport, sql, params or [])
        if not resp.get("success", False):
            errors = resp.get("errors") or resp.get("messages") or "unknown D1 error"
            raise RuntimeError(f"D1 query failed: {errors}")
        result = resp.get("result") or []
        if not result:
            return []
        # The query endpoint returns one result object per statement.
        return result[0].get("results", []) or []

    def _http_transport(self, sql: str, params: list) -> dict:
        if not self._token:
            raise RuntimeError(
                "CLOUDFLARE_API_TOKEN not set (D1 requires an API token)"
            )
        url = (
            f"{_API_ROOT}/accounts/{self.account_id}"
            f"/d1/database/{self.database_id}/query"
        )
        body = json.dumps({"sql": sql, "params": params}).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=self._timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))


def _ident(name: str) -> str:
    """Quote a SQLite identifier, rejecting anything that isn't a plain name.

    introspection helpers interpolate the table name into PRAGMA/SELECT where
    binds aren't allowed, so we hard-validate to avoid injection.
    """
    if not name.replace("_", "").isalnum():
        raise ValueError(f"unsafe table identifier: {name!r}")
    return f'"{name}"'
