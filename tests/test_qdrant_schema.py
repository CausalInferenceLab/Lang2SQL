import asyncio
import sqlite3

import pytest

from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer
from lang2sql.adapters.storage.qdrant_schema import QdrantSchema
from lang2sql.core.identity import Identity
from lang2sql.tenancy.schema_search import schema_index
from lang2sql.tenancy.tool_runtime import build_tool_context


def test_schema_search_scope_and_dispatch(tmp_path, monkeypatch):
    monkeypatch.setenv("LANG2SQL_QDRANT_URL", "http://qdrant")
    monkeypatch.setenv("LANG2SQL_EMBEDDING_URL", "http://encoder/v1/embeddings")
    monkeypatch.setenv("LANG2SQL_EMBEDDING_MODEL", "test")
    db = SqlAlchemyExplorer("sqlite:///" + str(tmp_path / "one.db"))
    index = schema_index(db)
    assert index.alias("a") != index.alias("b")
    assert index.alias("a") != schema_index(
        SqlAlchemyExplorer(db.url, schema="other")
    ).alias("a")
    assert index.alias("a") != schema_index(SqlAlchemyExplorer(db.url + "2")).alias("a")
    assert schema_index(SqlAlchemyExplorer("sqlite:///:memory:")) is None
    calls = []

    async def search(self, scope, query):
        calls.append((scope, query))
        return [{"description": "customers\ncustomer_id INTEGER"}]

    monkeypatch.setattr(QdrantSchema, "search", search)
    ctx = build_tool_context(Identity("reader", guild_id="a"), explorer=db)
    try:
        result = asyncio.run(
            ctx.tools.dispatch("search_schema", {"query": "who buys?"}, ctx, "one")
        )
        assert "customer_id" in result.content
        assert calls == [("a", "who buys?")]
    finally:
        ctx.store.close()


@pytest.mark.asyncio
async def test_refresh_does_not_publish_partial_snapshot(tmp_path, monkeypatch):
    db = tmp_path / "db.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute("create table orders(id integer)")
    index = QdrantSchema("http://qdrant", "http://embed", "m", "", "", "warehouse")
    requests = []

    async def embed(self, session, texts):
        return [[1.0] for _ in texts]

    async def request(self, session, method, path, body=None):
        requests.append((method, path))
        if "/points?" in path:
            raise ValueError("upload failed")

    monkeypatch.setattr(QdrantSchema, "_embed", embed)
    monkeypatch.setattr(QdrantSchema, "_request", request)
    with pytest.raises(ValueError, match="upload failed"):
        await index.refresh("org", SqlAlchemyExplorer("sqlite:///" + str(db)))
    assert requests[-1] == ("DELETE", requests[0][1])
    assert all(path != "/collections/aliases" for _, path in requests)


@pytest.mark.asyncio
async def test_refresh_publishes_current_tables_and_replaces_previous(
    tmp_path, monkeypatch
):
    db = tmp_path / "sales.sqlite"
    explorer = SqlAlchemyExplorer("sqlite:///" + str(db))
    index = QdrantSchema("http://qdrant", "http://embed", "model", "", "", "test")
    alias = index.alias("org")
    collections = {}
    aliases = {}

    async def embed(self, session, texts):
        return [[1.0] for _ in texts]

    async def request(self, session, method, path, body=None):
        if path == "/aliases":
            return {
                "aliases": [
                    {"alias_name": k, "collection_name": v} for k, v in aliases.items()
                ]
            }
        if path == "/collections/aliases":
            for action in body["actions"]:
                if "delete_alias" in action:
                    del aliases[action["delete_alias"]["alias_name"]]
                else:
                    entry = action["create_alias"]
                    aliases[entry["alias_name"]] = entry["collection_name"]
            return
        name = path.split("/")[2]
        if method == "DELETE":
            del collections[name]
        elif path.endswith("wait=true"):
            collections[name].extend(p["payload"] for p in body["points"])
        else:
            collections[name] = []

    monkeypatch.setattr(QdrantSchema, "_embed", embed)
    monkeypatch.setattr(QdrantSchema, "_request", request)
    with sqlite3.connect(db) as conn:
        conn.execute("create table customers(id integer)")
    assert await index.refresh("org", explorer) == 1
    assert "customers" in collections[aliases[alias]][0]["description"]
    with sqlite3.connect(db) as conn:
        conn.execute("drop table customers")
    assert await index.refresh("org", explorer) == 0
    assert collections == {aliases[alias]: []}


@pytest.mark.asyncio
async def test_indexed_agent_discovers_tables_on_demand(tmp_path, monkeypatch):
    from lang2sql.harness.system_prompt import build_system_prompt
    from lang2sql.tenancy.concierge import ContextConcierge

    db = tmp_path / "catalog.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute("create table repeat_purchases(customer_id integer)")
    monkeypatch.setenv("LANG2SQL_QDRANT_URL", "http://qdrant")
    monkeypatch.setenv("LANG2SQL_EMBEDDING_URL", "http://encoder/v1/embeddings")
    monkeypatch.setenv("LANG2SQL_EMBEDDING_MODEL", "test")
    concierge = ContextConcierge(explorer=SqlAlchemyExplorer("sqlite:///" + str(db)))
    try:
        ctx = await concierge.build_context(Identity("reader", guild_id="org"))
        prompt = await build_system_prompt(ctx)
        assert "search_schema" in prompt and "repeat_purchases" not in prompt
        result = await ctx.tools.dispatch("explore_schema", {}, ctx, "fallback")
        assert "repeat_purchases" in result.content
    finally:
        concierge.store.close()
