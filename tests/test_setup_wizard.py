"""/setup wizard: pure DSN assembly + register-and-test + per-scope routing.

The Discord UI layer (setup_wizard.py modal/select) is exercised only by an
import-smoke; its async on_submit eventually calls
``CommandHandlers.register_db_for_guild``, which is what we cover end-to-end
against a real sqlite database.
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy.exc import NoSuchModuleError

from lang2sql.adapters.db import D1Explorer, SqlAlchemyExplorer
from lang2sql.adapters.db.dsn_builder import assemble
from lang2sql.core.identity import Identity
from lang2sql.frontends.discord.commands import CommandHandlers
from lang2sql.semantic.catalog import SemanticCatalog
from lang2sql.tenancy.concierge import ContextConcierge

# --- dsn_builder ---------------------------------------------------------


def test_assemble_postgres_url():
    spec = assemble(
        "postgresql",
        {
            "host": "db.example.com",
            "port": "5432",
            "database": "analytics",
            "user": "u",
            "password": "p",
        },
    )
    assert spec.dsn == "postgresql+psycopg://u:p@db.example.com:5432/analytics"
    assert spec.extras == {}


def test_assemble_url_encodes_special_chars_in_password():
    spec = assemble(
        "postgresql",
        {
            "host": "h",
            "port": "5432",
            "database": "d",
            "user": "u",
            "password": "p@ss/w:rd",
        },
    )
    assert "p%40ss%2Fw%3Ard" in spec.dsn  # @, /, : all encoded


def test_assemble_snowflake_attaches_warehouse():
    spec = assemble(
        "snowflake",
        {
            "account": "ab12345.us-east-1",
            "user": "u",
            "password": "p",
            "database": "DB",
            "warehouse": "WH",
        },
    )
    assert "warehouse=WH" in spec.dsn and "@ab12345.us-east-1/DB" in spec.dsn


def test_assemble_d1_returns_token_in_extras():
    spec = assemble(
        "d1",
        {
            "account_id": "acct",
            "database_id": "db",
            "api_token": "secret",
        },
    )
    assert spec.dsn == "d1://acct/db"
    assert spec.extras == {"d1_token": "secret"}


def test_assemble_sqlite_path():
    spec = assemble("sqlite", {"path": "/tmp/demo.db"})
    assert spec.dsn == "sqlite:////tmp/demo.db"
    assert spec.extras == {}


def test_assemble_missing_required_field_raises():
    with pytest.raises(ValueError, match="missing required"):
        assemble("postgresql", {"host": "h"})  # no user/password/db


def test_assemble_unknown_db_type_raises():
    with pytest.raises(ValueError, match="unsupported"):
        assemble("oracle", {})


# --- register_db_for_guild end-to-end (real sqlite) ----------------------


def _seed_sqlite(path: str) -> None:
    from sqlalchemy import create_engine, text

    eng = create_engine(f"sqlite:///{path}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)"))
        conn.execute(text("INSERT INTO products VALUES (1, 'a'), (2, 'b')"))


def test_register_db_for_guild_success_stores_encrypted(tmp_path):
    db = tmp_path / "demo.db"
    _seed_sqlite(str(db))

    concierge = ContextConcierge()
    handlers = CommandHandlers(concierge)
    identity = Identity(user_id="alice", guild_id="g1", channel_id="c", is_admin=True)

    result = asyncio.run(
        handlers.register_db_for_guild(identity, "sqlite", {"path": str(db)})
    )
    assert "연결 완료" in result.text
    assert asyncio.run(concierge.secrets.get("g1", "db_dsn")) == f"sqlite:///{db}"

    ctx = asyncio.run(concierge.build_context(identity))
    assert isinstance(ctx.explorer, SqlAlchemyExplorer)
    tables = asyncio.run(ctx.explorer.list_tables())
    assert "products" in {t.name for t in tables}


def test_register_db_for_guild_unknown_driver_gives_friendly_error():
    concierge = ContextConcierge()
    handlers = CommandHandlers(concierge)
    identity = Identity(user_id="u", guild_id="g-x", channel_id="c", is_admin=True)
    # Snowflake driver isn't installed in this env; the handler should catch
    # ModuleNotFoundError and produce a clear, non-technical message.
    res = asyncio.run(
        handlers.register_db_for_guild(
            identity,
            "snowflake",
            {
                "account": "a",
                "user": "u",
                "password": "p",
                "database": "d",
                "warehouse": "w",
            },
        )
    )
    assert "uv sync --extra snowflake" in res.text or "Couldn't connect" in res.text


def test_register_missing_duckdb_dialect_gives_install_command(
    monkeypatch, tmp_path
) -> None:
    concierge = ContextConcierge()
    handlers = CommandHandlers(concierge)
    identity = Identity(user_id="u", guild_id="g", channel_id="c", is_admin=True)

    def missing_driver(*_args, **_kwargs):
        raise NoSuchModuleError("Can't load plugin: sqlalchemy.dialects:duckdb")

    monkeypatch.setattr(
        "lang2sql.frontends.discord.commands.build_explorer", missing_driver
    )
    result = asyncio.run(
        handlers.register_db_for_guild(
            identity, "duckdb", {"path": str(tmp_path / "warehouse.duckdb")}
        )
    )
    assert "uv sync --extra duckdb" in result.text
    assert "파일 절대경로" not in result.text


def test_register_db_for_guild_missing_field_reports_setup_error():
    concierge = ContextConcierge()
    handlers = CommandHandlers(concierge)
    identity = Identity(user_id="u", guild_id="g", channel_id="c", is_admin=True)
    res = asyncio.run(
        handlers.register_db_for_guild(
            identity,
            "postgresql",
            {"host": "h"},  # missing user/password/db
        )
    )
    assert "Setup error" in res.text and "missing required" in res.text


def test_register_missing_sqlite_is_file_specific_and_does_not_create(tmp_path):
    missing = tmp_path / "missing.db"
    concierge = ContextConcierge()
    handlers = CommandHandlers(concierge)
    identity = Identity(user_id="u", guild_id="g", channel_id="c", is_admin=True)

    result = asyncio.run(
        handlers.register_db_for_guild(identity, "sqlite", {"path": str(missing)})
    )

    assert "파일 절대경로" in result.text
    assert "읽기 권한" in result.text
    assert not missing.exists()


# --- concierge per-scope explorer routing --------------------------------


def test_concierge_per_scope_dsn_routes_correctly(tmp_path):
    db = tmp_path / "scoped.db"
    _seed_sqlite(str(db))
    concierge = ContextConcierge()

    g_with = Identity(user_id="u", guild_id="g-real", channel_id="c", is_admin=True)
    g_without = Identity(user_id="u", guild_id="g-default", channel_id="c")

    result = asyncio.run(CommandHandlers(concierge).connect(g_with, f"sqlite:///{db}"))
    assert "연결 완료" in result.text

    ctx_with = asyncio.run(concierge.build_context(g_with))
    ctx_without = asyncio.run(concierge.build_context(g_without))

    assert isinstance(ctx_with.explorer, SqlAlchemyExplorer)
    # The guild without a stored DSN falls back to the concierge default
    # (PostgresExplorer stub in this offline env).
    assert ctx_with.explorer is not ctx_without.explorer


def test_concierge_d1_extras_threaded_through_secrets():
    concierge = ContextConcierge()
    concierge.activate_connection(
        scope="g-d1",
        dsn="d1://acct/db",
        extras={"d1_token": "tok-1"},
        catalog=SemanticCatalog(fingerprint="fixture"),
        expected_generation=0,
    )
    identity = Identity(user_id="u", guild_id="g-d1", channel_id="c")
    ctx = asyncio.run(concierge.build_context(identity))
    assert isinstance(ctx.explorer, D1Explorer)
    assert ctx.explorer._token == "tok-1"


def test_reactivation_rotates_generation_and_explorer_cache(tmp_path):
    db1 = tmp_path / "a.db"
    db2 = tmp_path / "b.db"
    _seed_sqlite(str(db1))
    _seed_sqlite(str(db2))
    concierge = ContextConcierge()
    identity = Identity(user_id="u", guild_id="g", channel_id="c", is_admin=True)
    handlers = CommandHandlers(concierge)

    first = asyncio.run(handlers.connect(identity, f"sqlite:///{db1}"))
    assert "연결 완료" in first.text
    ctx1 = asyncio.run(concierge.build_context(identity))
    binding1 = concierge.connection_binding("g")

    second = asyncio.run(handlers.connect(identity, f"sqlite:///{db2}"))
    assert "연결 완료" in second.text
    ctx_fresh = asyncio.run(concierge.build_context(identity))
    binding2 = concierge.connection_binding("g")
    assert ctx_fresh.explorer is not ctx1.explorer
    assert binding1 is not None and binding2 is not None
    assert binding2.generation == binding1.generation + 1


# --- UI module import smoke ----------------------------------------------


def test_setup_wizard_module_imports_without_discord_runtime():
    # The wizard imports discord.ui at module level. Make sure that succeeds in
    # a no-gateway environment — the same contract as bot.py's import-safety.
    import lang2sql.frontends.discord.setup_wizard  # noqa: F401


def test_setup_picker_has_a_label_for_every_supported_database():
    from lang2sql.adapters.db.dsn_builder import SUPPORTED_DB_TYPES
    from lang2sql.frontends.discord.setup_wizard import _LABELS

    assert set(SUPPORTED_DB_TYPES) <= set(_LABELS)


def test_discord_review_storage_failure_returns_retryable_message(monkeypatch):
    concierge = ContextConcierge()
    handlers = CommandHandlers(concierge)
    identity = Identity(user_id="u", guild_id="g", channel_id="c", is_admin=True)

    def fail_review(*_args, **_kwargs):
        raise RuntimeError("forced review storage failure")

    monkeypatch.setattr(concierge.semantic, "confirm_pending", fail_review)
    result = asyncio.run(handlers.semantic_review(identity, "sum"))
    assert "BLOCKED" in result.text
    assert "다시 시도" in result.text
