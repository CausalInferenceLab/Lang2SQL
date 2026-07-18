"""Tenancy tests — EncryptedSecrets round trip + ContextConcierge wiring.

No network: with no OPENAI_API_KEY the concierge falls back to FakeLLM.
"""

from __future__ import annotations

import asyncio
import os

from lang2sql.adapters.llm.fake import FakeLLM
from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.core.identity import Identity
from lang2sql.core.types import Message, Role
from lang2sql.harness.context import HarnessContext
from lang2sql.harness.session import Session
from lang2sql.tenancy.concierge import ContextConcierge
from lang2sql.tenancy.encrypted_secrets import EncryptedSecrets


def test_encrypted_secrets_round_trip() -> None:
    store = SqliteStore()
    secrets = EncryptedSecrets(store)

    assert asyncio.run(secrets.get("guild:1", "dsn")) is None
    asyncio.run(secrets.set("guild:1", "dsn", "postgresql://u:p@host/db"))
    assert asyncio.run(secrets.get("guild:1", "dsn")) == "postgresql://u:p@host/db"

    # Stored value is obfuscated, not plaintext.
    assert store.kv_get("guild:1", "dsn") != "postgresql://u:p@host/db"

    asyncio.run(secrets.set("guild:1", "dsn", "postgresql://new"))  # overwrite
    assert asyncio.run(secrets.get("guild:1", "dsn")) == "postgresql://new"

    asyncio.run(secrets.delete("guild:1", "dsn"))
    assert asyncio.run(secrets.get("guild:1", "dsn")) is None


def test_build_context_populates_llm_and_session() -> None:
    saved = os.environ.pop("OPENAI_API_KEY", None)
    try:
        concierge = ContextConcierge()
        identity = Identity(user_id="u1", guild_id="g", channel_id="c")
        ctx = asyncio.run(
            concierge.build_context(identity, user_text="how many orders?")
        )

        assert isinstance(ctx, HarnessContext)
        assert isinstance(ctx.llm, FakeLLM)  # no key → fallback
        assert isinstance(ctx.session, Session)
        assert ctx.session.identity == identity
        assert ctx.explorer is not None
        assert ctx.safety is not None
        assert ctx.store is not None
        assert ctx.audit is not None
    finally:
        if saved is not None:
            os.environ["OPENAI_API_KEY"] = saved


def test_build_context_restores_saved_session() -> None:
    store = SqliteStore()
    identity = Identity(user_id="u1", channel_id="c", thread_id="t")
    prior = Session(identity=identity)
    prior.add(Message(role=Role.USER, content="earlier turn"))
    asyncio.run(store.save(identity.session_key(), prior))

    concierge = ContextConcierge(store=store, llm=FakeLLM())
    ctx = asyncio.run(concierge.build_context(identity))

    assert len(ctx.session.transcript) == 1
    assert ctx.session.transcript[0].content == "earlier turn"


def test_injected_overrides_are_used() -> None:
    store = SqliteStore()
    fake = FakeLLM()
    concierge = ContextConcierge(store=store, llm=fake)
    ctx = asyncio.run(concierge.build_context(Identity(user_id="u1")))
    assert ctx.llm is fake
    assert ctx.audit is store
