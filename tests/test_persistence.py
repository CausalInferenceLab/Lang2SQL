"""Persistence tests — KV federation durability + Fernet secrets (★④ §4.1).

Two guarantees under test: (1) semantic definitions stored in KV survive a fresh
store instance pointed at the same sqlite file, and (2) secrets are stored as
real Fernet ciphertext, not recoverable without the key.
"""

from __future__ import annotations

import asyncio
import os

import pytest
from cryptography.fernet import Fernet, InvalidToken

from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.tenancy.encrypted_secrets import EncryptedSecrets
from lang2sql.tools.semantic_federation import FedEntry, _kv_key, _render_effective


def test_kv_federation_survives_new_instance(tmp_path) -> None:
    db = str(tmp_path / "kv.db")
    scope = "g1"

    writer = SqliteStore(db)
    entry = FedEntry(
        term="revenue", layer="guild", entity="", definition="sum of order totals"
    )
    writer.kv_set(scope, _kv_key("revenue", "guild", ""), entry.to_json())
    writer.close()

    reader = SqliteStore(db)
    rendered = _render_effective(reader, scope, "c1", "u1")
    assert "revenue" in rendered
    assert "sum of order totals" in rendered
    reader.close()


def test_kv_channel_overrides_guild_persisted(tmp_path) -> None:
    db = str(tmp_path / "kv.db")
    scope = "g1"

    store = SqliteStore(db)
    store.kv_set(
        scope,
        _kv_key("active_user", "guild", ""),
        FedEntry("active_user", "guild", "", "guild def").to_json(),
    )
    store.kv_set(
        scope,
        _kv_key("active_user", "channel", "c1"),
        FedEntry("active_user", "channel", "c1", "channel def").to_json(),
    )
    store.close()

    reader = SqliteStore(db)
    rendered = _render_effective(reader, scope, "c1", "u1")
    assert "channel def" in rendered
    assert "guild def" not in rendered
    reader.close()


def test_encrypted_secrets_round_trip_and_ciphertext(tmp_path) -> None:
    key = Fernet.generate_key()
    store = SqliteStore()
    secrets = EncryptedSecrets(store, key=key)

    assert asyncio.run(secrets.get("guild:1", "dsn")) is None
    asyncio.run(secrets.set("guild:1", "dsn", "postgresql://u:p@host/db"))
    assert asyncio.run(secrets.get("guild:1", "dsn")) == "postgresql://u:p@host/db"

    blob = store.kv_get("guild:1", "dsn")
    assert blob is not None
    assert "postgresql" not in blob
    assert blob != "postgresql://u:p@host/db"
    assert (
        Fernet(key).decrypt(blob.encode("ascii")).decode() == "postgresql://u:p@host/db"
    )

    asyncio.run(secrets.delete("guild:1", "dsn"))
    assert asyncio.run(secrets.get("guild:1", "dsn")) is None


def test_encrypted_secrets_wrong_key_fails() -> None:
    store = SqliteStore()
    asyncio.run(EncryptedSecrets(store, key=Fernet.generate_key()).set("s", "k", "v"))

    attacker = EncryptedSecrets(store, key=Fernet.generate_key())
    with pytest.raises(InvalidToken):
        asyncio.run(attacker.get("s", "k"))


def test_encrypted_secrets_survive_new_instance_via_persisted_key(tmp_path) -> None:
    db = str(tmp_path / "secrets.db")
    saved = os.environ.pop("LANG2SQL_SECRET_KEY", None)
    try:
        store = SqliteStore(db)
        asyncio.run(EncryptedSecrets(store).set("guild:1", "dsn", "postgresql://x"))
        store.close()

        reopened = SqliteStore(db)
        secrets = EncryptedSecrets(reopened)
        assert asyncio.run(secrets.get("guild:1", "dsn")) == "postgresql://x"
        reopened.close()
    finally:
        if saved is not None:
            os.environ["LANG2SQL_SECRET_KEY"] = saved
