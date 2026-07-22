"""EncryptedSecrets — per-scope credential storage over a :class:`SqliteStore` kv.

Real symmetric encryption via :class:`cryptography.fernet.Fernet` (AES-128-CBC +
HMAC). Each secret value is encrypted to a Fernet token before it lands in the
kv table, so a stolen database file yields no plaintext DSNs/API keys without
the key. The :class:`SecretsPort` boundary is unchanged, so callers (concierge,
``/connect``) need no edits.

Key management:

* If env ``LANG2SQL_SECRET_KEY`` holds a urlsafe-base64 Fernet key, that is the
  encryption key — point every deployment instance at the same value so secrets
  decrypt across restarts and across machines.
* Otherwise a key is generated once and persisted in the kv table under the
  reserved scope ``__secrets__`` / key ``fernet_key``. This keeps a single
  sqlite file self-contained (secrets survive restart) but is only as private
  as the file itself — set ``LANG2SQL_SECRET_KEY`` for real key separation.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os

from cryptography.fernet import Fernet

from ..adapters.storage.sqlite_store import SqliteStore

# Reserved kv location for the auto-generated key (when env key is absent).
_KEY_SCOPE = "__secrets__"
_KEY_NAME = "fernet_key"
_ENV_KEY = "LANG2SQL_SECRET_KEY"


def _resolve_key(store: SqliteStore) -> bytes:
    """Pick the Fernet key: env override, else persisted, else freshly generated."""
    env_key = os.environ.get(_ENV_KEY)
    if env_key:
        return env_key.encode("ascii")

    stored = store.kv_get(_KEY_SCOPE, _KEY_NAME)
    if stored is not None:
        return stored.encode("ascii")

    key = Fernet.generate_key()
    store.kv_set(_KEY_SCOPE, _KEY_NAME, key.decode("ascii"))
    return key


class EncryptedSecrets:
    """Implements :class:`SecretsPort` on top of a kv-capable store."""

    def __init__(self, store: SqliteStore, *, key: bytes | None = None) -> None:
        self._store = store
        resolved_key = key if key is not None else _resolve_key(store)
        self._fernet = Fernet(resolved_key)
        self._source_identity_key = hmac.new(
            resolved_key, b"lang2sql-source-identity-v1", hashlib.sha256
        ).digest()

    async def get(self, scope: str, key: str) -> str | None:
        blob = self._store.kv_get(scope, key)
        if blob is None:
            return None
        return self._fernet.decrypt(blob.encode("ascii")).decode("utf-8")

    async def set(self, scope: str, key: str, value: str) -> None:
        token = self.encode_for_storage(value)
        self._store.kv_set(scope, key, token)

    async def delete(self, scope: str, key: str) -> None:
        self._store.kv_delete(scope, key)

    def encode_for_storage(self, value: str) -> str:
        """Seal a value for an atomic bundle written by the concierge."""

        return self._fernet.encrypt(value.encode("utf-8")).decode("ascii")

    def decode_from_storage(self, value: str) -> str:
        """Open one value from a transactionally read encrypted bundle."""

        return self._fernet.decrypt(value.encode("ascii")).decode("utf-8")

    def source_identity(
        self, scope: str, dsn: str, extras: dict[str, str]
    ) -> str:
        """Return a non-reversible identity for the exact execution material."""

        material = json.dumps(
            {
                "version": 1,
                "scope": scope,
                "dsn": dsn.strip(),
                "extras": dict(sorted(extras.items())),
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hmac.new(
            self._source_identity_key, material, hashlib.sha256
        ).hexdigest()
