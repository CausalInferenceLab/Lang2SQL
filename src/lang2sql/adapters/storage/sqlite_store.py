"""SqliteStore — the real V1 persistence backend (stdlib :mod:`sqlite3`).

One store, three roles:

* :class:`AuditPort` — append-only ``audit`` table behind ``/audit me``.
* :class:`SessionStorePort` — serialize/restore a :class:`Session` as JSON.
* a generic key-value table the secrets adapter (tenancy) wraps.

sqlite is synchronous; V1 just runs the calls inline inside the async methods,
which is fine for the expected load. The connection uses
``check_same_thread=False`` so it tolerates being touched from the event-loop
thread pool.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from collections.abc import Callable
from typing import Any

from ...core.identity import Identity
from ...core.ports.audit import AuditEvent
from ...core.types import Message, Role, ToolCall
from ...harness.session import Session


class SqliteStore:
    """Append-only audit + session + kv storage on one sqlite connection."""

    def __init__(self, path: str = ":memory:") -> None:
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._create_tables()

    def _create_tables(self) -> None:
        with self._lock:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS audit (
                    id     INTEGER PRIMARY KEY AUTOINCREMENT,
                    actor  TEXT NOT NULL,
                    action TEXT NOT NULL,
                    scope  TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    ts     REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sessions (
                    key  TEXT PRIMARY KEY,
                    data TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS kv (
                    scope TEXT NOT NULL,
                    key   TEXT NOT NULL,
                    value TEXT NOT NULL,
                    PRIMARY KEY (scope, key)
                );
                """)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- AuditPort -------------------------------------------------------

    async def record(self, event: AuditEvent) -> None:
        ts = event.ts or time.time()
        with self._lock:
            self._conn.execute(
                "INSERT INTO audit (actor, action, scope, detail, ts) VALUES (?, ?, ?, ?, ?)",
                (event.actor, event.action, event.scope, json.dumps(event.detail), ts),
            )
            self._conn.commit()

    async def query(self, actor: str, limit: int = 20) -> list[AuditEvent]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT actor, action, scope, detail, ts FROM audit "
                "WHERE actor = ? ORDER BY id DESC LIMIT ?",
                (actor, limit),
            ).fetchall()
        return [
            AuditEvent(
                actor=r["actor"],
                action=r["action"],
                scope=r["scope"],
                detail=json.loads(r["detail"]),
                ts=r["ts"],
            )
            for r in rows
        ]

    # -- SessionStorePort ------------------------------------------------

    async def load(self, key: str) -> Session | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT data FROM sessions WHERE key = ?", (key,)
            ).fetchone()
        if row is None:
            return None
        return _deserialize_session(json.loads(row["data"]))

    async def save(self, key: str, session: Session) -> None:
        data = json.dumps(_serialize_session(session))
        with self._lock:
            self._conn.execute(
                "INSERT INTO sessions (key, data) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET data = excluded.data",
                (key, data),
            )
            self._conn.commit()

    # -- generic key-value (wrapped by the secrets adapter) --------------

    def kv_get(self, scope: str, key: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM kv WHERE scope = ? AND key = ?", (scope, key)
            ).fetchone()
        return row["value"] if row else None

    def kv_get_many(self, scope: str, keys: set[str]) -> dict[str, str]:
        """Read a related KV snapshot without mixing connection generations."""

        if not keys:
            return {}
        placeholders = ",".join("?" for _ in keys)
        with self._lock:
            rows = self._conn.execute(
                f"SELECT key, value FROM kv WHERE scope = ? AND key IN ({placeholders})",
                (scope, *sorted(keys)),
            ).fetchall()
        return {str(row["key"]): str(row["value"]) for row in rows}

    def kv_set(self, scope: str, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO kv (scope, key, value) VALUES (?, ?, ?) "
                "ON CONFLICT(scope, key) DO UPDATE SET value = excluded.value",
                (scope, key, value),
            )
            self._conn.commit()

    def kv_apply_atomic(
        self,
        scope: str,
        *,
        upserts: dict[str, str],
        delete_keys: set[str] | None = None,
    ) -> None:
        """Commit a related KV bundle in one crash-safe SQLite transaction."""

        deletes = set(delete_keys or ()) - set(upserts)
        with self._lock:
            with self._conn:
                self._conn.executemany(
                    "INSERT INTO kv (scope, key, value) VALUES (?, ?, ?) "
                    "ON CONFLICT(scope, key) DO UPDATE SET value = excluded.value",
                    [(scope, key, value) for key, value in upserts.items()],
                )
                if deletes:
                    self._conn.executemany(
                        "DELETE FROM kv WHERE scope = ? AND key = ?",
                        [(scope, key) for key in deletes],
                    )

    def kv_activate_generation(
        self,
        scope: str,
        *,
        expected_generation: int,
        build_upserts: Callable[[int], dict[str, str]],
        delete_keys: set[str] | None = None,
        generation_key: str,
    ) -> int:
        """CAS-activate one credential/catalog bundle with a monotonic generation."""

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT value FROM kv WHERE scope = ? AND key = ?",
                    (scope, generation_key),
                ).fetchone()
                current = int(row["value"]) if row is not None else 0
                if current != expected_generation:
                    raise RuntimeError("connection generation changed during scan")
                generation = current + 1
                upserts = dict(build_upserts(generation))
                upserts[generation_key] = str(generation)
                deletes = set(delete_keys or ()) - set(upserts)
                self._conn.executemany(
                    "INSERT INTO kv (scope, key, value) VALUES (?, ?, ?) "
                    "ON CONFLICT(scope, key) DO UPDATE SET value = excluded.value",
                    [(scope, key, value) for key, value in upserts.items()],
                )
                if deletes:
                    self._conn.executemany(
                        "DELETE FROM kv WHERE scope = ? AND key = ?",
                        [(scope, key) for key in deletes],
                    )
                self._conn.commit()
                return generation
            except BaseException:
                self._conn.rollback()
                raise

    def kv_set_bound_catalog(
        self,
        scope: str,
        *,
        catalog_key: str,
        catalog_value: str,
        binding_key: str,
        expected_binding_value: str,
        generation_key: str,
        expected_generation: int,
        expected_review_revision: int | None = None,
    ) -> None:
        """Write a catalog only while its active connection binding still wins."""

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                rows = self._conn.execute(
                    "SELECT key, value FROM kv WHERE scope = ? AND key IN (?, ?, ?)",
                    (scope, binding_key, generation_key, catalog_key),
                ).fetchall()
                snapshot = {
                    str(row["key"]): str(row["value"]) for row in rows
                }
                if (
                    snapshot.get(binding_key) != expected_binding_value
                    or snapshot.get(generation_key) != str(expected_generation)
                ):
                    raise RuntimeError("connection changed before catalog mutation")
                if expected_review_revision is not None:
                    try:
                        current_revision = int(
                            json.loads(snapshot[catalog_key]).get("review_revision", 0)
                        )
                    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                        raise RuntimeError(
                            "catalog changed before semantic review"
                        ) from None
                    if current_revision != expected_review_revision:
                        raise RuntimeError("catalog changed before semantic review")
                self._conn.execute(
                    "INSERT INTO kv (scope, key, value) VALUES (?, ?, ?) "
                    "ON CONFLICT(scope, key) DO UPDATE SET value = excluded.value",
                    (scope, catalog_key, catalog_value),
                )
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise

    def kv_mutate_snapshot(
        self,
        scope: str,
        *,
        keys: set[str],
        mutate: Callable[
            [dict[str, str]],
            tuple[dict[str, str], set[str], Any]
            | tuple[dict[str, str], set[str], Any, AuditEvent | None],
        ],
    ) -> Any:
        """Atomically validate a KV snapshot and apply its derived mutation.

        Semantic action capabilities use this seam so token consumption,
        connection/catalog CAS validation, catalog mutation, and receipt
        creation cannot be separated by a source-switch race.
        """

        ordered_keys = sorted(keys)
        placeholders = ",".join("?" for _ in ordered_keys)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                rows = self._conn.execute(
                    f"SELECT key, value FROM kv WHERE scope = ? "
                    f"AND key IN ({placeholders})",
                    (scope, *ordered_keys),
                ).fetchall()
                snapshot = {
                    str(row["key"]): str(row["value"]) for row in rows
                }
                mutation = mutate(snapshot)
                if len(mutation) == 3:
                    upserts, delete_keys, result = mutation
                    audit_event = None
                else:
                    upserts, delete_keys, result, audit_event = mutation
                deletes = set(delete_keys) - set(upserts)
                if upserts:
                    self._conn.executemany(
                        "INSERT INTO kv (scope, key, value) VALUES (?, ?, ?) "
                        "ON CONFLICT(scope, key) DO UPDATE SET value = excluded.value",
                        [
                            (scope, key, value)
                            for key, value in upserts.items()
                        ],
                    )
                if deletes:
                    self._conn.executemany(
                        "DELETE FROM kv WHERE scope = ? AND key = ?",
                        [(scope, key) for key in deletes],
                    )
                if audit_event is not None:
                    ts = audit_event.ts or time.time()
                    self._conn.execute(
                        "INSERT INTO audit (actor, action, scope, detail, ts) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (
                            audit_event.actor,
                            audit_event.action,
                            audit_event.scope,
                            json.dumps(audit_event.detail),
                            ts,
                        ),
                    )
                self._conn.commit()
                return result
            except BaseException:
                self._conn.rollback()
                raise

    def kv_mutate_scoped_snapshot(
        self,
        *,
        entries: set[tuple[str, str]],
        mutate: Callable[
            [dict[tuple[str, str], str]],
            tuple[
                dict[tuple[str, str], str],
                set[tuple[str, str]],
                Any,
            ]
            | tuple[
                dict[tuple[str, str], str],
                set[tuple[str, str]],
                Any,
                AuditEvent | None,
            ],
        ],
    ) -> Any:
        """Atomically mutate an exact set of KV entries across scopes.

        Pending reviews are requester-scoped while their catalog is guild-
        scoped.  This dedicated seam keeps the two records and the audit event
        in one SQLite transaction instead of approximating atomicity with
        sequential per-scope writes.
        """

        ordered_entries = sorted(entries)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                snapshot: dict[tuple[str, str], str] = {}
                for entry_scope, entry_key in ordered_entries:
                    row = self._conn.execute(
                        "SELECT value FROM kv WHERE scope = ? AND key = ?",
                        (entry_scope, entry_key),
                    ).fetchone()
                    if row is not None:
                        snapshot[(entry_scope, entry_key)] = str(row["value"])
                mutation = mutate(snapshot)
                if len(mutation) == 3:
                    upserts, delete_entries, result = mutation
                    audit_event = None
                else:
                    upserts, delete_entries, result, audit_event = mutation
                deletes = set(delete_entries) - set(upserts)
                if upserts:
                    self._conn.executemany(
                        "INSERT INTO kv (scope, key, value) VALUES (?, ?, ?) "
                        "ON CONFLICT(scope, key) DO UPDATE SET value = excluded.value",
                        [
                            (entry_scope, entry_key, value)
                            for (entry_scope, entry_key), value in upserts.items()
                        ],
                    )
                if deletes:
                    self._conn.executemany(
                        "DELETE FROM kv WHERE scope = ? AND key = ?",
                        list(deletes),
                    )
                if audit_event is not None:
                    ts = audit_event.ts or time.time()
                    self._conn.execute(
                        "INSERT INTO audit (actor, action, scope, detail, ts) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (
                            audit_event.actor,
                            audit_event.action,
                            audit_event.scope,
                            json.dumps(audit_event.detail),
                            ts,
                        ),
                    )
                self._conn.commit()
                return result
            except BaseException:
                self._conn.rollback()
                raise

    def kv_delete(self, scope: str, key: str) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM kv WHERE scope = ? AND key = ?", (scope, key)
            )
            self._conn.commit()

    def kv_delete_if_value(self, scope: str, key: str, expected_value: str) -> bool:
        """Delete only the exact record previously examined by the caller."""

        with self._lock:
            cursor = self._conn.execute(
                "DELETE FROM kv WHERE scope = ? AND key = ? AND value = ?",
                (scope, key, expected_value),
            )
            self._conn.commit()
            return cursor.rowcount == 1

    @staticmethod
    def _escape_like(s: str) -> str:
        return s.replace("!", "!!").replace("%", "!%").replace("_", "!_")

    def kv_delete_prefix(self, scope: str, prefix: str) -> int:
        """Delete all keys under scope that start with prefix. Returns count deleted."""
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM kv WHERE scope = ? AND key LIKE ? ESCAPE '!'",
                (scope, self._escape_like(prefix) + "%"),
            )
            self._conn.commit()
            return cur.rowcount

    def kv_list_prefix(self, scope: str, prefix: str) -> list[tuple[str, str]]:
        """Return (key, value) pairs for all keys under scope that start with prefix."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT key, value FROM kv WHERE scope = ? AND key LIKE ? ESCAPE '!' ORDER BY key",
                (scope, self._escape_like(prefix) + "%"),
            ).fetchall()
        return [(r["key"], r["value"]) for r in rows]

    def kv_list_key(self, key: str) -> list[tuple[str, str]]:
        """Return every scope/value for one exact key under the shared lock.

        Pending semantic approvals live in requester-owned scopes. The guild
        steward queue may locate them by opaque review ID, but callers must
        still filter each decoded record by its server-stamped catalog scope.
        """

        with self._lock:
            rows = self._conn.execute(
                "SELECT scope, value FROM kv WHERE key = ? ORDER BY scope",
                (key,),
            ).fetchall()
        return [(str(row["scope"]), str(row["value"])) for row in rows]


# -- Session (de)serialization ------------------------------------------


def _serialize_session(session: Session) -> dict[str, Any]:
    ident = session.identity
    return {
        "identity": {
            "user_id": ident.user_id,
            "guild_id": ident.guild_id,
            "channel_id": ident.channel_id,
            "thread_id": ident.thread_id,
            "is_admin": ident.is_admin,
        },
        "transcript": [_serialize_message(m) for m in session.transcript],
        "source_id": session.source_id,
        "connection_generation": session.connection_generation,
    }


def _deserialize_session(data: dict[str, Any]) -> Session:
    ident_data = data["identity"]
    identity = Identity(
        user_id=ident_data["user_id"],
        guild_id=ident_data.get("guild_id"),
        channel_id=ident_data.get("channel_id"),
        thread_id=ident_data.get("thread_id"),
        is_admin=ident_data.get("is_admin", False),
    )
    transcript = [_deserialize_message(m) for m in data.get("transcript", [])]
    return Session(
        identity=identity,
        transcript=transcript,
        source_id=str(data.get("source_id", "")),
        connection_generation=int(data.get("connection_generation", 0)),
    )


def _serialize_message(m: Message) -> dict[str, Any]:
    return {
        "role": m.role.value,
        "content": m.content,
        "tool_calls": [
            {"id": tc.id, "name": tc.name, "arguments": tc.arguments}
            for tc in m.tool_calls
        ],
        "tool_call_id": m.tool_call_id,
        "name": m.name,
        "transient": m.transient,
    }


def _deserialize_message(data: dict[str, Any]) -> Message:
    return Message(
        role=Role(data["role"]),
        content=data.get("content", ""),
        tool_calls=[
            ToolCall(id=tc["id"], name=tc["name"], arguments=tc.get("arguments", {}))
            for tc in data.get("tool_calls", [])
        ],
        tool_call_id=data.get("tool_call_id"),
        name=data.get("name"),
        transient=bool(data.get("transient", False)),
    )
