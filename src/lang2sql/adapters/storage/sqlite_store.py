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
import time
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
        self._create_tables()

    def _create_tables(self) -> None:
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
        self._conn.close()

    # -- AuditPort -------------------------------------------------------

    async def record(self, event: AuditEvent) -> None:
        ts = event.ts or time.time()
        self._conn.execute(
            "INSERT INTO audit (actor, action, scope, detail, ts) VALUES (?, ?, ?, ?, ?)",
            (event.actor, event.action, event.scope, json.dumps(event.detail), ts),
        )
        self._conn.commit()

    async def query(self, actor: str, limit: int = 20) -> list[AuditEvent]:
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
        row = self._conn.execute(
            "SELECT data FROM sessions WHERE key = ?", (key,)
        ).fetchone()
        if row is None:
            return None
        return _deserialize_session(json.loads(row["data"]))

    async def save(self, key: str, session: Session) -> None:
        data = json.dumps(_serialize_session(session))
        self._conn.execute(
            "INSERT INTO sessions (key, data) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET data = excluded.data",
            (key, data),
        )
        self._conn.commit()

    # -- generic key-value (wrapped by the secrets adapter) --------------

    def kv_get(self, scope: str, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM kv WHERE scope = ? AND key = ?", (scope, key)
        ).fetchone()
        return row["value"] if row else None

    def kv_set(self, scope: str, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT INTO kv (scope, key, value) VALUES (?, ?, ?) "
            "ON CONFLICT(scope, key) DO UPDATE SET value = excluded.value",
            (scope, key, value),
        )
        self._conn.commit()

    def kv_delete(self, scope: str, key: str) -> None:
        self._conn.execute("DELETE FROM kv WHERE scope = ? AND key = ?", (scope, key))
        self._conn.commit()

    @staticmethod
    def _escape_like(s: str) -> str:
        return s.replace("!", "!!").replace("%", "!%").replace("_", "!_")

    def kv_delete_prefix(self, scope: str, prefix: str) -> int:
        """Delete all keys under scope that start with prefix. Returns count deleted."""
        cur = self._conn.execute(
            "DELETE FROM kv WHERE scope = ? AND key LIKE ? ESCAPE '!'",
            (scope, self._escape_like(prefix) + "%"),
        )
        self._conn.commit()
        return cur.rowcount

    def kv_list_prefix(self, scope: str, prefix: str) -> list[tuple[str, str]]:
        """Return (key, value) pairs for all keys under scope that start with prefix."""
        rows = self._conn.execute(
            "SELECT key, value FROM kv WHERE scope = ? AND key LIKE ? ESCAPE '!' ORDER BY key",
            (scope, self._escape_like(prefix) + "%"),
        ).fetchall()
        return [(r["key"], r["value"]) for r in rows]


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
    return Session(identity=identity, transcript=transcript)


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
    )
