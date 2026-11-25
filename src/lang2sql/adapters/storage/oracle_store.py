"""Oracle-backed metadata KV and audit, separate from the read-only business account."""

from __future__ import annotations

import asyncio
import json
import time
import uuid

from sqlalchemy import (
    Column,
    Float,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    func,
    select,
)
from sqlalchemy.exc import IntegrityError

from ...core.ports.audit import AuditEvent


class OracleStore:
    def __init__(self, url: str, *, initialize: bool = False):
        self._engine = create_engine(url, hide_parameters=True)
        if self._engine.dialect.name != "oracle":
            self._engine.dispose()
            raise ValueError("Metadata URL must use an Oracle dialect")
        metadata = MetaData()
        self._kv = Table(
            "l2s_metadata",
            metadata,
            Column("scope", String(512), primary_key=True),
            Column("key", String(512), primary_key=True),
            Column("value", Text, nullable=False),
        )
        self._audit = Table(
            "l2s_audit",
            metadata,
            Column("id", String(32), primary_key=True),
            Column("actor", String(512), nullable=False),
            Column("action", String(128), nullable=False),
            Column("scope", String(512), nullable=False),
            Column("detail", Text, nullable=False),
            Column("ts", Float, nullable=False),
        )
        if initialize:
            metadata.create_all(self._engine)

    def close(self):
        self._engine.dispose()

    def _where(self, scope, key):
        return (self._kv.c.scope == scope) & (self._kv.c.key == key)

    def kv_get(self, scope, key):
        with self._engine.connect() as conn:
            row = conn.execute(
                select(self._kv.c.value).where(self._where(scope, key))
            ).one_or_none()
            return (row[0] or "") if row is not None else None

    def kv_compare_replace(self, scope, key, expected, value):
        stored_value = func.empty_clob() if value == "" else value
        try:
            with self._engine.begin() as conn:
                if expected is None:
                    conn.execute(
                        self._kv.insert().values(
                            scope=scope, key=key, value=stored_value
                        )
                    )
                    return True
                row = conn.execute(
                    select(self._kv.c.value)
                    .where(self._where(scope, key))
                    .with_for_update()
                ).one_or_none()
                current = (row[0] or "") if row is not None else None
                if current != expected:
                    return False
                conn.execute(
                    self._kv.update()
                    .where(self._where(scope, key))
                    .values(value=stored_value)
                )
                return True
        except IntegrityError as exc:
            # Only a competing insert is an expected optimistic concurrency conflict.
            if expected is None and getattr(exc.orig.args[0], "code", None) == 1:
                return False
            raise

    def kv_set(self, scope, key, value):
        expected = self.kv_get(scope, key)
        if not self.kv_compare_replace(scope, key, expected, value):
            raise RuntimeError("Concurrent metadata update; reload before retrying")

    def kv_delete(self, scope, key):
        with self._engine.begin() as conn:
            return (
                conn.execute(self._kv.delete().where(self._where(scope, key))).rowcount
                > 0
            )

    def _prefix(self, scope, prefix):
        escaped = prefix.replace("!", "!!").replace("%", "!%").replace("_", "!_")
        return (self._kv.c.scope == scope) & self._kv.c.key.like(
            escaped + "%", escape="!"
        )

    def kv_list_prefix(self, scope, prefix):
        with self._engine.connect() as conn:
            return [
                (row[0], row[1] or "")
                for row in conn.execute(
                    select(self._kv.c.key, self._kv.c.value)
                    .where(self._prefix(scope, prefix))
                    .order_by(self._kv.c.key)
                )
            ]

    def kv_delete_prefix(self, scope, prefix):
        with self._engine.begin() as conn:
            return conn.execute(
                self._kv.delete().where(self._prefix(scope, prefix))
            ).rowcount

    def _record(self, event):
        with self._engine.begin() as conn:
            conn.execute(
                self._audit.insert().values(
                    id=uuid.uuid4().hex,
                    actor=event.actor,
                    action=event.action,
                    scope=event.scope,
                    detail=json.dumps(event.detail, ensure_ascii=False),
                    ts=event.ts or time.time(),
                )
            )

    async def record(self, event):
        await asyncio.to_thread(self._record, event)

    def _query(self, actor, limit):
        with self._engine.connect() as conn:
            rows = conn.execute(
                select(self._audit)
                .where(self._audit.c.actor == actor)
                .order_by(self._audit.c.ts.desc())
                .limit(limit)
            ).mappings()
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

    async def query(self, actor, limit=20):
        return await asyncio.to_thread(self._query, actor, limit)
