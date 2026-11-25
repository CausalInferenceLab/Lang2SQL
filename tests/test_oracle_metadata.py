"""Optional integration test against a dedicated writable Oracle metadata account."""

import os
import uuid

import pytest

from lang2sql.adapters.storage.oracle_store import OracleStore
from lang2sql.core.ports.audit import AuditEvent


@pytest.mark.asyncio
async def test_oracle_metadata_roundtrip():
    url = os.environ.get("LANG2SQL_TEST_METADATA_URL")
    if not url:
        pytest.skip("Dedicated Oracle metadata URL not configured")
    scope = uuid.uuid4().hex
    store = OracleStore(url, initialize=True)
    try:
        store.kv_set(scope, "empty", "")
        assert store.kv_get(scope, "empty") == ""
        assert store.kv_compare_replace(scope, "empty", "", "filled")
        assert store.kv_compare_replace(scope, "empty", "filled", "")
        assert store.kv_get(scope, "empty") == ""
        value = '{"definition":"순매출","note":"' + "가" * 5000 + '"}'
        store.kv_set(scope, "percent_%", value)
        assert store.kv_get(scope, "percent_%") == value
        assert store.kv_list_prefix(scope, "percent_%") == [("percent_%", value)]
        assert not store.kv_compare_replace(scope, "percent_%", None, "wrong")
        assert not store.kv_compare_replace(scope, "percent_%", "stale", "wrong")
        assert store.kv_compare_replace(scope, "percent_%", value, "new")
        store.close()
        store = OracleStore(url)
        assert store.kv_get(scope, "percent_%") == "new"
        await store.record(
            AuditEvent(
                actor=scope, action="verified", scope=scope, detail={"한글": True}
            )
        )
        assert (await store.query(scope))[0].detail == {"한글": True}
        assert store.kv_delete_prefix(scope, "percent_%") == 1
    finally:
        store.kv_delete_prefix(scope, "")
        store.close()
