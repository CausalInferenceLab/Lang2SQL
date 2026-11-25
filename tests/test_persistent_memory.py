import pytest

from lang2sql.adapters.storage.persistent_memory import PersistentMemoryStore
from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.core.ports.memory import Fact


@pytest.mark.asyncio
async def test_restart_100_notes_and_identity_boundaries(tmp_path):
    path = str(tmp_path / "memory.db")
    store = SqliteStore(path)
    memory = PersistentMemoryStore(store, "org", "team")
    for i in range(100):
        await memory.add(Fact(str(i), "user", f"note {i}", ts=i))
    store.close()
    store = SqliteStore(path)
    try:
        notes = await PersistentMemoryStore(store, "org", "team").all("user")
        assert [f.text for f in notes] == [f"note {i}" for i in range(100)]
        assert not await PersistentMemoryStore(store, "other", "team").all("user")
        assert not await PersistentMemoryStore(store, "org", "other").all("user")
        assert not await PersistentMemoryStore(store, "org", "team").all("other")
    finally:
        store.close()
