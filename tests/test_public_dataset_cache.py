"""Offline contract tests for the public cross-domain dataset cache."""

from __future__ import annotations

import hashlib
import importlib.util
from io import BytesIO
import json
from pathlib import Path
import sqlite3
import zipfile

import pytest

_ROOT = Path(__file__).resolve().parent.parent
_MODULE_PATH = _ROOT / "bench" / "dataset_cache.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("dataset_cache", _MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


cache = _load_module()


def _one_csv_manifest(payload_limit: int = 1024) -> dict:
    return {
        "version": 1,
        "database_count": 1,
        "split_counts": {"dev": 1, "holdout": 0},
        "datasets": [
            {
                "dataset_id": "fixture",
                "domain": "test",
                "topology_family": "flat_event",
                "dialect": "sqlite",
                "split": "dev",
                "official_page": "https://example.test/catalog",
                "license": "test fixture",
                "download": {
                    "url": "https://example.test/data.csv",
                    "format": "csv",
                    "filename": "fixture.csv",
                    "max_bytes": payload_limit,
                    "checksum_policy": "lock_on_first_fetch",
                },
                "materialization": {
                    "kind": "csv_to_sqlite",
                    "table_name": "fixture_rows",
                    "max_rows": 100,
                },
            }
        ],
    }


def test_public_manifest_is_21_database_sqlite_holdout_contract():
    manifest = cache.load_manifest()
    databases = cache.declared_databases(manifest)

    assert len(manifest["datasets"]) == 11
    assert len(databases) == 21
    assert sum(item["split"] == "dev" for item in databases) == 11
    assert sum(item["split"] == "holdout" for item in databases) == 10
    assert {item["dialect"] for item in databases} == {"sqlite"}
    assert len({item["db_id"] for item in databases}) == 21
    assert all(
        entry["official_page"].startswith("https://")
        and entry["download"]["url"].startswith("https://")
        for entry in manifest["datasets"]
    )


def test_fetch_records_hash_and_requires_explicit_refresh_on_drift(tmp_path):
    manifest = _one_csv_manifest()
    first = b"id,value\n1,2\n"
    second = b"id,value\n1,3\n"

    record = cache.fetch_dataset(
        manifest, "fixture", tmp_path, opener=lambda _url: BytesIO(first)
    )
    assert record["sha256"] == hashlib.sha256(first).hexdigest()
    assert record["byte_size"] == len(first)
    lock = json.loads((tmp_path / cache.LOCK_FILENAME).read_text())
    assert lock["datasets"]["fixture"]["sha256"] == record["sha256"]

    source = tmp_path / "sources" / "fixture.csv"
    source.write_bytes(second)
    with pytest.raises(cache.DatasetCacheError, match="source drift"):
        cache.fetch_dataset(manifest, "fixture", tmp_path)

    refreshed = cache.fetch_dataset(
        manifest,
        "fixture",
        tmp_path,
        refresh=True,
        opener=lambda _url: BytesIO(second),
    )
    assert refreshed["sha256"] == hashlib.sha256(second).hexdigest()


def test_fetch_enforces_maximum_bytes_without_partial_cache(tmp_path):
    manifest = _one_csv_manifest(payload_limit=3)
    with pytest.raises(cache.DatasetCacheError, match="exceeds max_bytes"):
        cache.fetch_dataset(
            manifest, "fixture", tmp_path, opener=lambda _url: BytesIO(b"four")
        )
    assert not (tmp_path / "sources" / "fixture.csv").exists()


def test_zip_extraction_is_selective_and_rejects_traversal(tmp_path):
    safe_archive = tmp_path / "safe.zip"
    with zipfile.ZipFile(safe_archive, "w") as bundle:
        bundle.writestr("root/a/a.sqlite", b"sqlite-a")
        bundle.writestr("root/a/readme.txt", b"not extracted")
    extracted = cache.extract_sqlite_members(
        safe_archive, tmp_path / "safe", "root/*/*.sqlite"
    )
    assert [path.name for path in extracted] == ["a.sqlite"]
    assert not (tmp_path / "safe" / "root" / "a" / "readme.txt").exists()

    unsafe_archive = tmp_path / "unsafe.zip"
    with zipfile.ZipFile(unsafe_archive, "w") as bundle:
        bundle.writestr("../escape.sqlite", b"escape")
        bundle.writestr("root/a/a.sqlite", b"sqlite-a")
    with pytest.raises(cache.DatasetCacheError, match="unsafe ZIP member"):
        cache.extract_sqlite_members(
            unsafe_archive, tmp_path / "unsafe", "root/*/*.sqlite"
        )


def test_csv_materialization_is_generic_null_safe_and_has_no_synthetic_pk(tmp_path):
    source = tmp_path / "fixture.csv"
    source.write_text(
        "Order ID,order-id,Postal Code,Value,Empty\n1,2,00123,1.5,\n2,3,00456,2.0,\n",
        encoding="utf-8",
    )
    database = cache.materialize_csv(
        source, tmp_path / "fixture.sqlite", "fixture rows", 100
    )

    with sqlite3.connect(database) as connection:
        columns = connection.execute('PRAGMA table_info("fixture rows")').fetchall()
        rows = connection.execute('SELECT * FROM "fixture rows" ORDER BY 1').fetchall()
    assert [column[1] for column in columns] == [
        "order_id",
        "order_id_2",
        "postal_code",
        "value",
        "empty",
    ]
    assert [column[2] for column in columns] == [
        "INTEGER",
        "INTEGER",
        "TEXT",
        "REAL",
        "TEXT",
    ]
    assert all(column[5] == 0 for column in columns)
    assert rows[0] == (1, 2, "00123", 1.5, None)


def test_production_does_not_import_benchmark_or_embed_dataset_ids():
    manifest = cache.load_manifest()
    dataset_ids = {entry["dataset_id"] for entry in manifest["datasets"]}
    for path in (_ROOT / "src" / "lang2sql").rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert "import bench" not in text
        assert "from bench" not in text
        assert all(dataset_id not in text for dataset_id in dataset_ids)
