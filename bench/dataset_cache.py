#!/usr/bin/env python3
"""Reproducible public-dataset cache used only by the benchmark harness.

This module intentionally stays outside ``src/lang2sql``. It downloads public
fixtures, records immutable observations, and materializes CSV files as SQLite
without adding semantic hints or a synthetic primary key.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import fnmatch
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import sqlite3
import stat
import tempfile
from typing import Any, Callable, Mapping, Sequence
import unicodedata
from urllib.request import urlopen
import zipfile

import yaml

DEFAULT_MANIFEST = Path(__file__).parent / "datasets" / "public_lang2sql_domains.yaml"
DEFAULT_CACHE_ROOT = Path("lang2sql-datasets/cache")
LOCK_FILENAME = "public_lang2sql_domains.lock.json"
_CHUNK_SIZE = 1024 * 1024
_INTEGER_RE = re.compile(r"^[+-]?(?:0|[1-9][0-9]*)$")
_REAL_RE = re.compile(
    r"^[+-]?(?:(?:0|[1-9][0-9]*)\.[0-9]+|(?:0|[1-9][0-9]*)[eE][+-]?[0-9]+|"
    r"(?:0|[1-9][0-9]*)\.[0-9]+[eE][+-]?[0-9]+)$"
)


class DatasetCacheError(RuntimeError):
    """An explicit reproducibility, validation, or materialization failure."""


def load_manifest(path: Path = DEFAULT_MANIFEST) -> dict[str, Any]:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise DatasetCacheError(f"cannot load manifest {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise DatasetCacheError("manifest root must be a mapping")
    validate_manifest(raw)
    return raw


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise DatasetCacheError(f"{label} must be a mapping")
    return value


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DatasetCacheError(f"{label} must be non-empty text")
    return value


def declared_databases(manifest: Mapping[str, Any]) -> list[dict[str, str]]:
    """Expand all entries to immutable database-level split records."""

    records: list[dict[str, str]] = []
    for entry in manifest.get("datasets", []):
        materialization = _mapping(
            entry.get("materialization"), f"{entry.get('dataset_id')}.materialization"
        )
        common = {
            "dataset_id": str(entry["dataset_id"]),
            "domain": str(entry["domain"]),
            "topology_family": str(entry["topology_family"]),
            "dialect": str(entry["dialect"]),
        }
        if materialization.get("kind") == "bird_sqlite_bundle":
            databases = materialization.get("databases")
            if not isinstance(databases, list):
                raise DatasetCacheError(
                    f"{entry['dataset_id']}.materialization.databases must be a list"
                )
            for database in databases:
                db = _mapping(database, f"{entry['dataset_id']}.database")
                records.append(
                    {
                        **common,
                        "db_id": str(db.get("db_id", "")),
                        "split": str(db.get("split", "")),
                    }
                )
        else:
            records.append(
                {
                    **common,
                    "db_id": str(entry["dataset_id"]),
                    "split": str(entry.get("split", "")),
                }
            )
    return records


def validate_manifest(manifest: Mapping[str, Any]) -> None:
    """Validate source metadata and the DB-level holdout contract."""

    if manifest.get("version") != 1:
        raise DatasetCacheError("manifest version must be 1")
    datasets = manifest.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        raise DatasetCacheError("manifest.datasets must be a non-empty list")
    dataset_ids: set[str] = set()
    filenames: set[str] = set()
    for index, entry_raw in enumerate(datasets):
        entry = _mapping(entry_raw, f"datasets[{index}]")
        dataset_id = _text(entry.get("dataset_id"), "dataset_id")
        if dataset_id in dataset_ids:
            raise DatasetCacheError(f"duplicate dataset_id: {dataset_id}")
        dataset_ids.add(dataset_id)
        for field in ("domain", "topology_family", "official_page", "license"):
            _text(entry.get(field), f"{dataset_id}.{field}")
        if entry.get("dialect") != "sqlite":
            raise DatasetCacheError(f"{dataset_id}.dialect must be sqlite")
        if not str(entry["official_page"]).startswith("https://"):
            raise DatasetCacheError(f"{dataset_id}.official_page must use HTTPS")

        download = _mapping(entry.get("download"), f"{dataset_id}.download")
        url = _text(download.get("url"), f"{dataset_id}.download.url")
        if not url.startswith("https://"):
            raise DatasetCacheError(f"{dataset_id}.download.url must use HTTPS")
        file_format = download.get("format")
        if file_format not in {"csv", "zip"}:
            raise DatasetCacheError(f"{dataset_id}.download.format must be csv or zip")
        filename = _text(download.get("filename"), f"{dataset_id}.download.filename")
        if filename in filenames:
            raise DatasetCacheError(f"duplicate download filename: {filename}")
        filenames.add(filename)
        max_bytes = download.get("max_bytes")
        if not isinstance(max_bytes, int) or max_bytes <= 0:
            raise DatasetCacheError(f"{dataset_id}.download.max_bytes must be positive")
        policy = download.get("checksum_policy")
        if policy not in {"required", "lock_on_first_fetch"}:
            raise DatasetCacheError(f"invalid checksum policy for {dataset_id}")
        expected_sha = download.get("sha256")
        if policy == "required":
            if not isinstance(expected_sha, str) or not re.fullmatch(
                r"[0-9a-f]{64}", expected_sha
            ):
                raise DatasetCacheError(f"{dataset_id} requires a SHA-256 checksum")
        elif expected_sha is not None:
            raise DatasetCacheError(
                f"{dataset_id} lock_on_first_fetch must not embed a mutable checksum"
            )

        materialization = _mapping(
            entry.get("materialization"), f"{dataset_id}.materialization"
        )
        kind = materialization.get("kind")
        if kind == "csv_to_sqlite":
            if file_format != "csv":
                raise DatasetCacheError(
                    f"{dataset_id} CSV materialization needs CSV input"
                )
            if entry.get("split") not in {"dev", "holdout"}:
                raise DatasetCacheError(f"{dataset_id}.split must be dev or holdout")
            _text(materialization.get("table_name"), f"{dataset_id}.table_name")
            max_rows = materialization.get("max_rows")
            if not isinstance(max_rows, int) or max_rows <= 0:
                raise DatasetCacheError(f"{dataset_id}.max_rows must be positive")
        elif kind == "bird_sqlite_bundle":
            if file_format != "zip":
                raise DatasetCacheError(
                    f"{dataset_id} bundle materialization needs ZIP"
                )
            _text(materialization.get("database_glob"), f"{dataset_id}.database_glob")
            databases = materialization.get("databases")
            if not isinstance(databases, list) or not databases:
                raise DatasetCacheError(f"{dataset_id}.databases must be non-empty")
            local_db_ids: set[str] = set()
            for db_raw in databases:
                db = _mapping(db_raw, f"{dataset_id}.database")
                db_id = _text(db.get("db_id"), f"{dataset_id}.db_id")
                if db_id in local_db_ids:
                    raise DatasetCacheError(f"duplicate BIRD db_id: {db_id}")
                local_db_ids.add(db_id)
                if db.get("split") not in {"dev", "holdout"}:
                    raise DatasetCacheError(f"{dataset_id}.{db_id}.split is invalid")
        else:
            raise DatasetCacheError(
                f"unsupported materialization kind for {dataset_id}"
            )

    records = declared_databases(manifest)
    db_ids = [record["db_id"] for record in records]
    if any(not db_id for db_id in db_ids) or len(db_ids) != len(set(db_ids)):
        raise DatasetCacheError("database IDs must be non-empty and globally unique")
    if manifest.get("database_count") != len(records):
        raise DatasetCacheError("database_count does not match declared databases")
    split_counts = {
        split: sum(record["split"] == split for record in records)
        for split in ("dev", "holdout")
    }
    if manifest.get("split_counts") != split_counts:
        raise DatasetCacheError(
            f"split_counts={manifest.get('split_counts')!r} does not match {split_counts}"
        )


def _dataset_by_id(manifest: Mapping[str, Any], dataset_id: str) -> Mapping[str, Any]:
    for entry in manifest["datasets"]:
        if entry["dataset_id"] == dataset_id:
            return entry
    raise DatasetCacheError(f"unknown dataset_id: {dataset_id}")


def _sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(_CHUNK_SIZE):
                digest.update(chunk)
                size += len(chunk)
    except OSError as exc:
        raise DatasetCacheError(f"cannot hash {path}: {exc}") from exc
    return digest.hexdigest(), size


def _load_lock(cache_root: Path) -> dict[str, Any]:
    path = cache_root / LOCK_FILENAME
    if not path.exists():
        return {"version": 1, "datasets": {}}
    try:
        lock = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DatasetCacheError(f"cannot read cache lock {path}: {exc}") from exc
    if not isinstance(lock, dict) or lock.get("version") != 1:
        raise DatasetCacheError(f"unsupported cache lock format: {path}")
    if not isinstance(lock.get("datasets"), dict):
        raise DatasetCacheError(f"cache lock datasets must be a mapping: {path}")
    return lock


def _write_lock(cache_root: Path, lock: Mapping[str, Any]) -> None:
    cache_root.mkdir(parents=True, exist_ok=True)
    target = cache_root / LOCK_FILENAME
    fd, temporary_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=cache_root)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(lock, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, target)
    except Exception:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def _download(
    url: str,
    destination: Path,
    max_bytes: int,
    expected_sha256: str | None,
    opener: Callable[[str], Any],
) -> tuple[str, int]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", dir=destination.parent
    )
    digest = hashlib.sha256()
    size = 0
    try:
        with os.fdopen(fd, "wb") as output, opener(url) as response:
            while chunk := response.read(_CHUNK_SIZE):
                size += len(chunk)
                if size > max_bytes:
                    raise DatasetCacheError(
                        f"download exceeds max_bytes={max_bytes}: {url}"
                    )
                digest.update(chunk)
                output.write(chunk)
            output.flush()
            os.fsync(output.fileno())
        observed_sha = digest.hexdigest()
        if expected_sha256 is not None and observed_sha != expected_sha256:
            raise DatasetCacheError(
                f"SHA-256 mismatch for {url}: expected {expected_sha256}, "
                f"observed {observed_sha}"
            )
        os.replace(temporary_name, destination)
        return observed_sha, size
    except Exception:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def fetch_dataset(
    manifest: Mapping[str, Any],
    dataset_id: str,
    cache_root: Path = DEFAULT_CACHE_ROOT,
    *,
    refresh: bool = False,
    opener: Callable[[str], Any] = urlopen,
) -> dict[str, Any]:
    """Fetch or lock one source without accepting unnoticed source drift."""

    entry = _dataset_by_id(manifest, dataset_id)
    download = entry["download"]
    destination = cache_root / "sources" / download["filename"]
    lock = _load_lock(cache_root)
    locked = lock["datasets"].get(dataset_id)
    expected_sha = download.get("sha256")
    if destination.exists() and not refresh:
        observed_sha, size = _sha256_file(destination)
        if size > download["max_bytes"]:
            raise DatasetCacheError(f"cached {dataset_id} exceeds max_bytes")
        if expected_sha is not None and observed_sha != expected_sha:
            raise DatasetCacheError(f"cached SHA-256 mismatch for {dataset_id}")
        if locked is not None:
            if (
                locked.get("sha256") != observed_sha
                or locked.get("url") != download["url"]
            ):
                raise DatasetCacheError(
                    f"cached source drift for {dataset_id}; use --refresh explicitly"
                )
            return dict(locked)
    else:
        observed_sha, size = _download(
            download["url"],
            destination,
            int(download["max_bytes"]),
            expected_sha,
            opener,
        )
        if locked is not None and locked.get("sha256") != observed_sha and not refresh:
            raise DatasetCacheError(
                f"source drift for {dataset_id}; use --refresh explicitly"
            )
    record = {
        "url": download["url"],
        "filename": download["filename"],
        "byte_size": size,
        "sha256": observed_sha,
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
    }
    lock["datasets"][dataset_id] = record
    _write_lock(cache_root, lock)
    return record


def fetch_all(
    manifest: Mapping[str, Any],
    cache_root: Path = DEFAULT_CACHE_ROOT,
    *,
    refresh: bool = False,
    opener: Callable[[str], Any] = urlopen,
) -> dict[str, dict[str, Any]]:
    return {
        entry["dataset_id"]: fetch_dataset(
            manifest, entry["dataset_id"], cache_root, refresh=refresh, opener=opener
        )
        for entry in manifest["datasets"]
    }


def _safe_zip_member(name: str) -> bool:
    path = PurePosixPath(name)
    return bool(name) and not path.is_absolute() and ".." not in path.parts


def extract_sqlite_members(
    archive: Path,
    destination: Path,
    pattern: str,
    *,
    refresh: bool = False,
) -> list[Path]:
    """Safely extract only SQLite members matching the declared bundle glob."""

    destination.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(archive) as bundle:
            for info in bundle.infolist():
                if not _safe_zip_member(info.filename):
                    raise DatasetCacheError(
                        f"unsafe ZIP member in {archive}: {info.filename!r}"
                    )
                mode = info.external_attr >> 16
                if stat.S_ISLNK(mode):
                    raise DatasetCacheError(
                        f"ZIP symlink is not allowed in {archive}: {info.filename!r}"
                    )
            selected = [
                info
                for info in bundle.infolist()
                if not info.is_dir()
                and info.filename.lower().endswith(".sqlite")
                and fnmatch.fnmatch(info.filename, pattern)
            ]
            if not selected:
                raise DatasetCacheError(
                    f"no SQLite members match {pattern!r} in {archive}"
                )
            extracted: list[Path] = []
            root = destination.resolve()
            for info in selected:
                target = destination / PurePosixPath(info.filename)
                resolved_target = target.resolve()
                if root not in resolved_target.parents:
                    raise DatasetCacheError(
                        f"ZIP target escapes destination: {info.filename}"
                    )
                target.parent.mkdir(parents=True, exist_ok=True)
                if target.exists() and not refresh:
                    extracted.append(target)
                    continue
                fd, temporary_name = tempfile.mkstemp(
                    prefix=f".{target.name}.", dir=target.parent
                )
                try:
                    with os.fdopen(fd, "wb") as output, bundle.open(info) as source:
                        shutil.copyfileobj(source, output, _CHUNK_SIZE)
                        output.flush()
                        os.fsync(output.fileno())
                    os.replace(temporary_name, target)
                except Exception:
                    try:
                        os.unlink(temporary_name)
                    except FileNotFoundError:
                        pass
                    raise
                extracted.append(target)
            return extracted
    except (OSError, zipfile.BadZipFile) as exc:
        raise DatasetCacheError(f"cannot extract {archive}: {exc}") from exc


def normalize_headers(headers: Sequence[str]) -> list[str]:
    """Normalize headers deterministically while preserving collisions."""

    normalized: list[str] = []
    counts: dict[str, int] = {}
    for index, header in enumerate(headers, start=1):
        text = unicodedata.normalize("NFKC", header).strip().lower()
        base = "".join(character if character.isalnum() else "_" for character in text)
        base = re.sub(r"_+", "_", base).strip("_")
        if not base:
            base = f"column_{index}"
        if base[0].isdigit():
            base = f"column_{base}"
        counts[base] = counts.get(base, 0) + 1
        normalized.append(base if counts[base] == 1 else f"{base}_{counts[base]}")
    return normalized


def _cell_kind(value: str) -> str | None:
    value = value.strip()
    if not value:
        return None
    if _INTEGER_RE.fullmatch(value):
        unsigned = value.lstrip("+-")
        if unsigned == "0" or not unsigned.startswith("0"):
            return "INTEGER"
    if _REAL_RE.fullmatch(value):
        return "REAL"
    return "TEXT"


def infer_sqlite_types(rows: Sequence[Sequence[str]], width: int) -> list[str]:
    types = ["INTEGER"] * width
    seen = [False] * width
    for row in rows:
        for index, value in enumerate(row):
            kind = _cell_kind(value)
            if kind is None:
                continue
            seen[index] = True
            if kind == "TEXT":
                types[index] = "TEXT"
            elif kind == "REAL" and types[index] == "INTEGER":
                types[index] = "REAL"
    return [
        data_type if seen[index] else "TEXT" for index, data_type in enumerate(types)
    ]


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _convert_cell(value: str, data_type: str) -> Any:
    stripped = value.strip()
    if not stripped:
        return None
    if data_type == "INTEGER":
        return int(stripped)
    if data_type == "REAL":
        return float(stripped)
    return value


def materialize_csv(
    source: Path,
    destination: Path,
    table_name: str,
    max_rows: int,
    *,
    refresh: bool = False,
) -> Path:
    """Materialize a bounded CSV without inventing keys or semantic metadata."""

    if destination.exists() and not refresh:
        return destination
    try:
        with source.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            raw_headers = next(reader, None)
            if not raw_headers:
                raise DatasetCacheError(f"CSV has no header: {source}")
            headers = normalize_headers(raw_headers)
            rows: list[list[str]] = []
            for row_number, row in enumerate(reader, start=2):
                if len(rows) >= max_rows:
                    break
                if len(row) > len(headers):
                    raise DatasetCacheError(
                        f"CSV row {row_number} has {len(row)} fields; expected {len(headers)}"
                    )
                rows.append(row + [""] * (len(headers) - len(row)))
    except (OSError, csv.Error, UnicodeDecodeError) as exc:
        raise DatasetCacheError(f"cannot read CSV {source}: {exc}") from exc

    data_types = infer_sqlite_types(rows, len(headers))
    columns_sql = ", ".join(
        f"{_quote_identifier(name)} {data_type}"
        for name, data_type in zip(headers, data_types)
    )
    placeholders = ", ".join("?" for _ in headers)
    create_sql = f"CREATE TABLE {_quote_identifier(table_name)} ({columns_sql})"
    insert_sql = f"INSERT INTO {_quote_identifier(table_name)} VALUES ({placeholders})"
    converted_rows = [
        tuple(
            _convert_cell(value, data_types[index]) for index, value in enumerate(row)
        )
        for row in rows
    ]

    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".sqlite", dir=destination.parent
    )
    os.close(fd)
    try:
        with sqlite3.connect(temporary_name) as connection:
            connection.execute(create_sql)
            if converted_rows:
                connection.executemany(insert_sql, converted_rows)
            connection.commit()
        os.replace(temporary_name, destination)
    except Exception:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise
    return destination


def materialize_dataset(
    manifest: Mapping[str, Any],
    dataset_id: str,
    cache_root: Path = DEFAULT_CACHE_ROOT,
    *,
    refresh: bool = False,
) -> list[Path]:
    entry = _dataset_by_id(manifest, dataset_id)
    source = cache_root / "sources" / entry["download"]["filename"]
    if not source.exists():
        raise DatasetCacheError(f"source not fetched for {dataset_id}: {source}")
    materialization = entry["materialization"]
    kind = materialization["kind"]
    if kind == "bird_sqlite_bundle":
        extracted = extract_sqlite_members(
            source,
            cache_root / "materialized" / dataset_id,
            materialization["database_glob"],
            refresh=refresh,
        )
        declared = {db["db_id"] for db in materialization["databases"]}
        observed = {path.stem for path in extracted}
        if observed != declared:
            raise DatasetCacheError(
                f"BIRD DB inventory mismatch: declared={sorted(declared)}, "
                f"observed={sorted(observed)}"
            )
        return extracted
    destination = cache_root / "materialized" / f"{dataset_id}.sqlite"
    return [
        materialize_csv(
            source,
            destination,
            materialization["table_name"],
            int(materialization["max_rows"]),
            refresh=refresh,
        )
    ]


def materialize_all(
    manifest: Mapping[str, Any],
    cache_root: Path = DEFAULT_CACHE_ROOT,
    *,
    refresh: bool = False,
) -> dict[str, list[str]]:
    return {
        entry["dataset_id"]: [
            str(path)
            for path in materialize_dataset(
                manifest, entry["dataset_id"], cache_root, refresh=refresh
            )
        ]
        for entry in manifest["datasets"]
    }


def _database_path(
    cache_root: Path, entry: Mapping[str, Any], db_id: str
) -> Path | None:
    materialization = entry["materialization"]
    if materialization["kind"] == "csv_to_sqlite":
        path = cache_root / "materialized" / f"{entry['dataset_id']}.sqlite"
        return path if path.exists() else None
    root = cache_root / "materialized" / entry["dataset_id"]
    matches = list(root.rglob(f"{db_id}.sqlite")) if root.exists() else []
    if len(matches) > 1:
        raise DatasetCacheError(f"multiple materialized databases for {db_id}")
    return matches[0] if matches else None


def _sqlite_inventory(path: Path) -> dict[str, Any]:
    digest, size = _sha256_file(path)
    tables: list[dict[str, Any]] = []
    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as connection:
            names = [
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
                )
            ]
            for name in names:
                quoted = _quote_identifier(name)
                column_count = len(
                    connection.execute(f"PRAGMA table_info({quoted})").fetchall()
                )
                row_count = int(
                    connection.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()[0]
                )
                tables.append(
                    {"table": name, "columns": column_count, "rows": row_count}
                )
    except sqlite3.Error as exc:
        raise DatasetCacheError(
            f"cannot inventory SQLite database {path}: {exc}"
        ) from exc
    return {"path": str(path), "byte_size": size, "sha256": digest, "tables": tables}


def inventory(
    manifest: Mapping[str, Any],
    cache_root: Path = DEFAULT_CACHE_ROOT,
    *,
    inspect_materialized: bool = False,
) -> dict[str, Any]:
    records = declared_databases(manifest)
    by_id = {entry["dataset_id"]: entry for entry in manifest["datasets"]}
    output: list[dict[str, Any]] = []
    for record in records:
        item: dict[str, Any] = dict(record)
        path = _database_path(cache_root, by_id[record["dataset_id"]], record["db_id"])
        item["materialized"] = path is not None
        if path is not None:
            item["path"] = str(path)
            if inspect_materialized:
                item["sqlite"] = _sqlite_inventory(path)
        output.append(item)
    return {
        "database_count": len(output),
        "split_counts": {
            split: sum(item["split"] == split for item in output)
            for split in ("dev", "holdout")
        },
        "materialized_count": sum(bool(item["materialized"]) for item in output),
        "databases": output,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--cache-root", type=Path, default=DEFAULT_CACHE_ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("validate")
    fetch = subparsers.add_parser("fetch")
    fetch.add_argument("dataset_id")
    fetch.add_argument("--refresh", action="store_true")
    fetch_all_parser = subparsers.add_parser("fetch-all")
    fetch_all_parser.add_argument("--refresh", action="store_true")
    materialize = subparsers.add_parser("materialize")
    materialize.add_argument("dataset_id")
    materialize.add_argument("--refresh", action="store_true")
    materialize_all_parser = subparsers.add_parser("materialize-all")
    materialize_all_parser.add_argument("--refresh", action="store_true")
    inventory_parser = subparsers.add_parser("inventory")
    inventory_parser.add_argument("--inspect", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    manifest = load_manifest(args.manifest)
    if args.command == "validate":
        result: Any = inventory(manifest, args.cache_root)
    elif args.command == "fetch":
        result = fetch_dataset(
            manifest, args.dataset_id, args.cache_root, refresh=args.refresh
        )
    elif args.command == "fetch-all":
        result = fetch_all(manifest, args.cache_root, refresh=args.refresh)
    elif args.command == "materialize":
        result = [
            str(path)
            for path in materialize_dataset(
                manifest, args.dataset_id, args.cache_root, refresh=args.refresh
            )
        ]
    elif args.command == "materialize-all":
        result = materialize_all(manifest, args.cache_root, refresh=args.refresh)
    else:
        result = inventory(manifest, args.cache_root, inspect_materialized=args.inspect)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
