"""Stable fingerprints and atomic benchmark artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from contextlib import contextmanager
from pathlib import Path


class ProtocolError(ValueError):
    """The inputs or runtime do not match the recorded evaluation protocol."""


def digest(path: Path) -> str:
    with path.open("rb") as source:
        checksum = hashlib.sha256()
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            checksum.update(chunk)
    return checksum.hexdigest()


def json_digest(value) -> str:
    data = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=path.parent, mode="w", encoding="utf-8", newline="", delete=False
    ) as temporary:
        tmp = Path(temporary.name)
        try:
            temporary.write(text)
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary.close()
            tmp.replace(path)
        finally:
            tmp.unlink(missing_ok=True)


def write_json(path: Path, value) -> None:
    write_text(path, json.dumps(value, ensure_ascii=False, indent=2) + "\n")


@contextmanager
def lock_output(directory: Path):
    """Let SQLite release the exclusive writer slot even if the process dies."""
    connection = sqlite3.connect(directory / ".writer.sqlite", timeout=0)
    try:
        try:
            connection.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as error:
            if "locked" not in str(error):
                raise
            raise ProtocolError(f"Output is already in use: {directory}") from None
        yield
    finally:
        connection.close()
