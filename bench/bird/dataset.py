"""Freeze the BIRD cohort and project records for inference."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path

from bench.common.io import ProtocolError, digest, read_json, write_json, write_text

COHORT_PATH = Path(__file__).with_name("manifest-sqlite365.json")
TITLE = "BIRD Mini-Dev SQLite에서 evidence가 제공된 365문항"
EX_REFERENCE = {
    "repository": "bird-bench/mini_dev",
    "commit": "1c49a375e76c471e218564da3f4617513d33e371",
    "path": "evaluation/evaluation_ex.py",
    "blob_sha": "45cc3bb8cb6edf039e6a8a2897b7b5f06aa23970",
    "function": "calculate_ex",
}


@dataclass(frozen=True)
class Case:
    question_id: int
    db_id: str
    difficulty: str
    question: str
    evidence: str

    def metadata(self) -> dict:
        return {
            "question_id": self.question_id,
            "db_id": self.db_id,
            "difficulty": self.difficulty,
        }


def frozen_cohort() -> dict:
    return read_json(COHORT_PATH)


def source_rows(source: Path, cohort: dict) -> list[dict]:
    raw = source.read_bytes()
    if hashlib.sha256(raw).hexdigest() != cohort["source_sha256"]:
        raise ProtocolError("Annotation file differs from the frozen BIRD revision")
    rows = [json.loads(line) for line in raw.splitlines()]
    ids = [row["question_id"] for row in rows]
    if len(ids) != cohort["source_count"] or len(set(ids)) != len(ids):
        raise ProtocolError("Annotation count or question IDs do not match the cohort")
    selected = sorted(
        (row for row in rows if row["evidence"].strip()),
        key=lambda row: row["question_id"],
    )
    if [row["question_id"] for row in selected] != cohort["question_ids"]:
        raise ProtocolError("Evidence selection does not match the frozen question IDs")
    if sorted({row["db_id"] for row in selected}) != cohort["db_ids"]:
        raise ProtocolError("Database IDs do not match the frozen cohort")
    return selected


def project(row: dict) -> Case:
    return Case(
        question_id=row["question_id"],
        db_id=row["db_id"],
        difficulty=row["difficulty"],
        question=row["question"],
        evidence=row["evidence"],
    )


def database_path(directory: Path, db_id: str) -> Path:
    filename = f"{db_id}.sqlite"
    candidates = (directory / db_id / filename, directory / filename)
    files = {path.resolve() for path in candidates if path.is_file()}
    if len(files) != 1:
        raise ProtocolError(f"Expected one SQLite database file for {db_id}")
    db = files.pop()
    for suffix in ("-wal", "-journal"):
        sidecar = db.with_name(db.name + suffix)
        if sidecar.exists() and sidecar.stat().st_size:
            raise ProtocolError(
                f"SQLite snapshot has an active sidecar: {db_id}{suffix}"
            )
    return db


def prepare(source: Path, db_dir: Path, db_revision: str, output: Path) -> dict:
    cohort = frozen_cohort()
    cases = [project(row) for row in source_rows(source, cohort)]
    db_hashes = {
        db_id: digest(database_path(db_dir, db_id)) for db_id in cohort["db_ids"]
    }
    if not db_revision.strip():
        raise ProtocolError("A database snapshot revision is required")
    projected = "".join(
        json.dumps(asdict(case), ensure_ascii=False) + "\n" for case in cases
    )
    manifest = {
        "format_version": 1,
        "title": TITLE,
        "cohort": cohort,
        "database_revision": db_revision,
        "database_sha256": db_hashes,
        "cases": [case.metadata() for case in cases],
        "cases_sha256": hashlib.sha256(projected.encode("utf-8")).hexdigest(),
        "ex_reference": EX_REFERENCE,
    }
    output.mkdir(parents=True, exist_ok=False)
    write_text(output / "cases.jsonl", projected)
    write_json(output / "manifest.json", manifest)
    return manifest


def load_manifest(directory: Path) -> dict:
    manifest = read_json(directory / "manifest.json")
    cohort = frozen_cohort()
    if (
        manifest["format_version"] != 1
        or manifest["cohort"] != cohort
        or manifest["ex_reference"] != EX_REFERENCE
        or [c["question_id"] for c in manifest["cases"]] != cohort["question_ids"]
        or sorted(manifest["database_sha256"]) != cohort["db_ids"]
        or sorted({c["db_id"] for c in manifest["cases"]}) != cohort["db_ids"]
    ):
        raise ProtocolError("Manifest differs from the frozen cohort or EX protocol")
    return manifest


def load_cases(directory: Path, manifest: dict) -> list[Case]:
    data = (directory / "cases.jsonl").read_bytes()
    if hashlib.sha256(data).hexdigest() != manifest["cases_sha256"]:
        raise ProtocolError("Inference input hash does not match the manifest")
    cases = [Case(**json.loads(line)) for line in data.splitlines()]
    if [case.metadata() for case in cases] != manifest["cases"]:
        raise ProtocolError("Inference question metadata does not match the manifest")
    return cases


def verify_databases(db_dir: Path, manifest: dict) -> dict[str, Path]:
    paths = {}
    for db_id, checksum in manifest["database_sha256"].items():
        db = database_path(db_dir, db_id)
        if digest(db) != checksum:
            raise ProtocolError(f"SQLite snapshot hash changed: {db_id}")
        paths[db_id] = db
    return paths
