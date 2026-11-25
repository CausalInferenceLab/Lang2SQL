"""Freeze BIRD SQLite inputs and keep reference SQL out of inference."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path

from .io import ProtocolError, digest, read_json, write_json, write_text

COHORT_PATH = Path(__file__).with_name("manifest-sqlite100.json")
SAMPLE_SIZE = 100
SMOKE_LIMIT = 5
EX_REFERENCE = (
    "https://github.com/bird-bench/mini_dev/blob/"
    "1c49a375e76c471e218564da3f4617513d33e371/evaluation/evaluation_ex.py#L17"
)


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


def sample_cases(cases: list[Case], seed: int, count: int) -> list[Case]:
    """Assign proportional quotas by DB and difficulty, then rank IDs per stratum."""
    strata: dict[tuple[str, str], list[Case]] = defaultdict(list)
    for case in cases:
        strata[case.db_id, case.difficulty].append(case)
    shares = {
        key: divmod(count * len(members), len(cases)) for key, members in strata.items()
    }
    quotas = {key: whole for key, (whole, _) in shares.items()}
    remaining = count - sum(quotas.values())
    for key in sorted(shares, key=lambda key: (-shares[key][1], key))[:remaining]:
        quotas[key] += 1

    def rank(case: Case) -> tuple[bytes, int]:
        return (
            hashlib.sha256(f"{seed}:{case.question_id}".encode("ascii")).digest(),
            case.question_id,
        )

    selected = [
        case
        for key in sorted(strata)
        for case in sorted(strata[key], key=rank)[: quotas[key]]
    ]
    return sorted(selected, key=lambda case: case.question_id)


def read_source(source: Path, cohort: dict) -> list[dict]:
    raw = source.read_bytes()
    if hashlib.sha256(raw).hexdigest() != cohort["source_sha256"]:
        raise ProtocolError("Annotation file differs from the frozen BIRD revision")
    rows = [json.loads(line) for line in raw.splitlines()]
    if len(rows) != cohort["source_count"] or len(
        {r["question_id"] for r in rows}
    ) != len(rows):
        raise ProtocolError("Annotation count or question IDs do not match the cohort")
    return rows


def project(row: dict) -> Case:
    return Case(
        question_id=row["question_id"],
        db_id=row["db_id"],
        difficulty=row["difficulty"],
        question=row["question"],
        evidence=row["evidence"],
    )


def selected_metadata(cohort: dict, smoke_cases: int | None) -> list[dict]:
    cases = cohort["cases"]
    if smoke_cases is None:
        return cases
    if type(smoke_cases) is not int or not 1 <= smoke_cases <= SMOKE_LIMIT:
        raise ProtocolError(f"Smoke cases must be between 1 and {SMOKE_LIMIT}")
    # Round robin across databases so even a short smoke run opens different DBs.
    first_by_db = {}
    for case in cases:
        first_by_db.setdefault(case["db_id"], case)
    return sorted(
        list(first_by_db.values())[:smoke_cases], key=lambda case: case["question_id"]
    )


def database_path(directory: Path, db_id: str) -> Path:
    filename = f"{db_id}.sqlite"
    candidates = (directory / db_id / filename, directory / filename)
    files = {path.resolve() for path in candidates if path.is_file()}
    if len(files) != 1:
        raise ProtocolError(f"Expected one SQLite file for {db_id}")
    db = files.pop()
    for suffix in ("-wal", "-journal"):
        sidecar = db.with_name(db.name + suffix)
        if sidecar.exists() and sidecar.stat().st_size:
            raise ProtocolError(
                f"SQLite snapshot has an active sidecar: {db_id}{suffix}"
            )
    return db


def verify_databases(db_dir: Path, manifest: dict) -> dict[str, Path]:
    databases = {}
    for db_id, checksum in manifest["database_sha256"].items():
        db = database_path(db_dir, db_id)
        if digest(db) != checksum:
            raise ProtocolError(
                f"SQLite snapshot differs from the pinned bytes for {db_id}"
            )
        databases[db_id] = db
    return databases


def manifest_metadata(cohort: dict, smoke_cases: int | None) -> dict:
    cases = selected_metadata(cohort, smoke_cases)
    return {
        "format_version": 1,
        "cohort_sha256": digest(COHORT_PATH),
        "smoke_cases": smoke_cases,
        "database_revision": cohort["database_revision"],
        "database_sha256": {
            db_id: cohort["database_sha256"][db_id]
            for db_id in sorted({case["db_id"] for case in cases})
        },
        "cases": cases,
        "ex_reference": EX_REFERENCE,
    }


def prepare(
    source: Path, db_dir: Path, output: Path, smoke_cases: int | None = None
) -> dict:
    cohort = read_json(COHORT_PATH)
    eligible = [
        project(row) for row in read_source(source, cohort) if row["evidence"].strip()
    ]
    sample = sample_cases(eligible, cohort["seed"], SAMPLE_SIZE)
    if (
        len(eligible) != cohort["eligible_count"]
        or [case.metadata() for case in sample] != cohort["cases"]
    ):
        raise ProtocolError("Source does not reproduce the frozen 100-question sample")

    manifest = manifest_metadata(cohort, smoke_cases)
    verify_databases(db_dir, manifest)
    by_id = {case.question_id: case for case in sample}
    inference = "".join(
        json.dumps(asdict(by_id[item["question_id"]]), ensure_ascii=False) + "\n"
        for item in manifest["cases"]
    )
    manifest["cases_sha256"] = hashlib.sha256(inference.encode("utf-8")).hexdigest()
    output.mkdir(parents=True, exist_ok=False)
    write_text(output / "cases.jsonl", inference)
    write_json(output / "manifest.json", manifest)
    return manifest


def load_manifest(directory: Path) -> dict:
    manifest = read_json(directory / "manifest.json")
    expected = manifest_metadata(read_json(COHORT_PATH), manifest["smoke_cases"])
    if any(manifest.get(key) != value for key, value in expected.items()):
        raise ProtocolError("Manifest differs from the frozen cohort or EX protocol")
    return manifest


def load_cases(directory: Path, manifest: dict) -> list[Case]:
    data = (directory / "cases.jsonl").read_bytes()
    if hashlib.sha256(data).hexdigest() != manifest["cases_sha256"]:
        raise ProtocolError("Inference input hash does not match the manifest")
    cases = [Case(**json.loads(line)) for line in data.splitlines()]
    if [case.metadata() for case in cases] != manifest["cases"]:
        raise ProtocolError("Inference metadata does not match the manifest")
    return cases


def gold_rows(source: Path, manifest: dict) -> list[dict]:
    cohort = read_json(COHORT_PATH)
    by_id = {row["question_id"]: row for row in read_source(source, cohort)}
    rows = [by_id[item["question_id"]] for item in manifest["cases"]]
    if [project(row).metadata() for row in rows] != manifest["cases"]:
        raise ProtocolError("Gold question/database metadata differs from the manifest")
    return rows
