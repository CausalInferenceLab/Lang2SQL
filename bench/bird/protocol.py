"""Settings for an independent condition and model profile."""

from __future__ import annotations

import platform
import random
import re
import sqlite3
import subprocess
from dataclasses import asdict, dataclass
from importlib.metadata import version
from pathlib import Path

import lang2sql
from bench.common.io import ProtocolError, digest, json_digest, read_json

REPO_ROOT = Path(__file__).resolve().parents[2]
BASELINE = "e0a7fb197558dd8845377ab9fd95222d595e8978"
CONDITIONS = ("A", "D", "B")
DEFAULT_PROFILES = Path(__file__).with_name("models.example.json")
TOOLS = ("run_sql", "explore_schema")
SEED = 20261114
EVIDENCE_LABEL = "BIRD evidence"
INSTRUCTION = (
    "\n\nNo follow-up user is available. Give the best supported answer for the "
    "question. Return your final answer as exactly one fenced SQL block containing "
    "a single read-only SQLite SELECT/WITH query. Exclude intermediate SQL."
)


@dataclass(frozen=True)
class Budgets:
    max_calls: int = 8
    max_output_tokens: int = 4096
    call_timeout: float = 120.0
    case_timeout: float = 480.0
    sql_timeout: float = 30.0
    max_rows: int = 100_000
    concurrency: int = 4
    pilot_questions: int = 6


BUDGETS = Budgets()


def question_order(question_ids: list[int], seed: int = SEED) -> list[int]:
    ids = sorted(question_ids)
    random.Random(seed).shuffle(ids)
    return ids


def user_text(question: str, evidence: str, condition: str) -> str:
    attachment = f"\n\n{EVIDENCE_LABEL}:\n{evidence}" if condition == "B" else ""
    return question + attachment + INSTRUCTION


def load_profile(path: Path, name: str) -> dict[str, str]:
    source = read_json(path)
    if not isinstance(source, list):
        raise ProtocolError("Model profiles must be a JSON array")
    profiles = {}
    required = ("name", "model", "api_key_env")
    for item in source:
        if not isinstance(item, dict) or any(
            not isinstance(item.get(key), str) or not item[key].strip()
            for key in required
        ):
            raise ProtocolError(
                "Each model profile requires name, model and api_key_env"
            )
        profile = {key: item[key] for key in required}
        if not re.fullmatch(r"[A-Za-z0-9_-]+", profile["name"]):
            raise ProtocolError("Invalid model profile name")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", profile["api_key_env"]):
            raise ProtocolError("Invalid API key environment variable name")
        if "api_base" in item:
            if not isinstance(item["api_base"], str) or not item["api_base"].strip():
                raise ProtocolError("Model profile api_base must be a nonempty string")
            profile["api_base"] = item["api_base"]
        if profile["name"] in profiles:
            raise ProtocolError("Duplicate model profile name")
        profiles[profile["name"]] = profile
    if name not in profiles:
        raise ProtocolError(
            f"Unknown profile {name!r}. Available: {', '.join(profiles)}"
        )
    return profiles[name]


def git(*args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(REPO_ROOT), *args], text=True
    ).strip()


def verify_product() -> None:
    if git("diff", "--name-only", BASELINE, "--", "src/") or git(
        "ls-files", "--others", "--exclude-standard", "--", "src/"
    ):
        raise ProtocolError(f"Product source differs from {BASELINE}")
    expected = REPO_ROOT / "src/lang2sql/__init__.py"
    if Path(lang2sql.__file__).resolve() != expected:
        raise ProtocolError("Imported Lang2SQL does not come from this worktree")


def scoring_settings(budgets: Budgets) -> dict:
    return {
        "sql_timeout": budgets.sql_timeout,
        "max_rows": budgets.max_rows,
        "sqlite_version": sqlite3.sqlite_version,
        "scoring_sha256": digest(REPO_ROOT / "bench/bird/scoring.py"),
        "connection_sha256": digest(REPO_ROOT / "bench/common/sqlite.py"),
    }


def settings(
    manifest: dict,
    budgets: Budgets,
    provider_version: str,
    *,
    condition: str,
    profile: dict[str, str],
    mode: str = "live",
) -> dict:
    source_paths = sorted(
        path
        for folder in ("bench/bird", "bench/common")
        for path in (REPO_ROOT / folder).glob("*.py")
    )
    return {
        "format_version": 2,
        "mode": mode,
        "condition": condition,
        "profile": profile,
        "manifest_digest": json_digest(manifest),
        "product_commit": BASELINE,
        "git_head": git("rev-parse", "HEAD"),
        "code_sha256": {
            path.relative_to(REPO_ROOT).as_posix(): digest(path)
            for path in source_paths
        },
        "lock_sha256": digest(REPO_ROOT / "uv.lock"),
        "runtime": {
            "python": platform.python_version(),
            "sqlite": sqlite3.sqlite_version,
            "sqlalchemy": version("sqlalchemy"),
            "litellm": provider_version,
        },
        "provider_options": {"num_retries": 0},
        "budgets": asdict(budgets),
        "scoring": scoring_settings(budgets),
        "seed": SEED,
        "question_order": question_order(manifest["cohort"]["question_ids"]),
        "tools": list(TOOLS),
        "instruction": INSTRUCTION,
        "evidence_label": EVIDENCE_LABEL,
        "term_custom": {"layer": "team", "inferred": False},
    }


def check_settings(manifest: dict, snapshot: dict) -> None:
    if snapshot.get("format_version") != 2 or snapshot["condition"] not in CONDITIONS:
        raise ProtocolError(
            "Expected settings for one BIRD condition and model profile"
        )
    if snapshot["manifest_digest"] != json_digest(manifest):
        raise ProtocolError("Settings belong to a different manifest")
    if snapshot["question_order"] != question_order(
        manifest["cohort"]["question_ids"], snapshot["seed"]
    ):
        raise ProtocolError("Settings do not contain the deterministic question order")
