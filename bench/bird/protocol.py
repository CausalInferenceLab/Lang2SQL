"""Immutable settings for BIRD evidence and product-revision comparisons."""

from __future__ import annotations

import platform
import random
import re
import sqlite3
import subprocess
from dataclasses import asdict, dataclass
from importlib.metadata import PackageNotFoundError, distributions, version
from pathlib import Path

from .io import ProtocolError, digest, json_digest

BENCH_DIR = Path(__file__).resolve().parent
BENCH_ROOT = BENCH_DIR.parents[1]
CONDITIONS = ("B", "D")
REQUIRED_TOOLS = ("run_sql", "explore_schema")
OPTIONAL_TOOLS = ("review_sql",)
EVIDENCE_LABEL = "BIRD evidence"
INSTRUCTION = (
    "\n\nNo follow-up user is available. Give the best supported answer for the "
    "question. Return your final answer as exactly one fenced SQL block containing "
    "a single read-only SQLite SELECT/WITH query. Exclude intermediate SQL."
)
SEED = 20261114
TRANSPORT = "litellm:acompletion"
API_BASE = "https://api.openai.com/v1"
PROFILES = {
    "openai-gpt41-mini": {
        "model": "openai/gpt-4.1-mini-2025-04-14",
        "response_model": "gpt-4.1-mini-2025-04-14",
        "openai": "3.51.0",
    },
    "openai-gpt41": {
        "model": "openai/gpt-4.1-2025-04-14",
        "response_model": "gpt-4.1-2025-04-14",
        "openai": "3.51.0",
    },
    "openai-gpt6-luna": {
        "model": "openai/gpt-6-luna",
        "response_model": "gpt-6-luna",
        "openai": "3.57.0",
        "reasoning_effort": "none",
    },
}


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


def question_order(manifest: dict) -> list[int]:
    ids = [case["question_id"] for case in manifest["cases"]]
    random.Random(SEED).shuffle(ids)
    return ids


def user_text(question: str, evidence: str, condition: str) -> str:
    attachment = f"\n\n{EVIDENCE_LABEL}:\n{evidence}" if condition == "B" else ""
    return question + attachment + INSTRUCTION


def git(root: Path, *args: str) -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(root), *args], text=True, stderr=subprocess.PIPE
        ).strip()
    except subprocess.CalledProcessError:
        raise ProtocolError(f"Could not verify Git checkout at {root}") from None


def verify_checkout(root: Path, commit: str, *paths: str) -> None:
    if (
        not re.fullmatch(r"[0-9a-f]{40}", commit)
        or git(root, "rev-parse", "HEAD") != commit
    ):
        raise ProtocolError(
            f"Checkout must be at the specified full commit SHA: {root}"
        )
    if Path(git(root, "rev-parse", "--show-toplevel")).resolve() != root:
        raise ProtocolError(f"Expected a Git worktree root: {root}")
    if git(root, "diff", "--name-only", commit, "--", *paths) or git(
        root, "ls-files", "--others", "--exclude-standard", "--", *paths
    ):
        raise ProtocolError(
            f"Checkout has uncommitted benchmark or product inputs: {root}"
        )


def benchmark_commit() -> str:
    commit = git(BENCH_ROOT, "rev-parse", "HEAD")
    verify_checkout(BENCH_ROOT, commit, "bench/bird")
    return commit


def product_source(root: Path, commit: str) -> Path:
    verify_checkout(root, commit, "src", "pyproject.toml", "uv.lock")
    source = root / "src"
    if not (source / "lang2sql" / "__init__.py").is_file():
        raise ProtocolError(f"Lang2SQL source is missing in {root}")
    return source


def runtime(profile: dict, *, smoke: bool) -> dict:
    expected = {
        "sqlalchemy": "2.0.50",
        "cryptography": "45.0.6",
        "PyYAML": "6.0.3",
    }
    if not smoke:
        expected.update(litellm="1.109.1", openai=profile["openai"])
    actual = {
        "python": platform.python_version(),
        "sqlite": sqlite3.sqlite_version,
    }
    required = {"python": "3.13.14", "sqlite": "3.53.1", **expected}
    for package in expected:
        try:
            actual[package] = version(package)
        except PackageNotFoundError:
            actual[package] = "missing"
    mismatches = [
        f"{key}={actual[key]} (expected {expected_version})"
        for key, expected_version in required.items()
        if actual[key] != expected_version
    ]
    if mismatches:
        raise ProtocolError("Benchmark runtime mismatch: " + ", ".join(mismatches))
    actual["installed_packages"] = {
        item.metadata["Name"].lower(): item.version for item in distributions()
    }
    return actual


def scoring_settings(budgets: Budgets = BUDGETS) -> dict:
    return {
        "sql_timeout": budgets.sql_timeout,
        "max_rows": budgets.max_rows,
        "sqlite_version": sqlite3.sqlite_version,
        "scoring_sha256": digest(BENCH_DIR / "scoring.py"),
        "connection_sha256": digest(BENCH_DIR / "sqlite.py"),
    }


def fixed_settings(manifest: dict) -> dict:
    smoke = manifest["smoke_cases"] is not None
    return {
        "format_version": 1,
        "mode": "scripted-smoke" if smoke else "benchmark",
        "transport": "scripted" if smoke else TRANSPORT,
        "manifest_digest": json_digest(manifest),
        "lock_sha256": digest(BENCH_DIR / "uv.lock"),
        "provider_options": {"api_base": API_BASE, "num_retries": 0},
        "budgets": asdict(BUDGETS),
        "scoring": scoring_settings(),
        "seed": SEED,
        "question_order": question_order(manifest),
        "tool_policy": {
            "required": list(REQUIRED_TOOLS),
            "optional": list(OPTIONAL_TOOLS),
        },
        "instruction": INSTRUCTION,
        "evidence_label": EVIDENCE_LABEL,
    }


def settings(
    manifest: dict,
    *,
    condition: str,
    profile_name: str,
    product_commit: str,
    product_tree: Path,
    commit: str,
) -> dict:
    profile = PROFILES[profile_name]
    return {
        **fixed_settings(manifest),
        "condition": condition,
        "profile": {"name": profile_name, **profile},
        "product_commit": product_commit,
        "product_lock_sha256": digest(product_tree / "uv.lock"),
        "benchmark_commit": commit,
        "runtime": runtime(profile, smoke=manifest["smoke_cases"] is not None),
    }


def check_settings(manifest: dict, snapshot: dict, commit: str) -> None:
    if any(
        snapshot.get(key) != value for key, value in fixed_settings(manifest).items()
    ):
        raise ProtocolError("Settings do not match this manifest or benchmark protocol")
    if snapshot.get("benchmark_commit") != commit:
        raise ProtocolError("Run was created with a different benchmark commit")
    profile = snapshot["profile"]
    name = profile["name"]
    if (
        snapshot.get("condition") not in CONDITIONS
        or name not in PROFILES
        or profile != {"name": name, **PROFILES[name]}
    ):
        raise ProtocolError("Run has an unknown condition or changed model profile")
