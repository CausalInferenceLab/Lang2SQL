"""Pin, run and officially score Spider 2.0-Lite SQLite with unchanged Lang2SQL."""

from __future__ import annotations
import argparse
import asyncio
import csv
from collections import Counter
import hashlib
import importlib.util
import json
import logging
import os
from pathlib import Path
import random
import re
import sqlite3
import subprocess
import sys
import time
from dataclasses import asdict

from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer
from lang2sql.core.identity import Identity
from lang2sql.harness.loop import agent_loop
from lang2sql.tenancy.concierge import ContextConcierge
from .providers import MeasuredLLM

logger = logging.getLogger(__name__)
UPSTREAM_COMMIT = "cafb867313aab4e674652054198f383cf4018943"
BASELINE_COMMIT = "e0a7fb1"
INSTRUCTION = "\nReturn your final answer as exactly one fenced ```sql block containing the single read-only SQLite SELECT/WITH query that answers the question. Do not include exploratory SQL in the final answer. Use the supplied documentation; no user simulator is available."


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    tmp.replace(path)


def prepare(root: Path, db_dir: Path, output: Path, limit: int, seed: int):
    commit = subprocess.check_output(
        ["git", "-C", str(root), "rev-parse", "HEAD"], text=True
    ).strip()
    if commit != UPSTREAM_COMMIT:
        raise ValueError("Spider2 checkout does not match pinned commit")
    lite = root / "spider2-lite"
    cases = [
        json.loads(s) for s in (lite / "spider2-lite.jsonl").read_text().splitlines()
    ]
    cases = sorted(
        [c for c in cases if c["instance_id"].startswith("local")],
        key=lambda c: c["instance_id"],
    )
    random.Random(seed).shuffle(cases)
    if limit:
        cases = cases[:limit]
    hashes = {}
    gold_hashes = {}
    for case in cases:
        db = db_dir / (case["db"] + ".sqlite")
        if not db.is_file():
            raise FileNotFoundError(db)
        if case["db"] not in hashes:
            hashes[case["db"]] = digest(db)
        ident = case["instance_id"]
        gold_files = [
            f
            for f in (lite / "evaluation_suite/gold/exec_result").glob(ident + "*.csv")
            if f.stem == ident or re.fullmatch(re.escape(ident) + "_[a-z]", f.stem)
        ]
        if not gold_files:
            raise ValueError("Missing official gold: " + ident)
        gold_hashes.update({f.name: digest(f) for f in gold_files})
        doc = case.get("external_knowledge")
        case["document_sha256"] = (
            digest(lite / "resource/documents" / doc) if doc else None
        )
    write_json(
        output,
        dict(
            benchmark="Spider 2.0-Lite SQLite subset",
            upstream_commit=commit,
            baseline_commit=BASELINE_COMMIT,
            seed=seed,
            selection="seeded shuffle of local IDs before inference",
            question_file_sha256=digest(lite / "spider2-lite.jsonl"),
            evaluator_sha256=digest(lite / "evaluation_suite/evaluate.py"),
            db_sha256=hashes,
            gold_sha256=gold_hashes,
            gold_config_sha256=digest(
                lite / "evaluation_suite/gold/spider2lite_eval.jsonl"
            ),
            cases=cases,
        ),
    )


def final_sql(answer: str) -> str:
    blocks = re.findall(r"```sql\s*\n?(.*?)```", answer, re.I | re.S)
    if len(blocks) != 1:
        raise ValueError(
            "Final answer must contain exactly one SQL block; no last-tool fallback"
        )
    sql = blocks[0].strip()
    if not re.match(r"^(SELECT|WITH)\b", sql, re.I):
        raise ValueError("Final SQL is not SELECT/WITH")
    return sql


def read_connection(db: Path, deadline: float):
    conn = sqlite3.connect(
        db.resolve().as_uri() + "?mode=ro", uri=True, check_same_thread=False
    )
    conn.execute("PRAGMA query_only=ON")

    def authorize(action, *args):
        if action in (sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH):
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    conn.set_authorizer(authorize)
    conn.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
    return conn


def export_result(db: Path, sql: str, output: Path, timeout: float, max_rows: int):
    conn = read_connection(db, time.monotonic() + timeout)
    try:
        cursor = conn.execute(sql)
        if cursor.description is None:
            raise ValueError("Query returned no columns")
        with output.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([c[0] for c in cursor.description])
            count = 0
            for row in cursor:
                count += 1
                if count > max_rows:
                    raise ValueError(
                        "Result exceeds configured row budget; not silently truncated"
                    )
                writer.writerow(row)
        return count
    except Exception:
        output.unlink(missing_ok=True)
        raise
    finally:
        conn.close()


async def run_case(case, root, db_dir, directory, profile, key, args):
    from sqlalchemy import event

    started = time.monotonic()
    llm = MeasuredLLM(profile, key, args.call_timeout, args.max_calls)
    db = db_dir / (case["db"] + ".sqlite")
    explorer = SqlAlchemyExplorer("sqlite://")
    # Existing adapter/harness, but benchmark DB connections are physically read-only.
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool

    explorer._engine = create_engine(
        "sqlite://",
        poolclass=NullPool,
        creator=lambda: read_connection(db, time.monotonic() + args.sql_timeout),
    )

    @event.listens_for(explorer._engine, "before_cursor_execute")
    def deadline(connection, cursor, statement, parameters, context, executemany):
        until = time.monotonic() + args.sql_timeout
        cursor.connection.set_progress_handler(
            lambda: int(time.monotonic() > until), 1000
        )

    concierge = ContextConcierge(llm=llm, explorer=explorer, max_turns=args.max_calls)
    ctx = await concierge.build_context(
        Identity(
            user_id="benchmark", guild_id=case["instance_id"], channel_id="baseline"
        )
    )
    prompt = case["question"]
    if case.get("external_knowledge"):
        prompt += (
            "\n\nProvided benchmark documentation:\n"
            + (
                root / "spider2-lite/resource/documents" / case["external_knowledge"]
            ).read_text()
        )
    prompt += INSTRUCTION
    record = dict(
        instance_id=case["instance_id"],
        profile=profile["name"],
        model=profile["model"],
        status="started",
    )
    try:
        answer = await asyncio.wait_for(agent_loop(ctx, prompt), args.case_timeout)
        record["answer"] = answer
        sql = final_sql(answer)
        (directory / "sql" / f"{case['instance_id']}.sql").write_text(sql + "\n")
        record["rows"] = await asyncio.to_thread(
            export_result,
            db,
            sql,
            directory / "csv" / f"{case['instance_id']}.csv",
            args.sql_timeout,
            args.max_rows,
        )
        record.update(status="predicted", sql=sql)
    except Exception as exc:
        # Error text from providers may contain request secrets. Persist type only.
        record.update(status="failed", error_type=type(exc).__name__)
        logger.error(
            "case failed model=%s id=%s error=%s",
            profile["name"],
            case["instance_id"],
            type(exc).__name__,
        )
    finally:
        record.update(
            seconds=time.monotonic() - started,
            calls=llm.calls,
            transcript=[asdict(m) for m in ctx.session.history()],
        )
        write_json(directory / "records" / f"{case['instance_id']}.json", record)
        concierge.store.close()
        await asyncio.to_thread(explorer._engine.dispose)
    logger.info(
        "case finished model=%s id=%s status=%s",
        profile["name"],
        case["instance_id"],
        record["status"],
    )


def validate_manifest(manifest, root, db_dir):
    lite = root / "spider2-lite"
    if digest(lite / "spider2-lite.jsonl") != manifest["question_file_sha256"]:
        raise ValueError("Question file changed")
    if digest(lite / "evaluation_suite/evaluate.py") != manifest["evaluator_sha256"]:
        raise ValueError("Official evaluator changed")
    for db, sha in manifest["db_sha256"].items():
        if digest(db_dir / (db + ".sqlite")) != sha:
            raise ValueError("Database hash changed: " + db)
    questions = {
        c["instance_id"]: c
        for c in map(json.loads, (lite / "spider2-lite.jsonl").read_text().splitlines())
    }
    ids = [c["instance_id"] for c in manifest["cases"]]
    if not ids or len(set(ids)) != len(ids):
        raise ValueError("Manifest cases must be nonempty and unique")
    for c in manifest["cases"]:
        original = questions.get(c["instance_id"], {})
        if not c["instance_id"].startswith("local") or any(
            c.get(k) != original.get(k)
            for k in ("question", "db", "external_knowledge")
        ):
            raise ValueError("Manifest case differs from upstream")
        if (
            c.get("external_knowledge")
            and digest(lite / "resource/documents" / c["external_knowledge"])
            != c["document_sha256"]
        ):
            raise ValueError("Document hash changed")


async def run(args, env: dict[str, str]):
    manifest = json.loads(args.manifest.read_text())
    profiles = json.loads(args.profiles.read_text())
    names = [p["name"] for p in profiles]
    if len(set(names)) != len(names):
        raise ValueError("Duplicate model profile names")
    if subprocess.check_output(["git", "diff", BASELINE_COMMIT, "--", "src"]):
        raise ValueError("Product source differs from the pinned baseline")
    validate_manifest(manifest, args.root, args.db_dir)
    if args.output.exists():
        raise ValueError(
            "Output directory already exists; use a new run ID to prevent mixed results"
        )
    args.output.mkdir(parents=True)
    write_json(args.output / "manifest.json", manifest)
    write_json(
        args.output / "settings.json",
        dict(
            profiles=profiles,
            max_calls=args.max_calls,
            call_timeout=args.call_timeout,
            case_timeout=args.case_timeout,
            sql_timeout=args.sql_timeout,
            max_rows=args.max_rows,
            concurrency=args.concurrency,
            instruction=INSTRUCTION,
            max_output_tokens=4096,
            runner_sha256=digest(Path(__file__)),
            provider_sha256=digest(Path(__file__).with_name("providers.py")),
            git_commit=subprocess.check_output(
                ["git", "rev-parse", "HEAD"], text=True
            ).strip(),
        ),
    )
    for profile in profiles:
        if not re.fullmatch(r"[A-Za-z0-9_-]+", profile["name"]):
            raise ValueError("Unsafe profile name")
        directory = args.output / profile["name"]
        for sub in ("sql", "csv", "records"):
            (directory / sub).mkdir(parents=True)
        key = env.get(profile["api_key_env"])
        if not key:
            write_json(
                directory / "status.json",
                dict(status="blocked_missing_key", env=profile["api_key_env"]),
            )
            logger.error(
                "profile blocked missing key name=%s env=%s",
                profile["name"],
                profile["api_key_env"],
            )
            continue
        semaphore = asyncio.Semaphore(args.concurrency)

        async def bounded(case):
            async with semaphore:
                await run_case(
                    case, args.root, args.db_dir, directory, profile, key, args
                )

        model_started = time.monotonic()
        await asyncio.gather(*(bounded(case) for case in manifest["cases"]))
        write_json(
            directory / "status.json",
            dict(status="completed", wall_seconds=time.monotonic() - model_started),
        )


def score(root: Path, output: Path):
    manifest = json.loads((output / "manifest.json").read_text())
    settings = json.loads((output / "settings.json").read_text())
    script = root / "spider2-lite/evaluation_suite/evaluate.py"
    if digest(script) != manifest["evaluator_sha256"]:
        raise ValueError("Official evaluator hash mismatch")
    spec = importlib.util.spec_from_file_location("spider2_official", script)
    official = importlib.util.module_from_spec(spec)
    oldout, olderr = sys.stdout, sys.stderr
    oldcwd = Path.cwd()
    try:
        os.chdir(output)
        spec.loader.exec_module(official)
        tee = sys.stdout
    finally:
        sys.stdout, sys.stderr = oldout, olderr
        os.chdir(oldcwd)
    tee.close()
    gold = script.parent / "gold"
    for name, expected in manifest.get("gold_sha256", {}).items():
        if digest(gold / "exec_result" / name) != expected:
            raise ValueError("Gold result changed: " + name)
    if (
        manifest.get("gold_config_sha256")
        and digest(gold / "spider2lite_eval.jsonl") != manifest["gold_config_sha256"]
    ):
        raise ValueError("Gold evaluation settings changed")
    standards = official.load_jsonl_to_dict(str(gold / "spider2lite_eval.jsonl"))
    summaries = []
    for profile in settings["profiles"]:
        directory = output / profile["name"]
        status_path = directory / "status.json"
        status = (
            json.loads(status_path.read_text())
            if status_path.exists()
            else {"status": "incomplete"}
        )
        if status["status"] != "completed":
            summaries.append(
                dict(profile=profile["name"], status=status["status"], accuracy=None)
            )
            continue
        results = []
        failure_types = Counter()
        calls = []
        seconds = 0
        for c in manifest["cases"]:
            ident = c["instance_id"]
            if (
                ident not in standards
                or not official.resolve_gold_paths(ident, str(gold / "exec_result"))[0]
            ):
                raise ValueError("Missing official gold: " + ident)
            record = json.loads((directory / "records" / f"{ident}.json").read_text())
            calls.extend(record["calls"])
            seconds += record["seconds"]
            if record["status"] != "predicted":
                failure_types[record.get("error_type", "missing_prediction")] += 1
                result = dict(
                    instance_id=ident,
                    score=0,
                    error_info=record.get("error_type", "missing_prediction"),
                )
            else:
                result = official.evaluate_single_exec_result_instance(
                    ident, standards, str(directory / "csv"), str(gold / "exec_result")
                )
            results.append(result)
        write_json(directory / "scores.json", results)
        known = [c["usage"] for c in calls if c.get("usage")]
        summary = dict(
            profile=profile["name"],
            status="completed",
            correct=sum(r["score"] for r in results),
            total=len(results),
            accuracy=sum(r["score"] for r in results) / len(results),
            failed_predictions=sum(failure_types.values()),
            failure_types=dict(failure_types),
            api_calls=len(calls),
            seconds=seconds,
            wall_seconds=status.get("wall_seconds"),
            prompt_tokens=sum(u.get("prompt_tokens", 0) for u in known),
            completion_tokens=sum(u.get("completion_tokens", 0) for u in known),
            usage_missing_calls=len(calls) - len(known),
            cost_usd=None,
        )
        summaries.append(summary)
    write_json(output / "summary.json", summaries)
    print(json.dumps(summaries, ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("prepare", "run", "score"):
        p = sub.add_parser(name)
        p.add_argument("--root", type=Path, required=True)
        p.add_argument("--output", type=Path, required=True)
        if name != "score":
            p.add_argument("--db-dir", type=Path, required=True)
        if name == "prepare":
            p.add_argument("--limit", type=int, default=0)
            p.add_argument("--seed", type=int, default=20261114)
        if name == "run":
            p.add_argument("--manifest", type=Path, required=True)
            p.add_argument("--profiles", type=Path, required=True)
            p.add_argument("--max-calls", type=int, default=8)
            p.add_argument("--concurrency", type=int, default=2)
            p.add_argument("--call-timeout", type=float, default=120)
            p.add_argument("--case-timeout", type=float, default=480)
            p.add_argument("--sql-timeout", type=float, default=30)
            p.add_argument("--max-rows", type=int, default=100000)
            p.add_argument("--env-file", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    args.root = args.root.resolve()
    args.output = args.output.resolve()
    if args.command == "prepare":
        if args.limit < 0:
            parser.error("limit must be nonnegative")
        prepare(args.root, args.db_dir, args.output, args.limit, args.seed)
    elif args.command == "run":
        if (
            min(
                args.concurrency,
                args.max_calls,
                args.call_timeout,
                args.case_timeout,
                args.sql_timeout,
                args.max_rows,
            )
            <= 0
        ):
            parser.error("budgets must be positive")
        if args.env_file:
            from dotenv import load_dotenv

            load_dotenv(args.env_file, override=False)
        asyncio.run(run(args, dict(os.environ)))
    else:
        score(args.root, args.output)


if __name__ == "__main__":
    main()
