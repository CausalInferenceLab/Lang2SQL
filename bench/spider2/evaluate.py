"""Read-only case execution and official Spider scoring reused by accuracy CLI."""

from __future__ import annotations

import asyncio
import csv
import hashlib
import importlib.util
import json
import logging
import os
import re
import sqlite3
import sys
import time
from collections import Counter
from dataclasses import asdict
from pathlib import Path

from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer
from lang2sql.core.identity import Identity
from lang2sql.harness.loop import agent_loop
from lang2sql.safety.layers.whitelist import _strip_comments
from lang2sql.tenancy.concierge import ContextConcierge

from .providers import MeasuredLLM

logger = logging.getLogger(__name__)
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


def final_sql(answer: str) -> str:
    blocks = re.findall(r"```sql\s*\n?(.*?)```", answer, re.IGNORECASE | re.DOTALL)
    if len(blocks) != 1:
        raise ValueError(
            "Final answer must contain exactly one SQL block; no last-tool fallback"
        )
    sql = blocks[0].strip()
    if not re.match(r"^(SELECT|WITH)\b", _strip_comments(sql), re.IGNORECASE):
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
    schema_doc = getattr(args, "schema_documents", {}).get(case["db"])
    if schema_doc:
        prompt += (
            "\n\nOfficial database schema documentation (metadata, not SQL to execute):\n"
            + schema_doc
        )
    if case.get("external_knowledge"):
        prompt += (
            "\n\nProvided benchmark documentation:\n"
            + (
                root / "spider2-lite/resource/documents" / case["external_knowledge"]
            ).read_text()
        )
    if case.get("evidence"):
        prompt += "\n\nProvided benchmark evidence:\n" + case["evidence"]
    prompt += INSTRUCTION
    record = {
        "instance_id": case["instance_id"],
        "profile": profile["name"],
        "model": profile["model"],
        "status": "started",
    }
    try:
        answer = await asyncio.wait_for(agent_loop(ctx, prompt), args.case_timeout)
        record["initial_answer"] = answer
        record["answer"] = answer
        # A review is another measured agent turn, not a gold-scored candidate search.
        # Validate first: clarification/refusal must never be overridden by review.
        final_sql(answer)
        record["reviews"] = []
        for review in range(getattr(args, "review_rounds", 0)):
            remaining = args.case_timeout - (time.monotonic() - started)
            if remaining <= 0:
                raise TimeoutError("Case deadline exhausted before SQL review")
            logger.info("SQL review id=%s round=%d", case["instance_id"], review + 1)
            answer = await asyncio.wait_for(
                agent_loop(
                    ctx,
                    "Review your proposed SQL against the original request below. "
                    "Re-check the actual schema, stored values, join multiplicity, "
                    "missing-value semantics, date boundaries, numeric precision and "
                    "exact output columns. Use diagnostic SQL to test any suspected "
                    "mistake. Do not change a correct query without evidence. "
                    "Do not invent missing business definitions. Execute the final "
                    "query and return exactly one final SQL block.\n\n" + prompt,
                ),
                remaining,
            )
            record["reviews"].append(answer)
            record["answer"] = answer
            final_sql(answer)
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
    except Exception as exc:  # noqa: BLE001 — record all model/SQL failures
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
                {
                    "profile": profile["name"],
                    "status": status["status"],
                    "accuracy": None,
                }
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
                result = {
                    "instance_id": ident,
                    "score": 0,
                    "error_info": record.get("error_type", "missing_prediction"),
                }
            else:
                result = official.evaluate_single_exec_result_instance(
                    ident, standards, str(directory / "csv"), str(gold / "exec_result")
                )
            results.append(result)
        write_json(directory / "scores.json", results)
        known = [c["usage"] for c in calls if c.get("usage")]
        summary = {
            "profile": profile["name"],
            "status": "completed",
            "correct": sum(r["score"] for r in results),
            "total": len(results),
            "accuracy": sum(r["score"] for r in results) / len(results),
            "failed_predictions": sum(failure_types.values()),
            "failure_types": dict(failure_types),
            "api_calls": len(calls),
            "seconds": seconds,
            "wall_seconds": status.get("wall_seconds"),
            "prompt_tokens": sum(u.get("prompt_tokens", 0) for u in known),
            "completion_tokens": sum(u.get("completion_tokens", 0) for u in known),
            "usage_missing_calls": len(calls) - len(known),
            "cost_usd": None,
        }
        summaries.append(summary)
    write_json(output / "summary.json", summaries)
    print(json.dumps(summaries, ensure_ascii=False, indent=2))
