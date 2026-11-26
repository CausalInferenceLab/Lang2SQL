"""Run fixed benchmark questions through ChatGPT-authenticated Codex and real MCP.

Reuse pinned inputs, the production MCP server, SQL extraction and official scoring.
No answer keys or API credential files are passed to Codex.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import subprocess
import time
from pathlib import Path

from bench.accuracy.evaluate import load_schema_documents, validate_inputs
from bench.oracle_view.codex_demo import READ_TOOLS, mcp_settings
from bench.spider2.evaluate import (
    INSTRUCTION,
    digest,
    export_result,
    final_sql,
    validate_manifest,
    write_json,
)

logger = logging.getLogger(__name__)


def executed_sql(answer, events):
    """Only MCP-executed final answers qualify; no shell or final-tool fallback."""
    sql = final_sql(answer)
    items = [e["item"] for e in events if e.get("type") == "item.completed"]
    if any(
        i.get("type") in {"command_execution", "web_search", "file_change"}
        for i in items
    ):
        raise ValueError("Non-MCP tool used")
    calls = [i for i in items if i.get("type") == "mcp_tool_call"]
    if any(
        i.get("server") != "lang2sql" or i.get("tool") not in READ_TOOLS for i in calls
    ):
        raise ValueError("Unexpected MCP tool")
    if not any(
        i.get("tool") == "run_sql"
        and i.get("status") == "completed"
        and not i.get("error")
        and i.get("arguments", {}).get("sql", "").strip() == sql
        for i in calls
    ):
        raise ValueError("Final SQL was not executed through Lang2SQL MCP")
    return sql


async def run(args):
    repo = Path(__file__).resolve().parents[2]
    manifest = json.loads(args.manifest.read_text())
    validate_inputs(manifest, args.db_dir)
    if manifest["suite"] == "spider2":
        validate_manifest(manifest, args.root, args.db_dir)
    docs = load_schema_documents(args.schema_docs, manifest["suite"])
    selected = set(manifest["development_ids"]) if args.split == "development" else None
    manifest["cases"] = [
        c for c in manifest["cases"] if selected is None or c["instance_id"] in selected
    ]
    if args.output.exists():
        raise ValueError("Use a fresh output directory; never overwrite attempts")
    args.output.mkdir(parents=True)
    directory = args.output / "codex-mcp"
    for part in ("records", "sql", "csv", "events", "work"):
        (directory / part).mkdir(parents=True)
    write_json(args.output / "manifest.json", manifest)
    write_json(
        args.output / "settings.json",
        {
            "profiles": [
                {"name": "codex-mcp", "model": "Codex CLI default (no override)"}
            ],
            "backend": "ChatGPT login -> Codex CLI -> production Lang2SQL MCP",
            "revision": (
                await asyncio.to_thread(
                    subprocess.check_output,
                    ["git", "rev-parse", "HEAD"],
                    cwd=repo,
                    text=True,
                )
            ).strip(),
            "codex_version": (
                await asyncio.to_thread(
                    subprocess.check_output, ["codex", "--version"], text=True
                )
            ).strip(),
            "sources": {
                str(p.relative_to(repo)): digest(p)
                for p in [
                    Path(__file__).resolve(),
                    repo / "src/lang2sql/frontends/mcp/server.py",
                ]
            },
            "schema_document_sha256": digest(args.schema_docs),
            "split": args.split,
            "concurrency": args.concurrency,
            "case_timeout": args.case_timeout,
            "sql_timeout": args.sql_timeout,
            "max_rows": args.max_rows,
            "api_calls": None,
            "usage_unit": "one Codex CLI invocation per question; internal model-call count unavailable",
        },
    )
    semaphore = asyncio.Semaphore(args.concurrency)

    async def case_run(case):
        async with semaphore:
            ident = case["instance_id"]
            started = time.monotonic()
            record = {
                "instance_id": ident,
                "profile": "codex-mcp",
                "model": "Codex CLI default",
                "status": "started",
                "calls": [],
            }
            work = directory / "work" / ident
            work.mkdir()
            answer_path = directory / "events" / (ident + ".answer.txt")
            events_path = directory / "events" / (ident + ".jsonl")
            settings = mcp_settings(
                ["--org-id", "benchmark", "--user-id", ident], extra_env=("PYTHONPATH",)
            )
            settings.update(
                {
                    "features.shell_tool": False,
                    "features.unified_exec": False,
                    "features.apps": False,
                    "web_search": "disabled",
                }
            )
            command = [
                "codex",
                "exec",
                "--ignore-user-config",
                "--ephemeral",
                "--skip-git-repo-check",
                "--json",
                "--output-last-message",
                str(answer_path),
            ]
            for key, value in settings.items():
                command += ["-c", key + "=" + json.dumps(value)]
            command += ["-"]
            prompt = (
                "Use only lang2sql MCP tools. Do not use shell, files, web, or other servers. Do not invent missing business definitions.\n"
                + case["question"]
            )
            if case.get("evidence"):
                prompt += "\nProvided evidence:\n" + case["evidence"]
            if case.get("external_knowledge"):
                prompt += (
                    "\nDocumentation:\n"
                    + (
                        args.root
                        / "spider2-lite/resource/documents"
                        / case["external_knowledge"]
                    ).read_text()
                )
            prompt += (
                "\nOfficial schema metadata (examples are not exhaustive):\n"
                + docs[case["db"]]
                + INSTRUCTION
            )
            env = {
                k: v
                for k, v in os.environ.items()
                if k
                not in {
                    "OPENAI_API_KEY",
                    "OPEN_AI_KEY",
                    "OKF_BUNDLE_DIR",
                    "LANG2SQL_DB_SCHEMA",
                }
            }
            db = (args.db_dir / (case["db"] + ".sqlite")).resolve()
            env.update(
                PYTHONPATH=str(repo / "src"),
                LANG2SQL_DB_URL="sqlite:///file:" + str(db) + "?mode=ro&uri=true",
            )
            process = None
            try:
                with (
                    events_path.open("wb") as out,
                    (directory / "events" / (ident + ".stderr")).open("wb") as err,
                ):
                    process = await asyncio.create_subprocess_exec(
                        *command,
                        cwd=work,
                        env=env,
                        stdin=asyncio.subprocess.PIPE,
                        stdout=out,
                        stderr=err,
                    )
                    await asyncio.wait_for(
                        process.communicate(prompt.encode()), args.case_timeout
                    )
                events = [
                    json.loads(line)
                    for line in events_path.read_text().splitlines()
                    if line.startswith("{")
                ]
                record["events_sha256"] = digest(events_path)
                record["cli_exit_code"] = process.returncode
                record["cli_usage"] = [
                    e.get("usage") for e in events if e.get("type") == "turn.completed"
                ]
                record["mcp_calls"] = [
                    e["item"]
                    for e in events
                    if e.get("type") == "item.completed"
                    and e["item"].get("type") == "mcp_tool_call"
                ]
                record["answer"] = (
                    answer_path.read_text() if answer_path.exists() else ""
                )
                if process.returncode != 0:
                    raise RuntimeError("Codex invocation failed")
                sql = executed_sql(record["answer"], events)
                record["rows"] = await asyncio.to_thread(
                    export_result,
                    db,
                    sql,
                    directory / "csv" / (ident + ".csv"),
                    args.sql_timeout,
                    args.max_rows,
                )
                (directory / "sql" / (ident + ".sql")).write_text(sql + "\n")
                record.update(status="predicted", sql=sql)
            except Exception as exc:  # noqa: BLE001 — retain every failed benchmark prediction
                logger.warning(
                    "Codex case failed id=%s error=%s", ident, type(exc).__name__
                )
                record.update(status="failed", error_type=type(exc).__name__)
            finally:
                if process is not None and process.returncode is None:
                    process.kill()
                    await process.wait()
                record["seconds"] = time.monotonic() - started
                write_json(directory / "records" / (ident + ".json"), record)
            logger.info("Codex case finished id=%s status=%s", ident, record["status"])

    started = time.monotonic()
    await asyncio.gather(*(case_run(c) for c in manifest["cases"]))
    write_json(
        directory / "status.json",
        {"status": "completed", "wall_seconds": time.monotonic() - started},
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("manifest", "db-dir", "root", "schema-docs", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument(
        "--split", choices=["development", "all"], default="development"
    )
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--case-timeout", type=float, default=600)
    parser.add_argument("--sql-timeout", type=float, default=30)
    parser.add_argument("--max-rows", type=int, default=100000)
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
