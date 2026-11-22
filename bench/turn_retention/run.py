"""Paired fresh-session and 100-turn evaluations using the installed Lang2SQL harness."""

import argparse
import asyncio
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import time
from collections import Counter
from contextlib import closing
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer
from lang2sql.adapters.llm.openai_ import OpenAILLM
from lang2sql.core.identity import Identity
from lang2sql.harness.loop import agent_loop
from lang2sql.harness.tool_registry import ToolRegistry
from lang2sql.tenancy.concierge import ContextConcierge
from lang2sql.tools.ask_user import AskUser
from lang2sql.tools.explore_schema import ExploreSchema
from lang2sql.tools.run_sql import RunSQL

from .fixture import DEFINITION, TERM, freeze

MODEL = "gpt-4.1-mini-2025-04-14"
SEEDS = (26701, 26702, 26703)
MAX_CALLS = 16
FORMAT = """For a numeric answer, run the query, write RESULT=<integer> on its own line, then exactly one fenced sql block containing the final executable SQLite query. If clarification is needed, respond with CLARIFY: and your question. Do not guess a result."""
SQL = re.compile(r"```sql\s*(.*?)```", re.IGNORECASE | re.DOTALL)
VALUE = re.compile(r"^RESULT\s*=\s*(-?\d+)\s*$", re.MULTILINE | re.IGNORECASE)


class MeasuredModel(OpenAILLM):
    def __init__(self, seed):
        super().__init__(model=MODEL, timeout=120)
        self.seed = seed
        self.calls = []

    def _post(self, payload):
        if len(self.calls) >= MAX_CALLS:
            raise RuntimeError("Model call budget exhausted")
        payload.update(seed=self.seed, temperature=0.2, max_tokens=1024)
        payload["messages"][0]["content"] += "\n" + FORMAT
        record = {"usage": None}
        self.calls.append(record)
        raw = super()._post(payload)
        record.update(
            usage=raw["usage"],
            model=raw["model"],
            fingerprint=raw.get("system_fingerprint"),
        )
        return raw


def readonly(path):
    db = sqlite3.connect(
        path.resolve().as_uri() + "?mode=ro", uri=True, check_same_thread=False
    )
    db.execute("PRAGMA query_only=ON")
    db.set_authorizer(
        lambda action, *args: (
            sqlite3.SQLITE_DENY
            if action in (sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH)
            else sqlite3.SQLITE_OK
        )
    )
    deadline = time.monotonic() + 15
    db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
    return db


def scalar(path, sql):
    if not re.match(r"^(SELECT|WITH)\b", sql, re.IGNORECASE):
        raise ValueError("Expected SELECT")
    with closing(readonly(path)) as db:
        rows = db.execute(sql).fetchmany(2)
    if len(rows) != 1 or len(rows[0]) != 1:
        raise ValueError("Expected exactly one scalar")
    return rows[0][0]


def reported_value(answer):
    prose = SQL.sub("", answer)
    labeled = VALUE.findall(prose)
    if len(labeled) == 1:
        return int(labeled[0])
    # Natural Korean answers often echo the supplied year/month before the value.
    prose = re.sub(r"\d{4}-\d{2}(?:-\d{2})?|\d+\s*[년월일]", "", prose)
    numbers = set(re.findall(r"(?<![\d.])-?\d+(?:,\d{3})*(?:\.\d+)?(?!\d)", prose))
    values = {float(n.rstrip(",").replace(",", "")) for n in numbers}
    if len(values) == 1:
        value = values.pop()
        return int(value) if value.is_integer() else value
    return None


def final_query(answer, history):
    blocks = SQL.findall(answer)
    if blocks:
        return (blocks[0].strip(), "answer") if len(blocks) == 1 else ("", "ambiguous")
    # Match the frontend's SQL evidence to this turn's successful tool response.
    calls = {
        call.id: call.arguments.get("sql", "")
        for msg in history
        for call in msg.tool_calls
        if call.name == "run_sql"
    }
    for msg in reversed(history):
        if msg.name == "run_sql":
            if re.match(r"(?:\d+ row\(s\):|\(0 rows\))", msg.content):
                return calls.get(msg.tool_call_id, ""), "tool"
            break
    return "", "missing"


def grade(case, answer, data, history):
    sql, source = final_query(answer, history)
    shown = reported_value(answer)
    result = {
        "reported": shown,
        "sql_source": source,
        "sql": sql,
        "executed": False,
        "correct_sql": False,
        "correct": False,
        "probe_applied": None,
    }
    try:
        actual = scalar(data / "primary.sqlite", sql)
        result.update(
            executed=True,
            correct_sql=actual == case["primary"],
            correct=shown == actual == case["primary"],
        )
        if case["probe"]:
            result["probe_applied"] = (
                actual == case["primary"]
                and scalar(data / "shadow.sqlite", sql) == case["shadow"]
            )
    except (sqlite3.Error, ValueError) as exc:
        result["grade_error"] = str(exc)
    return result


async def register(ctx):
    result = await ctx.tools.dispatch(
        "term_custom",
        {"term": TERM, "definition": DEFINITION, "layer": "team", "kind": "metric"},
        ctx,
        "register",
    )
    if result.is_error:
        raise RuntimeError("Definition registration failed")


async def lane(data, out, mode, seed, cases):
    llm = MeasuredModel(seed)
    explorer = SqlAlchemyExplorer("sqlite://")
    explorer._engine = create_engine(
        "sqlite://",
        poolclass=NullPool,
        creator=lambda: readonly(data / "primary.sqlite"),
    )
    concierge = ContextConcierge(llm=llm, explorer=explorer, max_turns=MAX_CALLS)
    identity = Identity(user_id="analyst", guild_id="benchmark", channel_id="sales")
    with (out / f"{mode}-{seed}.jsonl").open("x", encoding="utf-8", buffering=1) as log:
        try:
            for case in cases:
                ctx = await concierge.build_context(identity)
                if mode == "fresh":
                    ctx.session.reset()
                before = len(ctx.session.history())
                llm.calls.clear()
                registration_tools = ctx.tools
                ctx.tools = ToolRegistry([RunSQL(), ExploreSchema(), AskUser()])
                answer = ""
                error = None
                try:
                    answer = await agent_loop(ctx, case["question"])
                except (RuntimeError, TimeoutError, OSError) as exc:
                    error = type(exc).__name__
                turn_history = ctx.session.history()[before:]
                actions = Counter(
                    call.name
                    for m in ctx.session.history()[before:]
                    for call in m.tool_calls
                )
                # Same compression/persistence order as the Discord question handler.
                ctx.session.compress()
                await concierge.store.save(identity.session_key(), ctx.session)
                ctx.tools = registration_tools
                if case["turn"] == 10:
                    await register(ctx)
                graded = grade(case, answer, data, turn_history)
                usage = [c["usage"] for c in llm.calls if c["usage"] is not None]
                inp = sum(u["prompt_tokens"] for u in usage)
                cached = sum(
                    u.get("prompt_tokens_details", {}).get("cached_tokens", 0)
                    for u in usage
                )
                output = sum(u["completion_tokens"] for u in usage)
                row = {
                    "mode": mode,
                    "seed": seed,
                    "turn": case["turn"],
                    "history_before": before,
                    "answer": answer,
                    "error": error,
                    **graded,
                    "tools": actions,
                    "clarified": bool(
                        actions["ask_user"]
                        or "CLARIFY:" in answer.upper()
                        or ("?" in answer and not graded["executed"])
                    ),
                    "calls": list(llm.calls),
                    "input": inp,
                    "cached": cached,
                    "output": output,
                    "estimated_usd": (
                        (inp - cached) * 0.40 + cached * 0.10 + output * 1.60
                    )
                    / 1_000_000,
                }
                log.write(json.dumps(row, ensure_ascii=False) + "\n")
        finally:
            concierge.store.close()
            explorer._engine.dispose()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("action", choices=["freeze", "run"])
    p.add_argument("data", type=Path)
    p.add_argument("--output", type=Path)
    a = p.parse_args()
    if a.action == "freeze":
        freeze(a.data)
        return
    if not a.output or not os.getenv("OPENAI_API_KEY"):
        p.error("--output and OPENAI_API_KEY are required")
    hashes = json.loads((a.data / "hashes.json").read_text())
    for name, expected in hashes.items():
        if hashlib.sha256((a.data / name).read_bytes()).hexdigest() != expected:
            raise ValueError(f"Frozen fixture changed: {name}")
    cases = json.loads((a.data / "manifest.json").read_text())["cases"]
    a.output.mkdir(parents=True, exist_ok=False)
    import inspect

    runtime = Path(inspect.getfile(agent_loop))
    config = {
        "model": MODEL,
        "seeds": SEEDS,
        "temperature": 0.2,
        "max_calls": MAX_CALLS,
        "output_cap": 1024,
        "hashes": hashes,
        "loop_sha256": hashlib.sha256(runtime.read_bytes()).hexdigest(),
        "runtime_head": subprocess.check_output(
            ["git", "-C", str(runtime.parent), "rev-parse", "HEAD"], text=True
        ).strip(),
        "input_usd_per_million": 0.40,
        "cached_usd_per_million": 0.10,
        "output_usd_per_million": 1.60,
    }
    (a.output / "config.json").write_text(json.dumps(config, indent=2) + "\n")

    async def run():
        await asyncio.gather(
            *(
                lane(a.data, a.output, mode, seed, cases)
                for seed in SEEDS
                for mode in ("fresh", "long")
            )
        )

    asyncio.run(run())


if __name__ == "__main__":
    main()
