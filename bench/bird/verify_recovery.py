"""Exercise evaluator adapters against the real product loop."""
import argparse
import asyncio
import json
import os
import sqlite3
import tempfile
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer
from lang2sql.core.identity import Identity
from lang2sql.harness.loop import agent_loop
from lang2sql.tenancy.concierge import ContextConcierge
from sqlalchemy import create_engine

SQL = "SELECT name FROM actor"
ANSWER = f"```sql\n{SQL}\n```"
FORCED = {"type": "function", "function": {"name": "explore_schema"}}


def response(*calls, content=""):
    message = {"content": content}
    if calls:
        message["tool_calls"] = [
            {
                "id": f"mock_{name}",
                "type": "function",
                "function": {"name": name, "arguments": json.dumps(arguments)},
            }
            for name, arguments in calls
        ]
    data = {"model": "scripted", "choices": [{"finish_reason": "tool_calls" if calls else "stop", "message": message}]}
    return SimpleNamespace(model_dump=lambda: data)


async def exercise(benchmark, profile, path, route, *, live):
    from litellm import acompletion
    from dotenv import dotenv_values

    if benchmark == "bird":
        from bench.bird.evaluate import live_transport
        from bench.common.providers import MeasuredLLM
        from bench.common.recovery import RecoveryTrace
        from bench.common.submissions import final_sql
    else:
        from bench.spider2.evaluate import final_sql
        from bench.spider2.providers import MeasuredLLM
        from bench.spider2.recovery import RecoveryTrace

    schema = ("explore_schema", {"tables": ["actor"]})
    execute = ("run_sql", {"sql": SQL})
    scripted = {
        "final": [response(content=ANSWER), response(schema), response(content=ANSWER)],
        "run_sql": [response(execute, content=ANSWER), response(schema), response(content=ANSWER)],
        "same_response": [response(schema, execute, content=ANSWER), response(content=ANSWER)],
    }[route]
    actual_requests = []

    async def transport(**wire):
        choice = wire["tool_choice"]
        actual_requests.append(choice)
        if len(actual_requests) == 3 or route == "same_response" and len(actual_requests) == 2:
            assert "name" in wire["messages"][-1]["content"] or "name" in wire["messages"][-2]["content"]
        if live and len(actual_requests) > 1:
            original = live_transport if benchmark == "bird" else acompletion
            return await original(**wire)
        return scripted[len(actual_requests) - 1]

    key = dotenv_values(os.environ["BENCH_CREDENTIALS"], interpolate=False)["OPENAI_API_KEY"] if live else ""
    if benchmark == "bird":
        llm = MeasuredLLM(
            model=profile["model"], api_base=profile.get("api_base"), api_key=key,
            timeout=120, max_calls=8, max_output_tokens=4096, transport=transport,
            observe_first_request=lambda *_: None, save_attempt=lambda *_: None,
            reasoning_effort=profile.get("reasoning_effort"),
        )
    else:
        llm = MeasuredLLM(profile, key, 120, 8)
    explorer = SqlAlchemyExplorer("sqlite://")
    explorer._engine = create_engine(f"sqlite:///{path}")
    trace = RecoveryTrace(explorer)
    concierge = ContextConcierge(llm=llm, explorer=explorer, max_turns=8)
    try:
        ctx = await concierge.build_context(Identity("benchmark", "smoke", "smoke"))
        trace.attach(ctx.tools)
        async def run():
            return await agent_loop(
                ctx, "Return all actor names as a single fenced SQL query. The table is actor.",
                extract_final_sql=final_sql,
            )
        if benchmark == "spider2":
            with patch("litellm.acompletion", transport):
                answer = await run()
        else:
            answer = await run()
        transcript = [asdict(message) for message in ctx.session.history()]
        recovery = trace.summarize(llm.calls, transcript, final_sql, submitted=answer.startswith("```"))
        if not live:
            assert final_sql(answer) == SQL
            expected = ["auto", "auto"] if route == "same_response" else ["auto", FORCED, "auto"]
            assert actual_requests == expected
            first = recovery["turn_events"][0]
            assert first["withheld_final_sql"] == (route == "final")
            assert len(first["withheld_run_sql"]) == int(route != "final")
            assert first["same_response_columns"] == (route == "same_response")
            assert recovery["rewritten_submission"]
            assert not recovery["db_executions"]
        else:
            # First completion is scripted solely to drive recovery. Subsequent entries are real.
            assert actual_requests[:2] == ["auto", FORCED]
        return {
            "benchmark": benchmark, "profile": profile, "route": route,
            "scripted_first_response": True, "live": live, "requests": actual_requests,
            "calls": llm.calls, "tools": trace.dispatches, "recovery": recovery, "answer": answer,
        }
    finally:
        concierge.store.close()
        explorer._engine.dispose()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("benchmark", choices=("spider2", "bird"))
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--profile", default="openai-gpt41-mini")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    profiles = json.loads(Path(f"bench/{args.benchmark}/models.example.json").read_text())
    profile = next(item for item in profiles if item["name"] == args.profile)
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "smoke.sqlite"
        with sqlite3.connect(path) as connection:
            connection.execute("CREATE TABLE actor (name TEXT)")
        routes = ("final",) if args.live else ("final", "run_sql", "same_response")
        results = [asyncio.run(exercise(args.benchmark, profile, path, route, live=args.live)) for route in routes]
    args.output.write_text(json.dumps(results, ensure_ascii=False, indent=2) + "\n")
    if args.live:
        call = results[0]["calls"][1]
        assert call["status"] == "ok" and call["tool_calls"][0]["name"] == "explore_schema", call
        assert results[0]["recovery"]["turn_events"][1]["tables_with_columns"] == ["actor"]
        assert results[0]["requests"][2] == "auto"
    print(args.benchmark, args.profile, "passed", len(results), "scenarios")


if __name__ == "__main__":
    main()
