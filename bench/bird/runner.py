"""Run the pinned input through the selected product's own SQL harness."""

from __future__ import annotations

import asyncio
import inspect
import json
import time
from contextlib import closing
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from cryptography.fernet import Fernet
from sqlalchemy import create_engine, event
from sqlalchemy.pool import NullPool

from lang2sql import tools as product_tools
from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer
from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.core.identity import Identity
from lang2sql.harness.loop import agent_loop
from lang2sql.harness.tool_registry import ToolRegistry
from lang2sql.tenancy.concierge import ContextConcierge
from lang2sql.tenancy.encrypted_secrets import EncryptedSecrets

from .dataset import Case
from .io import ProtocolError, json_digest, lock_output, read_json, write_json
from .protocol import (
    API_BASE,
    OPTIONAL_TOOLS,
    REQUIRED_TOOLS,
    Budgets,
    check_settings,
    user_text,
)
from .providers import CallBudgetExhausted, MeasuredLLM, ModelCallError, Transport
from .records import CaseFiles
from .sqlite import read_connection, set_deadline
from .submissions import SubmissionError, final_sql

HAS_FINAL_SQL_HOOK = "extract_final_sql" in inspect.signature(agent_loop).parameters


class ReadOnlyExplorer(SqlAlchemyExplorer):
    def __init__(self, db: Path, timeout: float):
        super().__init__("sqlite://")
        self._engine = create_engine(
            "sqlite://",
            poolclass=NullPool,
            creator=lambda: read_connection(db, time.monotonic() + timeout),
        )

        @event.listens_for(self._engine, "before_cursor_execute")
        def deadline(connection, cursor, statement, parameters, context, executemany):
            set_deadline(cursor.connection, time.monotonic() + timeout)

    def close(self) -> None:
        self._engine.dispose()


class CaseStore(SqliteStore):
    """Keep case knowledge empty while allowing the product to write audit events."""

    def kv_set(self, scope: str, key: str, value: str) -> None:
        raise ProtocolError("Knowledge writes are disabled during inference")

    def kv_delete(self, scope: str, key: str) -> None:
        raise ProtocolError("Knowledge writes are disabled during inference")

    def kv_delete_prefix(self, scope: str, prefix: str) -> int:
        raise ProtocolError("Knowledge writes are disabled during inference")


class QueryTools(ToolRegistry):
    def __init__(self):
        tools = [product_tools.RunSQL(), product_tools.ExploreSchema()]
        review = getattr(product_tools, "ReviewSQL", None)
        if review is not None:
            tools.append(review())
        super().__init__(tools)
        self.dispatches = []

    async def dispatch(self, name, args, ctx, call_id):
        result = await super().dispatch(name, args, ctx, call_id)
        self.dispatches.append({"name": name, "arguments": args, **asdict(result)})
        return result


def scripted_transport(db: Path, response_model: str, timeout: float) -> Transport:
    """Exercise actual schema, SQL and scoring paths without loading a provider."""
    with closing(read_connection(db, time.monotonic() + timeout)) as connection:
        table = connection.execute(
            "SELECT name FROM sqlite_schema WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name LIMIT 1"
        ).fetchone()[0]
    escaped_table = table.replace('"', '""')
    sql = f'SELECT COUNT(*) FROM "{escaped_table}"'

    async def complete(**request):
        tools = request.get("tools", [])
        messages = request["messages"]
        tool_turns = sum(message["role"] == "tool" for message in messages)
        if tool_turns >= 2:
            message = {"content": f"```sql\n{sql}\n```"}
            finish_reason = "stop"
        else:
            if tool_turns == 0:
                name = "explore_schema"
                spec = next(
                    tool["function"]
                    for tool in tools
                    if tool["function"]["name"] == name
                )
                args = (
                    {"tables": [table]}
                    if "tables" in spec["parameters"]["properties"]
                    else {"table": table}
                )
            else:
                name, args = "run_sql", {"sql": sql}
            message = {
                "content": None,
                "tool_calls": [
                    {
                        "id": f"scripted-{tool_turns}",
                        "type": "function",
                        "function": {"name": name, "arguments": json.dumps(args)},
                    }
                ],
            }
            finish_reason = "tool_calls"
        raw = {
            "model": response_model,
            "choices": [
                {
                    "message": {"role": "assistant", **message},
                    "finish_reason": finish_reason,
                }
            ],
        }
        return SimpleNamespace(model_dump=lambda: raw)

    return complete


async def run_case(
    case: Case,
    db: Path,
    files: CaseFiles,
    budgets: Budgets,
    profile: dict,
    transport: Transport | None,
) -> dict:
    started = time.monotonic()
    started_at = datetime.now(timezone.utc).isoformat()
    text = user_text(case.question, case.evidence, files.condition)
    trace: dict = {}
    answer = None
    transcript: list[dict] = []
    tools = QueryTools()
    store = CaseStore()
    explorer = ReadOnlyExplorer(db, budgets.sql_timeout)

    def observe(wire: list[dict], specs: list[dict]) -> None:
        if [message["role"] for message in wire] != ["system", "user"]:
            raise ProtocolError(
                "First request must have one system and one user message"
            )
        if wire[1]["content"] != text:
            raise ProtocolError(
                "First user message differs from the fixed evidence protocol"
            )
        names = tuple(tool["function"]["name"] for tool in specs)
        if names not in (REQUIRED_TOOLS, REQUIRED_TOOLS + OPTIONAL_TOOLS):
            raise ProtocolError("Unexpected first-request SQL tools")
        trace.update(
            system=wire[0]["content"],
            user=wire[1]["content"],
            evidence_role="user" if files.condition == "B" else None,
            tool_specs=specs,
        )

    def save_attempt(calls: list[dict]) -> None:
        files.save_attempt(trace, calls, started_at=started_at)

    save_attempt([])
    llm = MeasuredLLM(
        model=profile["model"],
        response_model=profile["response_model"],
        api_base=API_BASE,
        timeout=budgets.call_timeout,
        max_calls=budgets.max_calls,
        max_output_tokens=budgets.max_output_tokens,
        transport=transport
        or scripted_transport(db, profile["response_model"], budgets.sql_timeout),
        observe_first_request=observe,
        save_attempt=save_attempt,
        reasoning_effort=profile.get("reasoning_effort"),
    )

    async def generate() -> str:
        identity = Identity(
            user_id="benchmark",
            guild_id=f"bird-{case.question_id}",
            channel_id="benchmark",
        )
        concierge = ContextConcierge(
            store=store,
            llm=llm,
            explorer=explorer,
            max_turns=budgets.max_calls,
            secrets=EncryptedSecrets(store, key=Fernet.generate_key()),
        )
        ctx = await concierge.build_context(identity)
        ctx.okf_bundle_dir = None
        if store.kv_list_prefix(identity.kv_scope, "") or ctx.session.history():
            raise ProtocolError("Each case must start with an empty KV and session")
        ctx.tools = tools
        try:
            kwargs = {"extract_final_sql": final_sql} if HAS_FINAL_SQL_HOOK else {}
            return await agent_loop(ctx, text, **kwargs)
        finally:
            transcript.extend(asdict(message) for message in ctx.session.history())

    result = {"status": "failed"}
    try:
        answer = await asyncio.wait_for(generate(), timeout=budgets.case_timeout)
        if answer.startswith("(SQL withheld:"):
            failed_call = next(
                (call for call in reversed(llm.calls) if call["status"] == "error"),
                None,
            )
            if "model request failed" in answer and failed_call is not None:
                raise ModelCallError(
                    failed_call["error_reason"],
                    failed_call.get("error_type", "Provider"),
                    failed_call.get("status_code"),
                )
            if len(llm.calls) >= budgets.max_calls:
                raise CallBudgetExhausted
        if answer == "(reached max turns without a final answer)":
            raise CallBudgetExhausted
        result.update(status="predicted", sql=final_sql(answer))
    except CallBudgetExhausted:
        result["failure"] = {"stage": "generation", "reason": "call_budget_exhausted"}
    except asyncio.TimeoutError:
        result["failure"] = {"stage": "generation", "reason": "case_timeout"}
    except SubmissionError as error:
        result["failure"] = {"stage": "submission", "reason": error.reason}
    except ModelCallError as error:
        result["failure"] = {
            "stage": "generation",
            "reason": error.reason,
            "error_type": error.error_type,
            "status_code": error.status_code,
        }
    finally:
        store.close()
        explorer.close()
    for call in llm.calls:
        if call.get("error_reason") == "model_identity_mismatch":
            raise ProtocolError(
                f"Provider model {call.get('response_model')!r} differs from "
                f"{profile['response_model']}"
            )
        if call.get("status_code") in (401, 403, 404):
            raise ProtocolError(
                f"Provider rejected the credentials or model (HTTP {call['status_code']})"
            )
    if transport is None:
        succeeded = {tool["name"] for tool in tools.dispatches if not tool["is_error"]}
        if result["status"] != "predicted" or not succeeded.issuperset(REQUIRED_TOOLS):
            raise ProtocolError(
                f"Smoke case {case.question_id} did not complete both tools and submit SQL"
            )
    result.update(
        started_at=started_at,
        first_request=trace or None,
        calls=llm.calls,
        seconds=time.monotonic() - started,
        answer=answer,
        transcript=transcript,
        tools=tools.dispatches,
    )
    return files.finish(result)


async def run_evaluation(
    output: Path,
    manifest: dict,
    snapshot: dict,
    cases: list[Case],
    databases: dict[str, Path],
    transport: Transport | None,
    stop: asyncio.Event,
    *,
    resume: bool,
) -> dict:
    check_settings(manifest, snapshot, snapshot["benchmark_commit"])
    if (transport is None) != (manifest["smoke_cases"] is not None):
        raise ProtocolError("Scripted transport requires the smoke manifest")
    output.mkdir(parents=True, exist_ok=resume)
    with lock_output(output):
        if resume:
            if (
                read_json(output / "manifest.json") != manifest
                or read_json(output / "settings.json") != snapshot
            ):
                raise ProtocolError(
                    "Resume requires the original manifest and settings"
                )
            if (output / "status.json").exists() and read_json(output / "status.json")[
                "status"
            ] == "blocked":
                raise ProtocolError(
                    "Start a new run after resolving the protocol error"
                )
        else:
            write_json(output / "manifest.json", manifest)
            write_json(output / "settings.json", snapshot)

        by_id = {case.question_id: case for case in cases}
        settings_digest = json_digest(snapshot)
        budgets = Budgets(**snapshot["budgets"])
        condition, profile = snapshot["condition"], snapshot["profile"]
        save = {
            question_id: CaseFiles(
                output, question_id, condition, profile["name"], settings_digest
            )
            for question_id in by_id
        }
        records = {}
        for key, files in save.items():
            previous = files.resume() if resume else None
            if previous is not None:
                records[key] = previous
        failures: list[Exception] = []

        def status(state: str) -> dict:
            value = {
                "status": state,
                "condition": condition,
                "profile": profile["name"],
                "expected": len(save),
                "completed": len(records),
                "failed": sum(
                    record["status"] == "failed" for record in records.values()
                ),
            }
            write_json(output / "status.json", value)
            return value

        async def batch(question_ids: list[int]) -> None:
            queue = iter(question_ids)

            async def worker() -> None:
                while (
                    not stop.is_set() and (question_id := next(queue, None)) is not None
                ):
                    if question_id in records:
                        continue
                    case = by_id[question_id]
                    try:
                        records[question_id] = await run_case(
                            case,
                            databases[case.db_id],
                            save[question_id],
                            budgets,
                            profile,
                            transport,
                        )
                        status("running")
                    except (
                        Exception
                    ) as error:  # Drain active cases and preserve records.
                        failures.append(error)
                        stop.set()
                        return

            await asyncio.gather(*(worker() for _ in range(budgets.concurrency)))

        order = snapshot["question_order"]
        pilot, rest = order[: budgets.pilot_questions], order[budgets.pilot_questions :]
        status("running")
        await batch(pilot)
        if (
            pilot
            and not stop.is_set()
            and not any(
                call["status"] == "ok"
                for question_id in pilot
                for call in records[question_id]["calls"]
            )
        ):
            failures.append(ProtocolError("Pilot had no usable provider response"))
            stop.set()
        await batch(rest)
        if failures:
            status("blocked")
            raise failures[0]
        return status("completed" if len(records) == len(save) else "interrupted")
