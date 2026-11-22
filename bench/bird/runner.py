"""Run one condition and model profile through the product harness."""

from __future__ import annotations

import asyncio
import time
from dataclasses import asdict
from pathlib import Path

from cryptography.fernet import Fernet

from bench.common.io import (
    ProtocolError,
    json_digest,
    lock_output,
    read_json,
    write_json,
)
from bench.common.providers import (
    CallBudgetExhausted,
    MeasuredLLM,
    ModelCallError,
    Transport,
)
from bench.common.sqlite import ReadOnlyExplorer
from bench.common.submissions import SubmissionError, final_sql
from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.core.identity import Identity
from lang2sql.harness.loop import agent_loop
from lang2sql.harness.tool_registry import ToolRegistry
from lang2sql.tenancy.concierge import ContextConcierge
from lang2sql.tenancy.encrypted_secrets import EncryptedSecrets
from lang2sql.tools import ExploreSchema, RunSQL
from lang2sql.tools.semantic_federation import build_prompt_section

from .dataset import Case
from .protocol import (
    EVIDENCE_LABEL,
    TOOLS,
    Budgets,
    check_settings,
    user_text,
)
from .records import CaseFiles


class CaseStore(SqliteStore):
    """Seal the product's KV after case setup while leaving its audit writable."""

    sealed = False

    def _check_write(self) -> None:
        if self.sealed:
            raise ProtocolError("Knowledge writes are disabled during inference")

    def kv_set(self, scope: str, key: str, value: str) -> None:
        self._check_write()
        super().kv_set(scope, key, value)

    def kv_delete(self, scope: str, key: str) -> None:
        self._check_write()
        super().kv_delete(scope, key)

    def kv_delete_prefix(self, scope: str, prefix: str) -> int:
        self._check_write()
        return super().kv_delete_prefix(scope, prefix)


class RecordedTools(ToolRegistry):
    def __init__(self):
        super().__init__([RunSQL(), ExploreSchema()])
        self.events: list[dict] = []

    async def dispatch(self, name, args, ctx, call_id):
        result = await super().dispatch(name, args, ctx, call_id)
        self.events.append({"name": name, "arguments": args, **asdict(result)})
        return result


def prompt_evidence(
    case: Case,
    condition: str,
    wire: list[dict],
    specs: list[dict],
    empty_section: str,
    seeded_section: str,
) -> dict:
    if [message["role"] for message in wire] != ["system", "user"]:
        raise ProtocolError(
            "First request must contain one system and one user message"
        )
    if tuple(tool["function"]["name"] for tool in specs) != TOOLS:
        raise ProtocolError("Unexpected tool registry in first request")
    system, user = (message["content"] for message in wire)
    if user != user_text(case.question, case.evidence, condition):
        raise ProtocolError("Unexpected user message in first request")
    policy = "\n\n" + seeded_section
    if not system.endswith(policy):
        raise ProtocolError("System message differs from the scoped term prompt")
    expected_line = f"- **{EVIDENCE_LABEL}** [이 채널]: {case.evidence}"
    if condition == "A":
        covered = expected_line in seeded_section and system.count(case.evidence) == 1
        valid = covered and case.evidence not in user
    else:
        valid = case.evidence not in system and user.count(case.evidence) == int(
            condition == "B"
        )
    if not valid:
        raise ProtocolError(f"Unexpected evidence placement for condition {condition}")

    return {
        "system": system,
        "user": user,
        "evidence_role": {"A": "system", "D": None, "B": "user"}[condition],
        "evidence_digest": json_digest(case.evidence) if condition != "D" else None,
        "base_system_digest": json_digest(
            system[: -len(policy)] + "\n\n" + empty_section
        ),
        "tool_specs_digest": json_digest(specs),
        "knowledge_entries": int(condition == "A"),
    }


async def run_case(
    case: Case,
    db: Path,
    files: CaseFiles,
    budgets: Budgets,
    profile: dict[str, str],
    transport: Transport,
    api_key: str,
) -> dict:
    started = time.monotonic()
    condition = files.condition
    trace: dict = {}
    answer: str | None = None
    transcript: list[dict] = []
    tools = RecordedTools()
    store = CaseStore()
    explorer = ReadOnlyExplorer(db, budgets.sql_timeout)
    empty_section = seeded_section = ""

    def observe(wire: list[dict], specs: list[dict]) -> None:
        trace.update(
            prompt_evidence(case, condition, wire, specs, empty_section, seeded_section)
        )

    def save_attempt(calls: list[dict]) -> None:
        files.save_attempt(trace, calls)

    llm = MeasuredLLM(
        model=profile["model"],
        api_base=profile.get("api_base"),
        api_key=api_key,
        timeout=budgets.call_timeout,
        max_calls=budgets.max_calls,
        max_output_tokens=budgets.max_output_tokens,
        transport=transport,
        observe_first_request=observe,
        save_attempt=save_attempt,
    )

    async def generate() -> str:
        nonlocal empty_section, seeded_section
        identity = Identity(
            user_id="benchmark",
            guild_id=f"bird-{case.question_id}-{condition}",
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
        scope = identity.kv_scope
        if store.kv_list_prefix(scope, "") or ctx.session.history():
            raise ProtocolError("A new case must start with an empty KV and session")
        section_args = (store, scope, identity.effective_channel_id, identity.user_id)
        empty_section = build_prompt_section(*section_args)
        if condition == "A":
            result = await ctx.tools.dispatch(
                "term_custom",
                {
                    "term": EVIDENCE_LABEL,
                    "definition": case.evidence,
                    "layer": "team",
                    "inferred": False,
                },
                ctx,
                "cmd:term_custom",
            )
            if result.is_error or len(store.kv_list_prefix(scope, "cterm:")) != 1:
                raise ProtocolError("Could not seed the case through term_custom")
        seeded_section = build_prompt_section(*section_args)
        store.sealed = True
        ctx.tools = tools
        try:
            return await agent_loop(
                ctx, user_text(case.question, case.evidence, condition)
            )
        finally:
            transcript.extend(asdict(message) for message in ctx.session.history())

    result = {"status": "failed"}
    try:
        answer = await asyncio.wait_for(generate(), timeout=budgets.case_timeout)
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
        if error.status_code in (401, 403, 404):
            raise ProtocolError(
                f"Provider rejected credentials or model (HTTP {error.status_code})"
            ) from None
        result["failure"] = {
            "stage": "generation",
            "reason": error.reason,
            "error_type": error.error_type,
            "status_code": error.status_code,
        }
    finally:
        store.close()
        await asyncio.to_thread(explorer.close)
    result.update(
        first_request=trace or None,
        calls=llm.calls,
        seconds=time.monotonic() - started,
        answer=answer,
        transcript=transcript,
        tools=tools.events,
    )
    return files.finish(result)


async def run_evaluation(
    output: Path,
    manifest: dict,
    snapshot: dict,
    cases: list[Case],
    databases: dict[str, Path],
    transport: Transport,
    api_key: str,
    stop: asyncio.Event,
    *,
    resume: bool,
) -> dict:
    check_settings(manifest, snapshot)
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
                            api_key,
                        )
                        status("running")
                    except Exception as error:  # noqa: BLE001 - drain workers before propagating
                        failures.append(error)
                        stop.set()
                        return

            await asyncio.gather(*(worker() for _ in range(budgets.concurrency)))

        order = snapshot["question_order"]
        pilot, rest = order[: budgets.pilot_questions], order[budgets.pilot_questions :]
        status("running")
        await batch(pilot)
        if not stop.is_set() and not any(
            call["status"] == "ok"
            for question_id in pilot
            for call in records[question_id]["calls"]
        ):
            failures.append(ProtocolError("Pilot had no usable provider response"))
            stop.set()
        await batch(rest)
        if failures:
            status("blocked")
            raise failures[0]
        return status("completed" if len(records) == len(save) else "interrupted")
