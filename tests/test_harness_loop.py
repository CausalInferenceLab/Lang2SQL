"""Agent loop and natural-language frontend regression tests."""

from __future__ import annotations

import asyncio
import json
import re

import pytest

from lang2sql.adapters.db.postgres_explorer import PostgresExplorer
from lang2sql.adapters.llm.fake import FakeLLM
from lang2sql.adapters.llm.openai_ import OpenAILLM
from lang2sql.core.identity import Identity
from lang2sql.core.ports.explorer import Table
from lang2sql.core.types import Completion, Role, ToolCall
from lang2sql.frontends.cli import app
from lang2sql.frontends.discord.commands import CommandHandlers
from lang2sql.harness.context import HarnessContext
from lang2sql.harness.loop import agent_loop
from lang2sql.harness.session import Session
from lang2sql.harness.tool_registry import ToolRegistry
from lang2sql.tenancy.concierge import ContextConcierge
from lang2sql.tools.explore_schema import ExploreSchema
from lang2sql.tools.ping import Ping


def _ctx() -> HarnessContext:
    identity = Identity(user_id="tester")
    return HarnessContext(
        identity=identity,
        llm=FakeLLM(),
        tools=ToolRegistry([Ping()]),
        session=Session(identity=identity),
    )


def test_loop_runs_tool_then_answers():
    ctx = _ctx()
    answer = asyncio.run(agent_loop(ctx, "hello"))
    assert "pong" in answer  # tool ran and its result reached the final answer


def test_transcript_shape():
    ctx = _ctx()
    asyncio.run(agent_loop(ctx, "hello"))
    roles = [m.role for m in ctx.session.history()]
    # user → assistant(tool_call) → tool → assistant(final)
    assert roles == [Role.USER, Role.ASSISTANT, Role.TOOL, Role.ASSISTANT]


def test_tool_call_id_is_stamped():
    ctx = _ctx()
    asyncio.run(agent_loop(ctx, "hello"))
    tool_msgs = [m for m in ctx.session.history() if m.role == Role.TOOL]
    assert tool_msgs and tool_msgs[0].tool_call_id


def test_schema_batch_uses_the_catalog_from_the_system_prompt():
    class CountingExplorer(PostgresExplorer):
        catalog_reads = 0

        async def list_tables(self):
            self.catalog_reads += 1
            return await super().list_tables()

    class SchemaLLM:
        async def complete(self, messages, tools=()):
            if messages[-1].role == Role.TOOL:
                assert "- amount: numeric" in messages[-1].content
                assert "- email: text" in messages[-1].content
                return Completion(content="Found both tables.")
            assert "## Known tables\npublic.orders, public.users" in messages[0].content
            return Completion(
                tool_calls=[
                    ToolCall(
                        id="schema",
                        name="explore_schema",
                        arguments={"tables": ["public.orders", "public.users"]},
                    )
                ]
            )

    ctx = _ctx()
    explorer = CountingExplorer("postgresql://ignored")
    ctx.explorer = explorer
    ctx.llm = SchemaLLM()
    ctx.tools = ToolRegistry([ExploreSchema()])

    assert asyncio.run(agent_loop(ctx, "Show the schema for orders and users.")) == (
        "Found both tables."
    )
    assert explorer.catalog_reads == 1


def test_scope_chain_orders_narrow_to_wide():
    ident = Identity(user_id="u", guild_id="g", channel_id="c", thread_id="t")
    levels = [s.level.value for s in ident.scope_chain()]
    assert levels == ["thread", "team", "org", "builtin"]


class RecordingExplorer(PostgresExplorer):
    def __init__(self):
        super().__init__("postgresql://ignored")
        self.described = []
        self.executed = []

    async def describe_table(self, name):
        self.described.append(name)
        return await super().describe_table(name)

    async def execute(self, sql, limit=1000):
        self.executed.append(sql)
        return await super().execute(sql, limit)


class ScriptedOpenAI(OpenAILLM):
    """Record the actual API payload without sending requests."""

    def __init__(self, *replies):
        super().__init__(api_key="test")
        self.requests = []
        self.replies = replies

    def _post(self, payload):
        self.requests.append(payload)
        assert len(self.requests) <= len(self.replies), "Unexpected model request"
        content, calls = self.replies[len(self.requests) - 1]
        return {
            "choices": [
                {
                    "message": {
                        "content": content,
                        "tool_calls": [
                            {
                                "id": call_id,
                                "function": {
                                    "name": name,
                                    "arguments": json.dumps(args),
                                },
                            }
                            for call_id, name, args in calls
                        ],
                    }
                }
            ]
        }


def _calls(*calls):
    return "", calls


def _answer(text):
    return text, ()


def _submission(sql, output="result"):
    return _answer(json.dumps({"sql": sql, "output": output}))


_ACCEPT = _answer('{"status":"accept"}')
_FORCED = {"type": "function", "function": {"name": "explore_schema"}}
_SQL = "SELECT amount FROM orders"
_STALE_SQL = "SELECT guessed_amount FROM orders"
_EXECUTE = ("sql", "run_sql", {"sql": _STALE_SQL})
_SCHEMA = ("columns", "explore_schema", {"tables": ["public.orders"]})


def _context_for(llm, *, max_turns=8):
    concierge = ContextConcierge(
        llm=llm, explorer=RecordingExplorer(), max_turns=max_turns
    )
    return asyncio.run(concierge.build_context(Identity(user_id="analyst")))


def _choices(llm):
    return [request.get("tool_choice") for request in llm.requests]


def _tool_results(request):
    return {
        message["tool_call_id"]: message["content"]
        for message in request["messages"]
        if message["role"] == "tool"
    }


def test_run_sql_waits_for_columns_before_executing():
    llm = ScriptedOpenAI(
        _calls(_EXECUTE),
        _calls(_SCHEMA),
        _calls(("rewritten", "run_sql", {"sql": _SQL})),
        _answer("Order totals are available."),
        _submission(_SQL),
        _ACCEPT,
    )
    ctx = _context_for(llm)

    answer = asyncio.run(agent_loop(ctx, "Show order amounts"))
    assert "2 row(s):" in answer and answer.endswith("SQL: " + _SQL)
    assert _choices(llm) == [None, _FORCED, None, None, None, None]
    assert "SQL withheld" in _tool_results(llm.requests[1])["sql"]
    assert "- amount: numeric" in _tool_results(llm.requests[2])["columns"]
    assert ctx.explorer.executed == [_SQL + "\nLIMIT 1000"] * 3


@pytest.mark.parametrize(
    "output", ["{sql}", "```sql\n{sql}\n```", "**최종 SQL:**\n```sql\n{sql}\n```"]
)
def test_final_sql_is_withheld_until_the_model_has_columns(output):
    draft = output.format(sql=_STALE_SQL)
    revised = output.format(sql=_SQL)
    llm = ScriptedOpenAI(
        _answer(draft),
        _calls(_SCHEMA),
        _answer(revised),
        _ACCEPT,
    )
    ctx = _context_for(llm)

    assert asyncio.run(agent_loop(ctx, "Write the order amounts query")) == revised
    assert _choices(llm) == [None, _FORCED, None, None]
    assert llm.requests[1]["messages"][-1] == {"role": "assistant", "content": draft}
    assert not any(_STALE_SQL in message.content for message in ctx.session.history())
    schema = [
        tool["function"]["parameters"]
        for request in llm.requests
        for tool in request.get("tools", [])
        if tool["function"]["name"] == "explore_schema"
    ]
    original, recovery, restored = schema
    assert "required" not in original and "table" in original["properties"]
    assert recovery["required"] == ["tables"]
    assert recovery["properties"]["tables"]["minItems"] == 1
    assert "table" not in recovery["properties"]
    assert original == restored


def test_evaluator_uses_its_sql_output_contract():
    def extract_sql(answer):
        blocks = re.findall(r"```sql\s*(.*?)```", answer, re.I | re.S)
        if len(blocks) != 1:
            raise ValueError("Expected one SQL block")
        return blocks[0].strip()

    initial = f"Final query:\n```sql\n{_STALE_SQL}\n```"
    revised = f"Final query:\n```sql\n{_SQL}\n```"
    llm = ScriptedOpenAI(
        _answer(initial),
        _calls(_SCHEMA),
        _answer("The orders table is available."),
        _answer(revised),
        _ACCEPT,
    )
    ctx = _context_for(llm)

    assert (
        asyncio.run(agent_loop(ctx, "Write SQL", extract_final_sql=extract_sql))
        == revised
    )
    assert _choices(llm) == [None, _FORCED, None, None, None]
    assert not any(
        message.content == "The orders table is available."
        for message in ctx.session.history()
    )


@pytest.mark.parametrize("schema_first", [True, False])
def test_schema_and_sql_in_the_same_response_use_the_same_model_context(schema_first):
    batch = [_SCHEMA, _EXECUTE] if schema_first else [_EXECUTE, _SCHEMA]
    llm = ScriptedOpenAI((_STALE_SQL, batch), _answer(_SQL), _ACCEPT)
    ctx = _context_for(llm)

    assert asyncio.run(agent_loop(ctx, "Show order amounts")) == _SQL
    assert _choices(llm) == [None, None, None]
    assert ctx.explorer.executed == [_SQL + "\nLIMIT 1000"]
    results = _tool_results(llm.requests[1])
    assert list(results) == [call[0] for call in batch]
    assert "- amount: numeric" in results["columns"]
    assert "SQL withheld" in results["sql"]
    ctx.session.compress()
    assert [message.content for message in ctx.session.history()] == [
        "Show order amounts",
        _SQL,
    ]


def test_recovery_rejects_list_only_legacy_and_empty_calls_and_uses_partial_success():
    llm = ScriptedOpenAI(
        _calls(("list", "explore_schema", {})),
        _answer(_STALE_SQL),
        _calls(("recovery-list", "explore_schema", {})),
        _calls(("legacy", "explore_schema", {"table": "orders"})),
        _calls(("empty", "explore_schema", {"tables": []})),
        _calls(("failed", "explore_schema", {"tables": ["missing"]})),
        _calls(("partial", "explore_schema", {"tables": ["missing", "public.orders"]})),
        _answer(_SQL),
        _ACCEPT,
    )
    ctx = _context_for(llm, max_turns=9)

    assert asyncio.run(agent_loop(ctx, "Show order amounts")) == _SQL
    assert _choices(llm) == [None, None, *([_FORCED] * 5), None, None]
    list_result, draft = llm.requests[2]["messages"][-2:]
    assert list_result["tool_call_id"] == "list"
    assert draft == {"role": "assistant", "content": _STALE_SQL}
    results = _tool_results(llm.requests[-2])
    assert results["list"] == "Tables:\n- public.orders\n- public.users"
    assert all(
        "non-empty" in results[key] for key in ("recovery-list", "legacy", "empty")
    )
    assert "unknown table: missing" in results["failed"]
    assert "unknown table: missing" in results["partial"]
    assert "- amount: numeric" in results["partial"]
    assert ctx.explorer.described == ["missing", "missing", "public.orders"]


@pytest.mark.parametrize(
    ("tables", "reason"),
    [
        (["missing"], "no table columns obtained"),
        (["orders"], "rewritten final answer"),
    ],
)
def test_recovery_respects_existing_turn_budget(tables, reason):
    llm = ScriptedOpenAI(
        _answer(_STALE_SQL), _calls(("retry", "explore_schema", {"tables": tables}))
    )
    ctx = _context_for(llm, max_turns=2)
    answer = asyncio.run(agent_loop(ctx, "Show order amounts"))

    assert reason in answer
    assert _STALE_SQL not in answer
    assert _choices(llm) == [None, _FORCED]
    assert ctx.session.history()[-1].content == answer


def test_legacy_adapter_returns_reason_when_required_tool_choice_is_unavailable():
    class LegacyLLM:
        async def complete(self, messages, tools=()):
            assert messages[-1].role == Role.USER
            return Completion(content=_STALE_SQL)

    ctx = _context_for(LegacyLLM())
    answer = asyncio.run(agent_loop(ctx, "Show order amounts"))

    assert "model adapter cannot require schema lookup" in answer
    assert ctx.session.history()[-1].content == answer


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT (SELECT amount FROM orders)",
        "/* order totals */ WITH amounts AS NOT MATERIALIZED "
        '(SELECT amount FROM "orders") SELECT * FROM amounts',
    ],
)
def test_nested_table_sources_trigger_recovery(sql):
    llm = ScriptedOpenAI(
        _calls(("draft", "run_sql", {"sql": sql})),
        _calls(_SCHEMA),
        _answer(_SQL),
        _ACCEPT,
    )
    ctx = _context_for(llm)

    assert asyncio.run(agent_loop(ctx, "Show order amounts")) == _SQL
    assert _choices(llm) == [None, _FORCED, None, None]
    assert ctx.explorer.executed == [_SQL + "\nLIMIT 1000"]


def test_schema_response_without_columns_requires_recovery():
    class EmptyTableExplorer(RecordingExplorer):
        async def describe_table(self, name):
            if name == "empty":
                return Table(name="empty")
            return await super().describe_table(name)

    llm = ScriptedOpenAI(
        _calls(("empty", "explore_schema", {"table": "empty"})),
        _answer(_STALE_SQL),
        _calls(_SCHEMA),
        _answer(_SQL),
        _ACCEPT,
    )
    ctx = _context_for(llm)
    ctx.explorer = EmptyTableExplorer()

    assert asyncio.run(agent_loop(ctx, "Show order amounts")) == _SQL
    assert "(no columns)" in _tool_results(llm.requests[1])["empty"]
    assert _choices(llm) == [None, None, _FORCED, None, None]


def test_prompt_with_actual_columns_allows_sql_on_the_first_call():
    llm = ScriptedOpenAI(
        _calls(("sql", "run_sql", {"sql": _SQL})), _answer(_SQL), _ACCEPT
    )
    ctx = _context_for(llm)
    ctx.store.kv_set(ctx.identity.kv_scope, "schema_relationships", '["orders"]')

    assert asyncio.run(agent_loop(ctx, "Show order amounts")) == _SQL
    assert _choices(llm) == [None, None, None]
    assert "- amount" in llm.requests[0]["messages"][0]["content"]
    assert ctx.explorer.executed == [_SQL + "\nLIMIT 1000"] * 2


def test_column_results_are_scoped_to_the_current_user_turn():
    llm = ScriptedOpenAI(
        _calls(_SCHEMA),
        _answer(_SQL),
        _ACCEPT,
        _answer(_STALE_SQL),
        _calls(("next-turn-columns", "explore_schema", {"tables": ["orders"]})),
        _answer(_SQL),
        _ACCEPT,
    )
    ctx = _context_for(llm)

    assert asyncio.run(agent_loop(ctx, "Show order amounts")) == _SQL
    assert asyncio.run(agent_loop(ctx, "Show order amounts again")) == _SQL
    assert _choices(llm) == [None, None, None, None, _FORCED, None, None]


@pytest.mark.parametrize(
    "answer",
    [
        "Hello. How can I help?",
        "Which definition of revenue should I use?",
        f"For example, you can write:\n```sql\n{_SQL}\n```",
        "Select a date from the list.",
        "SELECT 'FROM orders' AS example",
        "SELECT EXTRACT(YEAR FROM CURRENT_DATE)",
        "SELECT 42 IS DISTINCT FROM 7",
        "SELECT * FROM (SELECT 42 AS amount)",
    ],
)
def test_general_answers_remain_automatic(answer):
    sql_answer = answer.startswith("SELECT ")
    llm = ScriptedOpenAI(_answer(answer), _ACCEPT)
    ctx = _context_for(llm)

    assert asyncio.run(agent_loop(ctx, "Help me understand my options")) == answer
    assert len(llm.requests) == (2 if sql_answer else 1)
    assert all("tool_choice" not in request for request in llm.requests)


def test_run_sql_allows_queries_without_table_sources():
    sql = "SELECT EXTRACT(YEAR FROM CURRENT_DATE)"
    llm = ScriptedOpenAI(
        _calls(("current-year", "run_sql", {"sql": sql})), _submission(sql), _ACCEPT
    )
    ctx = _context_for(llm)

    answer = asyncio.run(agent_loop(ctx, "Read the current year"))
    assert answer == f"1 row(s):\nresult\n---\n1\nSQL: {sql}"
    assert _choices(llm) == [None, None, None]
    assert ctx.explorer.executed == [sql + "\nLIMIT 1000"] * 3


def test_term_custom_and_enrich_keep_their_existing_calls():
    llm = ScriptedOpenAI(
        _calls(
            (
                "term",
                "term_custom",
                {
                    "term": "net revenue",
                    "definition": "Exclude refunds",
                    "layer": "user",
                },
            ),
            ("enrich", "enrich_schema", {}),
        ),
        _answer('{"columns": {"orders.amount": "Order total"}, "relationships": []}'),
        _answer("Saved the definition."),
    )
    ctx = _context_for(llm)

    assert asyncio.run(agent_loop(ctx, "Save net revenue and enrich the schema.")) == (
        "Saved the definition."
    )
    assert _choices(llm) == [None, None, None]
    assert "tools" not in llm.requests[1]
    assert "Exclude refunds" in _tool_results(llm.requests[2])["term"]
    assert "orders.amount: Order total" in _tool_results(llm.requests[2])["enrich"]


def test_cli_and_discord_natural_language_use_schema_recovery(monkeypatch, capsys):
    cli_model = ScriptedOpenAI(
        _answer(_STALE_SQL), _calls(_SCHEMA), _answer(_SQL), _ACCEPT
    )
    cli_concierge = ContextConcierge(llm=cli_model, explorer=RecordingExplorer())
    monkeypatch.setattr(app, "ContextConcierge", lambda: cli_concierge)
    assert app.main(["Show", "order", "amounts"]) == 0
    assert capsys.readouterr().out == f"{_SQL}\n"
    assert _choices(cli_model) == [None, _FORCED, None, None]
    assert cli_model.requests[0]["messages"][-1]["content"] == "Show order amounts"

    discord_model = ScriptedOpenAI(
        _answer(_STALE_SQL), _calls(_SCHEMA), _answer(_SQL), _ACCEPT
    )
    concierge = ContextConcierge(llm=discord_model, explorer=RecordingExplorer())
    identity = Identity(user_id="tester", guild_id="shop", channel_id="analytics")

    async def run_discord():
        response = await CommandHandlers(concierge).query(
            identity, "Show order amounts"
        )
        session = await concierge.store.load(identity.session_key())
        return response, session

    response, session = asyncio.run(run_discord())
    assert response.text == _SQL
    assert _choices(discord_model) == [None, _FORCED, None, None]
    assert [message.content for message in session.history()] == [
        "Show order amounts",
        _SQL,
    ]


def _sqlite_context(tmp_path, llm, *, max_turns=8):
    import sqlite3

    from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer

    path = tmp_path / "orders.db"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE orders(amount INTEGER, status TEXT)")
    explorer = SqlAlchemyExplorer(f"sqlite:///file:{path}?mode=ro&uri=true")
    concierge = ContextConcierge(llm=llm, explorer=explorer, max_turns=max_turns)
    ctx = asyncio.run(concierge.build_context(Identity(user_id="analyst")))
    return ctx


def test_final_db_error_allows_exploration_then_rechecks_the_revision(tmp_path):
    columns = _calls(("lookup", "explore_schema", {"tables": ["orders"]}))
    probe = _calls(("probe", "run_sql", {"sql": "SELECT DISTINCT status FROM orders"}))
    llm = ScriptedOpenAI(
        columns, _answer(_STALE_SQL), columns, probe, _answer(_SQL), _ACCEPT
    )
    ctx = _sqlite_context(tmp_path, llm)

    assert asyncio.run(agent_loop(ctx, "List the order amounts")) == _SQL
    feedback = llm.requests[2]["messages"][-2:]
    assert feedback[0]["content"] == _STALE_SQL
    assert "no such column: guessed_amount" in feedback[1]["content"]
    assert {"run_sql", "explore_schema"} <= {
        tool["function"]["name"] for tool in llm.requests[2]["tools"]
    }
    events = asyncio.run(ctx.audit.query("analyst"))
    checks = [event.detail for event in events if event.action == "check_final_sql"]
    assert len(checks) == 2
    assert any(
        check["error"] and check["candidate_sql"] == _STALE_SQL for check in checks
    )
    assert any(
        check["checked_sql"] == f"EXPLAIN QUERY PLAN {_SQL}\nLIMIT 1000"
        and check["error"] is None
        for check in checks
    )
    assert ctx.session.history()[-1].content == _SQL


def test_revised_sql_is_withheld_if_it_still_fails(tmp_path):
    llm = ScriptedOpenAI(_answer("SELECT missing"), _answer("SELECT missing_again"))
    ctx = _sqlite_context(tmp_path, llm)
    answer = asyncio.run(agent_loop(ctx, "Get an amount"))
    assert answer.startswith("(SQL withheld: revised SQL failed validation")
    assert "no such column" in answer
    assert len(llm.requests) == 2


def test_no_calls_left_for_db_repair(tmp_path):
    llm = ScriptedOpenAI(_answer("SELECT missing"))
    ctx = _sqlite_context(tmp_path, llm, max_turns=1)
    answer = asyncio.run(agent_loop(ctx, "Get an amount"))
    assert "call budget exhausted" in answer
    assert len(llm.requests) == 1


def test_unsafe_submission_fails_before_db(tmp_path):
    llm = ScriptedOpenAI(
        _answer("SQL: DROP TABLE orders"), _answer("SELECT 1"), _ACCEPT
    )
    ctx = _sqlite_context(tmp_path, llm)
    assert asyncio.run(agent_loop(ctx, "Show one")) == "SELECT 1"
    assert "BLOCKED by whitelist" in llm.requests[1]["messages"][-1]["content"]
    checks = asyncio.run(ctx.audit.query("analyst"))
    assert any(
        e.detail["candidate_sql"] == "DROP TABLE orders"
        and e.detail.get("checked_sql") is None
        for e in checks
    )


_REVISE = _answer(
    json.dumps(
        {
            "status": "revise",
            "requirement": "paid orders only",
            "clause": "missing WHERE status = 'paid'",
            "evidence": "The business definition excludes unpaid orders.",
        }
    )
)


def test_semantic_review_requests_one_grounded_rewrite(tmp_path):
    question = "List recognized amounts."
    revised = _SQL + " WHERE status = 'paid'"
    columns = _calls(("schema", "explore_schema", {"tables": ["orders"]}))
    final = f"```sql\n{revised}\n```"
    llm = ScriptedOpenAI(columns, _answer(_SQL), _REVISE, _answer(final), _ACCEPT)
    ctx = _sqlite_context(tmp_path, llm, max_turns=5)
    asyncio.run(
        ctx.tools.dispatch(
            "term_custom",
            {
                "term": "recognized",
                "definition": "paid orders only",
                "layer": "user",
            },
            ctx,
            "seed",
        )
    )

    answer = asyncio.run(agent_loop(ctx, question))
    assert answer == final == ctx.session.history()[-1].content
    for request, sql in [(llm.requests[2], _SQL), (llm.requests[4], revised)]:
        assert "tools" not in request
        system, user = request["messages"]
        assert system["role"] == "system" and user["role"] == "user"
        inputs = json.loads(user["content"])
        assert inputs["question"] == question and inputs["sql"] == sql
        assert "LIMIT 1000" not in inputs["sql"]
        context = "\n".join(inputs["schema_and_definitions"])
        assert "paid orders only" in context and "- status: TEXT" in context
    rewrite = llm.requests[3]["messages"]
    assert "missing WHERE" in rewrite[-1]["content"] and rewrite[-2]["content"] == _SQL
    events = asyncio.run(ctx.audit.query("analyst"))
    assert {
        e.detail["candidate_sql"] for e in events if e.action == "check_final_sql"
    } == {_SQL, revised}


def test_reviewer_can_request_schema_lookup_before_new_submission(tmp_path):
    needs_schema = _answer(
        json.dumps(
            {
                "status": "needs_schema",
                "tables": ["orders"],
                "evidence": "Inspect the status column for the paid filter",
            }
        )
    )
    llm = ScriptedOpenAI(
        _answer("SELECT 1"),
        needs_schema,
        _calls(("columns", "explore_schema", {"tables": ["orders"]})),
        _answer(_SQL),
        _ACCEPT,
    )
    ctx = _sqlite_context(tmp_path, llm, max_turns=5)
    assert asyncio.run(agent_loop(ctx, "List amounts")) == _SQL
    rewrite = llm.requests[2]
    assert "Inspect the status column" in rewrite["messages"][-1]["content"]
    assert {"explore_schema", "run_sql"} <= {
        tool["function"]["name"] for tool in rewrite["tools"]
    }
    assert "- amount: INTEGER" in _tool_results(llm.requests[3])["columns"]


@pytest.mark.parametrize(
    "budget, reason",
    [
        (1, "before semantic review"),
        (2, "before receiving a validated revision"),
        (3, "before semantic review"),
    ],
)
def test_shared_review_and_rewrite_budget(tmp_path, budget, reason):
    llm = ScriptedOpenAI(_answer("SELECT 1"), _REVISE, _answer("SELECT 2"))
    ctx = _sqlite_context(tmp_path, llm, max_turns=budget)
    answer = asyncio.run(agent_loop(ctx, "Show a paid amount"))
    assert "SQL withheld: call budget exhausted" in answer and reason in answer
    assert len(llm.requests) == budget


def test_one_rewrite_shared_across_db_and_semantic_checks(tmp_path):
    llm = ScriptedOpenAI(_answer("SELECT missing"), _answer("SELECT 2"), _REVISE)
    ctx = _sqlite_context(tmp_path, llm)
    answer = asyncio.run(agent_loop(ctx, "Show a paid amount"))
    assert answer.startswith("(SQL withheld: revised SQL failed validation")
    assert len(llm.requests) == 3


@pytest.mark.parametrize(
    "review",
    [
        _answer("not JSON"),
        _answer('{"status":"revise","clause":"WHERE"}'),
        _answer('{"status":"needs_schema","tables":[]}'),
        _calls(_SCHEMA),
    ],
)
def test_malformed_review_withholds_sql(tmp_path, review):
    llm = ScriptedOpenAI(_answer("SELECT 1"), review)
    ctx = _sqlite_context(tmp_path, llm)
    answer = asyncio.run(agent_loop(ctx, "Show one"))
    assert answer.startswith("(SQL withheld: incomplete semantic review")
    assert len(llm.requests) == 2


def test_review_provider_failure_withholds_sql(tmp_path):
    class FailingReview(ScriptedOpenAI):
        def _post(self, payload):
            if "tools" not in payload:
                raise RuntimeError("provider unavailable")
            return super()._post(payload)

    ctx = _sqlite_context(tmp_path, FailingReview(_answer("SELECT 1")))
    answer = asyncio.run(agent_loop(ctx, "Show one"))
    assert "model request failed during semantic review" in answer


def test_review_asks_for_one_missing_business_definition(tmp_path):
    question = "Which statuses count as recognized revenue?"
    llm = ScriptedOpenAI(
        _answer("SELECT 1"),
        _answer(json.dumps({"status": "clarify", "question": question})),
    )
    ctx = _sqlite_context(tmp_path, llm)
    answer = asyncio.run(agent_loop(ctx, "Show recognized revenue"))
    assert answer == f"❓ Awaiting user: {question}"
    assert ctx.session.history()[-1].content == answer
    assert len(llm.requests) == 2


def test_auxiliary_tool_model_calls_share_review_budget(tmp_path):
    llm = ScriptedOpenAI(
        _calls(("enrich", "enrich_schema", {})),
        _answer('{"columns": {}, "relationships": []}'),
        _answer("SELECT 1"),
    )
    ctx = _sqlite_context(tmp_path, llm, max_turns=3)
    answer = asyncio.run(agent_loop(ctx, "Inspect the schema then show one"))
    assert "budget exhausted before semantic review" in answer
    assert len(llm.requests) == 3


def test_answer_to_review_clarification_reaches_next_review(tmp_path):
    first = "Show recognized revenue."
    question = "Which statuses count as recognized?"
    definition = "Only paid orders."
    llm = ScriptedOpenAI(
        _answer("SELECT 1"),
        _answer(json.dumps({"status": "clarify", "question": question})),
        _calls(("schema", "explore_schema", {"tables": ["orders"]})),
        _answer("SELECT sum(amount) FROM orders WHERE status = 'paid'"),
        _ACCEPT,
    )
    ctx = _sqlite_context(tmp_path, llm)
    assert question in asyncio.run(agent_loop(ctx, first))
    assert "sum(amount)" in asyncio.run(agent_loop(ctx, definition))
    inputs = json.loads(llm.requests[-1]["messages"][-1]["content"])
    assert inputs["question"] == first
    assert any(
        question in text and definition in text
        for text in inputs["schema_and_definitions"]
    )


def test_sql_labeled_after_prose_is_checked(tmp_path):
    submitted = "Here is the result.\n\nFinal SQL:\n```sql\nSELECT missing\n```"
    llm = ScriptedOpenAI(_answer(submitted), _answer("SELECT 1"), _ACCEPT)
    ctx = _sqlite_context(tmp_path, llm)
    assert asyncio.run(agent_loop(ctx, "Show one")) == "SELECT 1"
    assert "no such column: missing" in llm.requests[1]["messages"][-1]["content"]


def test_result_belongs_to_reviewed_sql_not_probe_or_rejected_draft(tmp_path):
    probe = "SELECT 9 AS probe"
    draft = "SELECT 10 AS draft"
    final = "SELECT 42 AS total"
    llm = ScriptedOpenAI(
        _calls(("probe", "run_sql", {"sql": probe})),
        _answer("The total is 9."),
        _submission(draft),
        _REVISE,
        _submission(final),
        _ACCEPT,
    )
    ctx = _sqlite_context(tmp_path, llm)
    answer = asyncio.run(agent_loop(ctx, "Show the total"))
    assert answer == f"1 row(s):\ntotal\n---\n42\nSQL: {final}"
    assert ctx.session.history()[-1].content == answer
    assert [
        json.loads(llm.requests[i]["messages"][-1]["content"])["sql"] for i in (3, 5)
    ] == [draft, final]
    events = asyncio.run(ctx.audit.query("analyst"))
    assert [event.detail["sql"] for event in events if event.action == "run_sql"] == [
        final + "\nLIMIT 1000",
        probe + "\nLIMIT 1000",
    ]


def test_failed_result_execution_uses_same_repair_and_output_mode(tmp_path):
    # This constant expression prepares on SQLite but fails when evaluated.
    sql = "SELECT abs(-9223372036854775808)"
    final = "SELECT 42 AS total"
    llm = ScriptedOpenAI(_submission(sql), _ACCEPT, _submission(final, "sql"), _ACCEPT)
    ctx = _sqlite_context(tmp_path, llm)
    answer = asyncio.run(agent_loop(ctx, "Show the total"))
    assert "integer overflow" in llm.requests[2]["messages"][-1]["content"]
    assert answer == f"1 row(s):\ntotal\n---\n42\nSQL: {final}"


def test_sql_only_and_result_state_are_scoped_to_one_request(tmp_path):
    sql = "SELECT abs(-9223372036854775808)"
    next_sql = "SELECT 3 AS n WHERE 0"
    llm = ScriptedOpenAI(
        _submission(sql, "sql"),
        _ACCEPT,
        _answer(json.dumps({"sql": next_sql})),
        _ACCEPT,
    )
    ctx = _sqlite_context(tmp_path, llm)
    assert asyncio.run(agent_loop(ctx, "Write the query")) == sql
    assert asyncio.run(agent_loop(ctx, "Show the rows")) == f"(0 rows)\nSQL: {next_sql}"
    events = asyncio.run(ctx.audit.query("analyst"))
    assert [e.detail["sql"] for e in events if e.action == "run_sql"] == [
        next_sql + "\nLIMIT 1000"
    ]


def test_probe_alone_cannot_become_the_answer(tmp_path):
    llm = ScriptedOpenAI(
        _calls(("probe", "run_sql", {"sql": "SELECT 9"})), _answer("It was 9.")
    )
    ctx = _sqlite_context(tmp_path, llm, max_turns=2)
    answer = asyncio.run(agent_loop(ctx, "Show the total"))
    assert answer.startswith("(SQL withheld:") and "final SQL submission" in answer


def test_clarification_after_a_probe_is_returned_without_probe_rows(tmp_path):
    llm = ScriptedOpenAI(
        _calls(("probe", "run_sql", {"sql": "SELECT 9"})),
        _calls(("clarify", "ask_user", {"question": "Which year?"})),
    )
    ctx = _sqlite_context(tmp_path, llm)
    assert (
        asyncio.run(agent_loop(ctx, "Show the total"))
        == "❓ Awaiting user: Which year?"
    )


def test_discord_displays_only_the_result_from_the_accepted_candidate():
    probe = "SELECT 999 AS probe"
    final = "SELECT 42 AS total"
    from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer

    llm = ScriptedOpenAI(
        _calls(("probe", "run_sql", {"sql": probe})), _submission(final), _ACCEPT
    )
    concierge = ContextConcierge(llm=llm, explorer=SqlAlchemyExplorer("sqlite://"))
    answer = asyncio.run(
        CommandHandlers(concierge).query(Identity(user_id="tester"), "Get the total")
    )
    assert answer.text == f"1 row(s):\ntotal\n---\n42\nSQL: {final}"


def test_malformed_submission_requests_correction_before_using_results(tmp_path):
    sql = "SELECT 7 AS n"
    llm = ScriptedOpenAI(_answer('{"sql": null}'), _submission(sql), _ACCEPT)
    ctx = _sqlite_context(tmp_path, llm)
    assert (
        asyncio.run(agent_loop(ctx, "Get the count"))
        == f"1 row(s):\nn\n---\n7\nSQL: {sql}"
    )
    assert "expected sql text" in llm.requests[1]["messages"][-1]["content"]
