"""Oracle routing, safety, and clarification regressions without network access."""

import asyncio

import pytest
from sqlalchemy.engine import make_url

from lang2sql.adapters.db.dsn_builder import assemble
from lang2sql.adapters.db.factory import build_explorer
from lang2sql.adapters.llm.fake import FakeLLM
from lang2sql.core.identity import Identity
from lang2sql.core.ports.safety import SafetyContext
from lang2sql.core.types import Completion, Role, ToolCall
from lang2sql.harness.loop import agent_loop
from lang2sql.harness.system_prompt import build_system_prompt
from lang2sql.safety.pipeline import SafetyPipeline
from lang2sql.tenancy.concierge import ContextConcierge
from lang2sql.tools.run_sql import RunSQL
from lang2sql.tools.semantic_federation import build_prompt_section


def test_oracle_form_roundtrips_service_and_credentials():
    url = make_url(
        assemble(
            "oracle",
            {
                "host": "localhost:11521",
                "service_name": "FREEPDB1",
                "user": "reader",
                "password": "p@ss /?#",
            },
        ).dsn
    )
    assert url.drivername == "oracle+oracledb"
    assert url.password == "p@ss /?#"
    assert url.port == 11521
    assert url.query == {"service_name": "FREEPDB1"}
    explorer = build_explorer("oracle://u:p@host/?service_name=FREEPDB1")
    assert explorer.dialect == "oracle"
    assert explorer._engine is None


@pytest.mark.parametrize(
    "sql,expected",
    [
        ("SELECT * FROM orders", "SELECT * FROM orders\nFETCH FIRST 7 ROWS ONLY"),
        ("SELECT * FROM orders;", "SELECT * FROM orders\nFETCH FIRST 7 ROWS ONLY"),
        (
            "SELECT * FROM orders FETCH FIRST 3 ROWS ONLY",
            "SELECT * FROM orders FETCH FIRST 3 ROWS ONLY",
        ),
        (
            "SELECT * FROM (SELECT * FROM orders FETCH FIRST 3 ROWS ONLY)",
            "SELECT * FROM (SELECT * FROM orders FETCH FIRST 3 ROWS ONLY)\nFETCH FIRST 7 ROWS ONLY",
        ),
        (
            "SELECT * FROM orders OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY",
            "SELECT * FROM orders OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY",
        ),
    ],
)
def test_oracle_row_limit(sql, expected):
    decision = SafetyPipeline().evaluate(
        sql, SafetyContext(row_limit=7, extras={"dialect": "oracle"})
    )
    assert decision.sql == expected


class Explorer:
    dialect = "oracle"

    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    async def list_tables(self):
        return []

    async def execute(self, sql, limit=1000):
        self.calls.append(sql)
        if self.fail:
            raise PermissionError("ORA-01031: insufficient privileges")
        return [{"revenue": 2000000}]


def test_run_sql_passes_dialect_and_records_db_failure():
    async def run():
        explorer = Explorer(fail=True)
        concierge = ContextConcierge(llm=FakeLLM(), explorer=explorer)
        ctx = await concierge.build_context(Identity(user_id="sales"))
        with pytest.raises(PermissionError):
            await RunSQL().run({"sql": "SELECT * FROM salaries"}, ctx)
        assert explorer.calls[0].endswith("FETCH FIRST 1000 ROWS ONLY")
        audit = await concierge.store.query("sales")
        assert audit[0].action == "run_sql_failed"
        assert audit[0].detail["error_type"] == "PermissionError"
        concierge.store.close()

    asyncio.run(run())


@pytest.mark.parametrize("sql_first", [True, False])
def test_clarification_cancels_entire_batch_and_resumes_next_turn(sql_first):
    class Model:
        n = 0

        async def complete(self, messages, tools=()):
            self.n += 1
            if self.n == 1:
                calls = [
                    ToolCall("sql", "run_sql", {"sql": "SELECT * FROM orders"}),
                    ToolCall("ask", "ask_user", {"question": "충성고객의 기준은?"}),
                ]
                return Completion(tool_calls=calls if sql_first else calls[::-1])
            if self.n == 2:
                return Completion(
                    tool_calls=[
                        ToolCall(
                            "confirmed",
                            "run_sql",
                            {"sql": "SELECT SUM(gross_amount) FROM orders"},
                        )
                    ]
                )
            return Completion(content="2000000원")

    async def run():
        explorer = Explorer()
        model = Model()
        concierge = ContextConcierge(llm=model, explorer=explorer)
        ctx = await concierge.build_context(Identity(user_id="sales"))
        answer = await agent_loop(ctx, "충성고객 매출")
        assert "기준" in answer
        assert explorer.calls == []
        assert model.n == 1
        tool_messages = [m for m in ctx.session.history() if m.role == Role.TOOL]
        assert {m.tool_call_id for m in tool_messages} == {"sql", "ask"}
        assert await agent_loop(ctx, "전체 고객으로 해줘") == "2000000원"
        assert len(explorer.calls) == 1
        concierge.store.close()

    asyncio.run(run())


def test_prompt_has_one_clarification_policy_and_oracle_dialect():
    async def run():
        concierge = ContextConcierge(llm=FakeLLM(), explorer=Explorer())
        ctx = await concierge.build_context(Identity(user_id="sales"))
        prompt = await build_system_prompt(ctx)
        assert "FETCH FIRST" in prompt
        section = build_prompt_section(concierge.store, "g", "sales", "u")
        assert "SQL을 실행하기 전에 ask_user" in section
        assert "가장 합리적인 해석으로 SQL을 실행" not in prompt
        concierge.store.close()

    asyncio.run(run())


def test_oracle_fetch_in_literal_or_comment_does_not_disable_cap():
    for sql in [
        "SELECT 'FETCH FIRST 1 ROWS ONLY' AS note FROM dual",
        "SELECT * FROM orders -- FETCH FIRST 1 ROWS ONLY",
    ]:
        decision = SafetyPipeline().evaluate(
            sql, SafetyContext(row_limit=7, extras={"dialect": "oracle"})
        )
        assert decision.sql.endswith("\nFETCH FIRST 7 ROWS ONLY")


def test_oracle_existing_fetch_trailing_semicolon_is_removed():
    decision = SafetyPipeline().evaluate(
        "SELECT * FROM orders FETCH FIRST 1 ROW ONLY;",
        SafetyContext(extras={"dialect": "oracle"}),
    )
    assert decision.sql == "SELECT * FROM orders FETCH FIRST 1 ROW ONLY"


def test_qualified_describe_and_sample_use_configured_schema(tmp_path):
    from sqlalchemy import create_engine, text
    from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer

    url = f"sqlite:///{tmp_path / 'db.sqlite'}"
    engine = create_engine(url)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE orders (amount INTEGER)"))
        conn.execute(text("INSERT INTO orders VALUES (42)"))
    explorer = SqlAlchemyExplorer(url, schema="main")
    assert asyncio.run(explorer.describe_table("main.orders")).name == "orders"
    assert asyncio.run(explorer.sample_rows("orders")) == [{"amount": 42}]
    engine.dispose()
    explorer._engine.dispose()


def test_access_denial_stops_retries_and_remaining_batch():
    from lang2sql.core.ports.explorer import DataAccessError

    class DeniedExplorer(Explorer):
        async def execute(self, sql, limit=1000):
            self.calls.append(sql)
            raise DataAccessError("ORA-00942: missing or unauthorized")

    class Model:
        async def complete(self, messages, tools=()):
            return Completion(
                tool_calls=[
                    ToolCall("denied", "run_sql", {"sql": "SELECT * FROM salaries"}),
                    ToolCall(
                        "fallback", "run_sql", {"sql": "SELECT * FROM other_salaries"}
                    ),
                ]
            )

    async def run():
        explorer = DeniedExplorer()
        concierge = ContextConcierge(llm=Model(), explorer=explorer)
        ctx = await concierge.build_context(Identity(user_id="sales"))
        answer = await agent_loop(ctx, "급여 조회")
        assert "권한이 없거나" in answer
        assert len(explorer.calls) == 1
        tools = [m for m in ctx.session.history() if m.role == Role.TOOL]
        assert [m.tool_call_id for m in tools] == ["denied", "fallback"]
        assert "Not executed" in tools[-1].content
        assert (await concierge.store.query("sales"))[0].action == "run_sql_failed"
        concierge.store.close()

    asyncio.run(run())


@pytest.mark.parametrize("code,translated", [(942, True), (1031, True), (904, False)])
def test_oracle_access_errors_are_distinct_from_sql_errors(code, translated):
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from sqlalchemy.exc import DatabaseError
    from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer
    from lang2sql.core.ports.explorer import DataAccessError

    explorer = SqlAlchemyExplorer(
        "oracle+oracledb://u:p@localhost/?service_name=FREEPDB1"
    )
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.side_effect = (
        DatabaseError(
            "SELECT x FROM t",
            {},
            Exception(SimpleNamespace(code=code)),
        )
    )
    explorer._engine = engine
    with pytest.raises(DataAccessError if translated else DatabaseError):
        asyncio.run(explorer.execute("SELECT x FROM t"))
