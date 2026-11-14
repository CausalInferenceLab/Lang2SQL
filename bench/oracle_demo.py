"""Oracle demo: disposable fixtures and the real Lang2SQL harness.

`seed` creates new demo users (never drops/replaces existing objects).
`verify` uses only three read-only accounts; --live uses authenticated Codex CLI.
The scripted mode proves wiring, not LLM accuracy. Both modes use a real Oracle DB.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer
from lang2sql.adapters.llm.codex import CodexLLM
from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.core.identity import Identity
from lang2sql.core.types import Completion, ToolCall
from lang2sql.harness.loop import agent_loop
from lang2sql.tenancy.concierge import ContextConcierge
from lang2sql.tools.semantic_federation import SemanticFederationTool

logger = logging.getLogger(__name__)
OWNER = "l2s_demo_owner"
TEAMS = ("sales", "finance", "hr")
REVENUE_QUESTION = "2026년 10월 매출 얼마야? 원 단위로 알려줘."
UNKNOWN_QUESTION = "2026년 10월 충성고객 매출을 알려줘."
SALARY_QUESTION = "부서별 급여 평균을 알려줘."
PERIOD = "ordered_at >= DATE '2026-10-01' AND ordered_at < DATE '2026-11-01'"
SALARY_SQL = f"SELECT department, AVG(salary) AS avg_salary FROM {OWNER}.salaries GROUP BY department"
DEFINITIONS = {
    "sales": f"주문 금액 합계 SUM(gross_amount). 테이블 {OWNER}.orders. 원 단위. 날짜는 ordered_at.",
    "finance": f"순매출 SUM(gross_amount - discount_amount - return_amount). 테이블 {OWNER}.orders. 원 단위. 날짜는 ordered_at.",
}


def seed(url: str) -> None:
    """Create a disposable owner and read-only personas, refusing existing users."""
    engine = create_engine(url)
    password = make_url(url).password
    if not password:
        raise ValueError("Demo admin URL must include a password")
    quoted_password = engine.dialect.identifier_preparer.quote_identifier(password)
    users = [OWNER, *(f"l2s_demo_{team}" for team in TEAMS)]
    try:
        with engine.connect() as conn:
            for user in users:
                exists = conn.execute(
                    text("SELECT COUNT(*) FROM all_users WHERE username = :name"),
                    {"name": user.upper()},
                ).scalar_one()
                if exists:
                    raise ValueError(
                        f"Demo user {user} already exists; use a fresh disposable container"
                    )
            for user in users:
                conn.execute(
                    text(f"CREATE USER {user} IDENTIFIED BY {quoted_password}")
                )
                conn.execute(text(f"GRANT CREATE SESSION TO {user}"))
            conn.execute(text(f"ALTER USER {OWNER} QUOTA 10M ON USERS"))
            conn.execute(
                text(
                    f"CREATE TABLE {OWNER}.orders (id NUMBER PRIMARY KEY, ordered_at DATE NOT NULL, gross_amount NUMBER NOT NULL, discount_amount NUMBER NOT NULL, return_amount NUMBER NOT NULL)"
                )
            )
            conn.execute(
                text(
                    f"CREATE TABLE {OWNER}.salaries (department VARCHAR2(30), salary NUMBER)"
                )
            )
            # Out-of-period rows catch a missing date filter.
            for row in [
                (1, "2026-10-05", 1000000, 50000, 0),
                (2, "2026-10-10", 600000, 50000, 200000),
                (3, "2026-10-20", 400000, 0, 0),
                (4, "2026-09-30", 9000000, 0, 0),
                (5, "2026-11-01", 8000000, 0, 0),
            ]:
                conn.execute(
                    text(
                        f"INSERT INTO {OWNER}.orders VALUES (:id, TO_DATE(:day, 'YYYY-MM-DD'), :gross, :discount, :returned)"
                    ),
                    dict(zip(("id", "day", "gross", "discount", "returned"), row)),
                )
            conn.execute(
                text(f"INSERT INTO {OWNER}.salaries VALUES ('sales', 3000000)")
            )
            conn.execute(
                text(f"INSERT INTO {OWNER}.salaries VALUES ('finance', 4000000)")
            )
            conn.commit()
            for team in TEAMS:
                conn.execute(text(f"GRANT SELECT ON {OWNER}.orders TO l2s_demo_{team}"))
            conn.execute(text(f"GRANT SELECT ON {OWNER}.salaries TO l2s_demo_hr"))
        logger.info("oracle seed complete users=%d", len(users))
    finally:
        engine.dispose()


class RecordingExplorer(SqlAlchemyExplorer):
    """Observe executed SQL/results at the existing explorer seam."""

    def __init__(self, url: str):
        super().__init__(url, schema=OWNER)
        self.executions: list[dict] = []

    async def execute(self, sql: str, limit: int = 1000) -> list[dict]:
        entry = {"sql": sql}
        self.executions.append(entry)
        try:
            rows = await super().execute(sql, limit)
        except Exception as exc:
            code = re.search(r"ORA-\d+", str(exc))
            entry["error"] = code.group() if code else type(exc).__name__
            raise
        entry["rows"] = rows
        return rows


class ScriptedLLM:
    """Explicit test double: no model inference or hard-coded success grading."""

    def __init__(self, call: ToolCall):
        self.call = call
        self.sent = False

    async def complete(self, messages, tools=()):
        if not self.sent:
            self.sent = True
            return Completion(tool_calls=[self.call])
        return Completion(content=messages[-1].content)


async def verify(
    url: str, live: bool, env: dict[str, str], model: str | None, repeats: int
) -> dict:
    base = make_url(url)
    report = {
        "mode": "codex_harness" if live else "scripted",
        "model": (model or "Codex CLI default") if live else None,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "cases": [],
    }
    store = SqliteStore()
    try:
        for repeat in range(repeats):
            for team, case, question in [
                ("sales", "revenue", REVENUE_QUESTION),
                ("finance", "revenue", REVENUE_QUESTION),
                ("sales", "clarification", UNKNOWN_QUESTION),
                ("hr", "salary", SALARY_QUESTION),
                ("sales", "denied", SALARY_QUESTION),
            ]:
                user = f"l2s_demo_{team}"
                reader_url = base.set(username=user).render_as_string(
                    hide_password=False
                )
                explorer = RecordingExplorer(reader_url)
                sql = SALARY_SQL
                if case == "revenue":
                    expression = (
                        "gross_amount"
                        if team == "sales"
                        else "gross_amount - discount_amount - return_amount"
                    )
                    sql = f"SELECT SUM({expression}) AS revenue FROM {OWNER}.orders WHERE {PERIOD}"
                call = ToolCall("demo", "run_sql", {"sql": sql})
                if case == "clarification":
                    call = ToolCall(
                        "demo",
                        "ask_user",
                        {"question": "충성고객의 기준이 무엇인가요?"},
                    )
                llm = CodexLLM(model=model, env=env) if live else ScriptedLLM(call)
                concierge = ContextConcierge(store=store, explorer=explorer, llm=llm)
                identity = Identity(
                    user_id=user, guild_id="oracle-demo", channel_id=team
                )
                ctx = await concierge.build_context(identity)
                try:
                    # Seed definitions through the same public tool as normal use.
                    for definition_team, definition in DEFINITIONS.items():
                        definition_ctx = await concierge.build_context(
                            Identity(
                                user_id="curator",
                                guild_id="oracle-demo",
                                channel_id=definition_team,
                            )
                        )
                        result = await SemanticFederationTool().run(
                            {
                                "term": "매출",
                                "kind": "metric",
                                "layer": "team",
                                "definition": definition,
                            },
                            definition_ctx,
                        )
                        if result.is_error:
                            raise RuntimeError(result.content)
                    await SemanticFederationTool().run(
                        {
                            "term": "급여",
                            "layer": "team",
                            "kind": "metric",
                            "definition": f"{OWNER}.salaries의 salary, 부서는 department. 부서별 평균은 AVG(salary). 접근 권한은 DB에서 결정.",
                        },
                        ctx,
                    )
                    db_user = (
                        await explorer.execute("SELECT USER AS db_user FROM dual")
                    )[0]["db_user"]
                    version = (await explorer.execute("SELECT banner FROM v$version"))[
                        0
                    ]["banner"]
                    report["database"] = version
                    explorer.executions.clear()
                    # Independently prove the server rejects a known-valid query.
                    permission_probe = None
                    if case == "denied":
                        try:
                            await explorer.execute(SALARY_SQL)
                        except Exception as exc:
                            logger.info(
                                "permission probe rejected team=%s error=%s",
                                team,
                                type(exc).__name__,
                            )
                            permission_probe = explorer.executions[-1].get("error")
                        else:
                            permission_probe = "UNEXPECTED_ACCESS"
                        explorer.executions.clear()
                    answer = await agent_loop(ctx, question)
                    rows = [r for e in explorer.executions for r in e.get("rows", [])]
                    errors = [e["error"] for e in explorer.executions if "error" in e]
                    values = [v for row in rows for v in row.values()]
                    if case == "revenue":
                        expected = 2000000 if team == "sales" else 1700000
                        passed = len(rows) == 1 and expected in values and not errors
                    elif case == "clarification":
                        passed = not explorer.executions and any(
                            m.name == "ask_user" and "Awaiting user" in m.content
                            for m in ctx.session.history()
                        )
                    elif case == "salary":
                        passed = {tuple(sorted(r.values(), key=str)) for r in rows} == {
                            (3000000, "sales"),
                            (4000000, "finance"),
                        } and not errors
                    else:
                        passed = (
                            permission_probe in {"ORA-00942", "ORA-01031"}
                            and not rows
                            and bool(
                                re.search(
                                    r"권한|접근|permission|privilege|ORA-00942|ORA-01031",
                                    answer,
                                    re.IGNORECASE,
                                )
                            )
                        )
                    passed = passed and db_user == user.upper()
                    report["cases"].append(
                        {
                            "repeat": repeat + 1,
                            "case": case,
                            "team": team,
                            "db_user": db_user,
                            "question": question,
                            "definition": DEFINITIONS.get(team),
                            "passed": passed,
                            "permission_probe": permission_probe,
                            "answer": answer,
                            "executions": explorer.executions,
                            "tool_calls": [
                                c.name
                                for m in ctx.session.history()
                                for c in m.tool_calls
                            ],
                        }
                    )
                    logger.info(
                        "oracle demo case=%s team=%s repeat=%d passed=%s",
                        case,
                        team,
                        repeat + 1,
                        passed,
                    )
                finally:
                    if explorer._engine is not None:
                        await asyncio.to_thread(explorer._engine.dispose)
        report["passed"] = all(case["passed"] for case in report["cases"])
        return report
    finally:
        store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["seed", "verify"])
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--output", type=Path, required=False)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    url = os.environ.get("LANG2SQL_DB_URL", "")
    if not url or make_url(url).drivername != "oracle+oracledb":
        parser.error(
            "LANG2SQL_DB_URL must be an oracle+oracledb URL to the disposable demo database"
        )
    if args.repeats < 1:
        parser.error("--repeats must be positive")
    if args.command == "seed":
        seed(url)
        return
    report = asyncio.run(
        verify(
            url,
            args.live,
            dict(os.environ),
            os.environ.get("LANG2SQL_LLM_MODEL") or None,
            args.repeats,
        )
    )
    serialized = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(serialized + "\n")
    print(serialized)
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
