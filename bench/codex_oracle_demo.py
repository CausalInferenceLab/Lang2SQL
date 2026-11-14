"""Verify Codex -> MCP -> Lang2SQL -> Oracle using the existing Oracle fixtures."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.engine import make_url

from oracle_demo import (
    DEFINITIONS,
    OWNER,
    REVENUE_QUESTION,
    SALARY_QUESTION,
    SALARY_SQL,
    UNKNOWN_QUESTION,
)
from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer
from lang2sql.adapters.llm.fake import FakeLLM
from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.core.identity import Identity
from lang2sql.core.ports.explorer import DataAccessError
from lang2sql.frontends.codex.app import run
from lang2sql.tenancy.concierge import ContextConcierge
from lang2sql.tools.semantic_federation import SemanticFederationTool

logger = logging.getLogger(__name__)


async def verify(
    url: str,
    env: dict[str, str],
    model: str | None,
    repeats: int,
    timeout: float,
    output_path: Path | None = None,
) -> dict:
    report = {
        "runtime": "codex-mcp",
        "model": model or "Codex CLI default",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "cases": [],
        "complete": False,
    }
    base = make_url(url)
    with tempfile.TemporaryDirectory(prefix="lang2sql-codex-eval-") as directory:
        path = str(Path(directory) / "context.sqlite")
        store = SqliteStore(path)
        # Explicit LLM injection avoids creating any API-backed inner agent.
        concierge = ContextConcierge(
            store=store, llm=FakeLLM(), explorer=SqlAlchemyExplorer(url, schema=OWNER)
        )
        for team in ("sales", "finance", "hr"):
            ctx = await concierge.build_context(
                Identity(user_id="curator", guild_id="oracle-demo", channel_id=team)
            )
            terms = {
                "급여": f"{OWNER}.salaries의 salary, 부서는 department. 부서별 평균은 AVG(salary). DB 접근 권한을 따른다."
            }
            if team in DEFINITIONS:
                terms["매출"] = DEFINITIONS[team]
            for term, definition in terms.items():
                result = await SemanticFederationTool().run(
                    {
                        "term": term,
                        "definition": definition,
                        "layer": "team",
                        "kind": "metric",
                    },
                    ctx,
                )
                if result.is_error:
                    raise RuntimeError(result.content)
        try:
            for repeat in range(repeats):
                for team, case, question in [
                    ("sales", "revenue", REVENUE_QUESTION),
                    ("finance", "revenue", REVENUE_QUESTION),
                    ("sales", "clarification", UNKNOWN_QUESTION),
                    ("hr", "salary", SALARY_QUESTION),
                    ("sales", "denied", SALARY_QUESTION),
                ]:
                    actor = f"{team}-{case}-{repeat + 1}"
                    reader_url = base.set(username=f"l2s_demo_{team}").render_as_string(
                        hide_password=False
                    )
                    explorer = SqlAlchemyExplorer(reader_url, schema=OWNER)
                    run_env = dict(
                        env,
                        LANG2SQL_DB_URL=reader_url,
                        LANG2SQL_DB_SCHEMA=OWNER,
                        LANG2SQL_DATA_PATH=path,
                        LANG2SQL_USER_ID=actor,
                        LANG2SQL_ORG_ID="oracle-demo",
                        LANG2SQL_TEAM_ID=team,
                    )
                    try:
                        db_user = (
                            await explorer.execute("SELECT USER AS db_user FROM dual")
                        )[0]["db_user"]
                        probe = None
                        if case == "denied":
                            try:
                                await explorer.execute(SALARY_SQL)
                            except DataAccessError as exc:
                                probe = str(exc).split(":", 1)[0]
                                logger.info(
                                    "permission probe rejected actor=%s code=%s",
                                    actor,
                                    probe,
                                )
                        output = await run(
                            question, env=run_env, model=model, timeout=timeout
                        )
                        audit = list(reversed(await store.query(actor, limit=1000)))
                        calls = [e.detail for e in audit if e.action == "mcp_tool"]
                        sql_events = [
                            e
                            for e in audit
                            if e.action in {"run_sql", "run_sql_failed"}
                        ]
                        # Re-execute exactly the audited successful SQL against the same
                        # fixed fixture/identity; never grade the model's answer alone.
                        rows = []
                        for event in sql_events:
                            if event.action == "run_sql":
                                rows.extend(await explorer.execute(event.detail["sql"]))
                        errors = [e for e in sql_events if e.action == "run_sql_failed"]
                        if case == "revenue":
                            expected = 2000000 if team == "sales" else 1700000
                            passed = (
                                len(rows) == 1
                                and expected in rows[0].values()
                                and not errors
                            )
                        elif case == "clarification":
                            passed = not sql_events and any(
                                c["tool"] == "ask_user" and c["stop_turn"]
                                for c in calls
                            )
                        elif case == "salary":
                            passed = {
                                tuple(sorted(r.values(), key=str)) for r in rows
                            } == {
                                (3000000, "sales"),
                                (4000000, "finance"),
                            } and not errors
                        else:
                            passed = (
                                probe in {"ORA-00942", "ORA-01031"}
                                and not rows
                                and len(errors) == 1
                                and any(
                                    c["tool"] == "run_sql" and c["stop_turn"]
                                    for c in calls
                                )
                            )
                        passed = (
                            passed
                            and db_user == f"L2S_DEMO_{team.upper()}"
                            and bool(calls)
                        )
                        report["cases"].append(
                            {
                                "repeat": repeat + 1,
                                "case": case,
                                "team": team,
                                "db_user": db_user,
                                "question": question,
                                "passed": passed,
                                "permission_probe": probe,
                                "answer": output["answer"],
                                "calls": calls,
                                "rows": rows,
                                "events": output["events"],
                            }
                        )
                        if output_path is not None:
                            output_path.write_text(
                                json.dumps(
                                    report, ensure_ascii=False, indent=2, default=str
                                )
                                + "\n"
                            )
                        logger.info(
                            "codex verification case=%s team=%s repeat=%d passed=%s",
                            case,
                            team,
                            repeat + 1,
                            passed,
                        )
                    finally:
                        if explorer._engine is not None:
                            await asyncio.to_thread(explorer._engine.dispose)
        finally:
            store.close()
    report["complete"] = True
    report["passed"] = all(c["passed"] for c in report["cases"])
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.repeats < 1 or args.timeout <= 0:
        parser.error("repeats and timeout must be positive")
    url = os.environ.get("LANG2SQL_DB_URL", "")
    if not url or make_url(url).drivername != "oracle+oracledb":
        parser.error("LANG2SQL_DB_URL must point to the seeded disposable Oracle demo")
    logging.basicConfig(level=logging.INFO)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    report = asyncio.run(
        verify(
            url, dict(os.environ), args.model, args.repeats, args.timeout, args.output
        )
    )
    args.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n"
    )
    print(f"{sum(c['passed'] for c in report['cases'])}/{len(report['cases'])} passed")
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
