import sqlite3
from contextlib import closing

from bench.turn_retention.fixture import PROBES, database, make_cases
from bench.turn_retention.run import final_query, grade, reported_value
from lang2sql.core.types import Message, Role, ToolCall


def test_frozen_questions_and_definition_probe():
    cases = make_cases()
    assert len(cases) == 100
    assert len({c["question"] for c in cases}) == 100
    assert [c["turn"] for c in cases if c["probe"]] == list(PROBES)


def test_grade_natural_discord_answer_and_wrong_sql(tmp_path):
    path = tmp_path / "primary.sqlite"
    with closing(sqlite3.connect(path)) as db:
        db.execute("create table orders(amount integer)")
        db.execute("insert into orders values(31)")
        db.commit()
    sql = "SELECT SUM(amount) FROM orders"
    events = [
        Message(Role.ASSISTANT, tool_calls=[ToolCall("x", "run_sql", {"sql": sql})]),
        Message(Role.TOOL, content="1 row(s):\n31", name="run_sql", tool_call_id="x"),
    ]
    case = {"primary": 31, "probe": False}
    assert grade(case, "2025년 3월 매출은 31원입니다.", tmp_path, events)["correct"]
    assert not grade(case, "RESULT=31\n```sql\nSELECT 7\n```", tmp_path, events)[
        "correct"
    ]
    assert not grade(case, "매출은 32원입니다.", tmp_path, events)["correct"]
    assert not grade(case, "31원입니다.", tmp_path, [])["correct"]
    assert reported_value("24건 또는 31건") is None
    events.append(
        Message(Role.TOOL, content="invalid", name="run_sql", tool_call_id="x")
    )
    assert final_query("31원입니다.", events) == ("", "missing")


def test_probes_require_query_to_generalize_to_hidden_data(tmp_path):
    c = next(c for c in make_cases() if c["probe"])
    for name, seed in [("primary", 267), ("shadow", 728)]:
        db = tmp_path / (name + ".sqlite")
        database(db, seed)
        with closing(sqlite3.connect(db)) as conn:
            c[name] = conn.execute(c["gold_sql"]).fetchone()[0]
    assert c["primary"] != c["shadow"]
    constant = f"RESULT={c['primary']}\n```sql\nSELECT {c['primary']}\n```"
    assert grade(c, constant, tmp_path, [])["correct"]
    assert not grade(c, constant, tmp_path, [])["probe_applied"]
    actual = f"RESULT={c['primary']}\n```sql\n{c['gold_sql']}\n```"
    assert grade(c, actual, tmp_path, [])["probe_applied"]
