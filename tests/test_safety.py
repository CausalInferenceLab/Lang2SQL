"""V1 safety pipeline regressions — the 12 CI-gate cases from §4.5.

Gate-time vs exec-time distinction
----------------------------------
The Whitelist + Timeout pipeline only decides whether SQL is *allowed to run*.
It does NOT execute SQL, so anything whose failure surfaces only when the query
actually runs against the DB is an EXECUTION-time concern and PASSes the gate:

  * #6 SELECT * FROM nonexistent  -> PG raises at run time (gate PASSes)
  * #7 SELECT pg_sleep(60)        -> bounded by ctx.timeout_seconds at run time
  * #8 SELECT * FROM huge_table   -> truncated by row_limit at run time

The Timeout layer is itself an exec-config layer: it sets ctx.timeout_seconds
and PASSes; it never inspects SQL for slow constructs. So #7's protection shows
up as "PASS + timeout configured", not as a BLOCK.

Only the statement-shape violations (#1-5, #12) BLOCK; #9/#10/#11 PASS.
"""

from __future__ import annotations

from lang2sql.core.ports.safety import SafetyContext, Verdict
from lang2sql.safety import SafetyPipeline


def _verdict(sql: str) -> Verdict:
    pipeline = SafetyPipeline()
    return pipeline.evaluate(sql, SafetyContext()).verdict


def _decision(sql: str):
    pipeline = SafetyPipeline()
    return pipeline.evaluate(sql, SafetyContext())


# --- BLOCK cases (#1-5, #12) -------------------------------------------------


def test_case_01_drop_table_blocks():
    assert _verdict("DROP TABLE users") is Verdict.BLOCK


def test_case_02_multi_statement_blocks():
    assert _verdict("; DELETE FROM t; --") is Verdict.BLOCK


def test_case_03_insert_blocks():
    assert _verdict("INSERT INTO t VALUES (1)") is Verdict.BLOCK


def test_case_04_update_blocks():
    assert _verdict("UPDATE t SET x=1") is Verdict.BLOCK


def test_case_05_cte_insert_fail_closed_blocks():
    # WITH starts the statement but an INSERT keyword lurks in the CTE body.
    sql = "WITH x AS (INSERT INTO t VALUES (1)) SELECT * FROM x"
    decision = _decision(sql)
    assert decision.verdict is Verdict.BLOCK
    assert "INSERT" in decision.reason


def test_case_12_empty_string_parse_error_blocks():
    decision = _decision("")
    assert decision.verdict is Verdict.BLOCK
    assert decision.reason == "parse_error"


# --- EXECUTION-time concerns: PASS the gate (#6, #7, #8) ---------------------


def test_case_06_nonexistent_table_passes_gate():
    # Resolution failure is a run-time PG error, not a gate decision.
    assert _verdict("SELECT * FROM nonexistent") is Verdict.PASS


def test_case_07_pg_sleep_passes_gate_with_timeout_configured():
    sql = "SELECT pg_sleep(60)"
    ctx = SafetyContext()
    ctx.timeout_seconds = 0  # simulate "unset" to prove the layer clamps it
    decision = SafetyPipeline().evaluate(sql, ctx)
    assert decision.verdict is Verdict.PASS
    # Timeout layer must have ensured a positive bound for run-time enforcement.
    assert ctx.timeout_seconds == 30


def test_case_08_huge_table_passes_gate():
    # row_limit truncation happens at execution time, not at the gate.
    assert _verdict("SELECT * FROM huge_table") is Verdict.PASS


# --- PASS cases (#9, #10, #11) -----------------------------------------------


def test_case_09_select_one_passes():
    assert _verdict("SELECT 1") is Verdict.PASS


def test_case_10_cte_select_passes():
    assert _verdict("WITH a AS (SELECT 1) SELECT * FROM a") is Verdict.PASS


def test_case_11_explain_select_passes():
    assert _verdict("EXPLAIN SELECT 1") is Verdict.PASS


# --- Extra guards (foreshadow V1.5; verify V1 fail-closed today) -------------


def test_explain_analyze_delete_blocks():
    # §4.5 notes EXPLAIN ANALYZE DELETE as a V1.5 regression; V1's fail-closed
    # keyword scan + leading-keyword check already blocks it.
    assert _verdict("EXPLAIN ANALYZE DELETE FROM t") is Verdict.BLOCK


def test_default_timeout_set_on_pass():
    ctx = SafetyContext()
    SafetyPipeline().evaluate("SELECT 1", ctx)
    assert ctx.timeout_seconds == 30


def test_pipeline_exposes_default_layers():
    pipeline = SafetyPipeline()
    names = [layer.name for layer in pipeline.layers]
    assert names == ["whitelist", "row_limit", "timeout"]


# --- RowLimitLayer tests ------------------------------------------------------


def test_row_limit_added_when_absent():
    ctx = SafetyContext(row_limit=500)
    decision = SafetyPipeline().evaluate("SELECT * FROM users", ctx)
    assert decision.verdict is Verdict.PASS
    assert "LIMIT 500" in decision.sql.upper()


def test_row_limit_not_added_when_present():
    sql = "SELECT * FROM users LIMIT 10"
    ctx = SafetyContext(row_limit=500)
    decision = SafetyPipeline().evaluate(sql, ctx)
    assert decision.verdict is Verdict.PASS
    assert decision.sql.upper().count("LIMIT") == 1


def test_row_limit_ignores_subquery_limit():
    sql = "SELECT * FROM (SELECT id FROM t LIMIT 5) sub"
    ctx = SafetyContext(row_limit=100)
    decision = SafetyPipeline().evaluate(sql, ctx)
    assert decision.verdict is Verdict.PASS
    assert "LIMIT 100" in decision.sql.upper()


def test_row_limit_ignores_cte_limit():
    sql = "WITH a AS (SELECT * FROM t LIMIT 10) SELECT * FROM a"
    ctx = SafetyContext(row_limit=200)
    decision = SafetyPipeline().evaluate(sql, ctx)
    assert decision.verdict is Verdict.PASS
    assert "LIMIT 200" in decision.sql.upper()


def test_row_limit_and_timeout_both_applied():
    ctx = SafetyContext(row_limit=42, timeout_seconds=0)
    decision = SafetyPipeline().evaluate("SELECT * FROM t", ctx)
    assert decision.verdict is Verdict.PASS
    assert "LIMIT 42" in decision.sql.upper()
    assert ctx.timeout_seconds == 30


def test_row_limit_default_1000_when_unset():
    ctx = SafetyContext()
    decision = SafetyPipeline().evaluate("SELECT * FROM t", ctx)
    assert "LIMIT 1000" in decision.sql.upper()


def test_case_08_huge_table_gets_limit():
    ctx = SafetyContext(row_limit=1000)
    decision = SafetyPipeline().evaluate("SELECT * FROM huge_table", ctx)
    assert decision.verdict is Verdict.PASS
    assert "LIMIT 1000" in decision.sql.upper()


def test_sqlite_signed_limit_is_not_duplicated():
    import sqlite3

    for suffix in ("LIMIT -1", "LIMIT +1", "LIMIT - 1", "LIMIT -1 OFFSET 1"):
        sql = "SELECT 1 UNION ALL SELECT 2 " + suffix
        decision = SafetyPipeline().evaluate(sql, SafetyContext(row_limit=1))
        assert decision.verdict is Verdict.PASS
        assert decision.sql == sql
        with sqlite3.connect(":memory:") as db:
            assert db.execute(decision.sql).fetchall() == db.execute(sql).fetchall()


def test_nested_negative_limit_does_not_disable_outer_limit():
    decision = SafetyPipeline().evaluate(
        "SELECT * FROM (SELECT 1 LIMIT -1)", SafetyContext(row_limit=5)
    )
    assert decision.sql.endswith("LIMIT 5")
