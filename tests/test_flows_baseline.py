import pytest

from lang2sql.core.base import BaseFlow
from lang2sql.flows.baseline import BaselineFlow, SequentialFlow


def test_requires_at_least_one_step():
    with pytest.raises(ValueError):
        SequentialFlow(steps=[])


def test_baseline_flow_emits_deprecation_warning():
    with pytest.warns(DeprecationWarning, match="BaselineFlow is deprecated"):
        flow = BaselineFlow(steps=[lambda x: x])
    assert isinstance(flow, SequentialFlow)


def test_run_passes_value_through_single_step():
    flow = SequentialFlow(steps=[lambda x: x + 1])
    assert flow.run(1) == 2


def test_run_chains_multiple_steps():
    flow = SequentialFlow(steps=[lambda x: x * 2, lambda x: x + 10])
    assert flow.run(5) == 20  # 5 * 2 = 10, 10 + 10 = 20


def test_step_order_is_preserved():
    trace = []

    def s1(x):
        trace.append("s1")
        return x

    def s2(x):
        trace.append("s2")
        return x

    def s3(x):
        trace.append("s3")
        return x

    SequentialFlow(steps=[s1, s2, s3]).run("input")
    assert trace == ["s1", "s2", "s3"]


def test_user_can_compose_flows_with_python_control_flow():
    default_flow = SequentialFlow(steps=[lambda x: x + "_default"])
    override_flow = SequentialFlow(steps=[lambda x: x + "_override"])

    assert default_flow("q") == "q_default"

    class CustomFlow(BaseFlow):
        def run(self, value):
            return override_flow(value)

    assert CustomFlow().run("q") == "q_override"


# -------------------------
# Advanced: retry patterns
# -------------------------


def test_custom_flow_fallback_then_revalidate():
    def gen_bad(_query):
        return "DROP TABLE users;"

    def validate(sql):
        return "drop " not in sql.lower()

    class FixThenRevalidateFlow(BaseFlow):
        def run(self, query):
            sql = gen_bad(query)
            if not validate(sql):
                sql = "SELECT 1;"
            return sql

    assert FixThenRevalidateFlow().run("q") == "SELECT 1;"


def test_custom_flow_retry_until_valid():
    attempts = {"count": 0}

    def generate(_query):
        attempts["count"] += 1
        return "DROP TABLE users;" if attempts["count"] == 1 else "SELECT 1;"

    def validate(sql):
        return "drop " not in sql.lower()

    class RetryFlow(BaseFlow):
        def run(self, query):
            for _ in range(3):
                sql = generate(query)
                if validate(sql):
                    return sql
            return sql

    result = RetryFlow().run("q")
    assert result == "SELECT 1;"
    assert attempts["count"] >= 2


# -------------------------
# Composition: flow as step
# -------------------------


def test_subflow_can_be_used_as_a_step():
    inner = SequentialFlow(steps=[lambda x: x + 1])
    outer = SequentialFlow(steps=[lambda x: x * 2, inner])

    # 3 * 2 = 6, 6 + 1 = 7
    assert outer.run(3) == 7


def test_subflow_can_be_conditionally_invoked_in_custom_flow():
    flow_a = SequentialFlow(steps=[lambda x: x + "_a"])

    class ConditionalFlow(BaseFlow):
        def run(self, value):
            if "use_a" in value:
                return flow_a(value)
            return value

    assert ConditionalFlow().run("nope") == "nope"
    assert ConditionalFlow().run("please use_a") == "please use_a_a"
