"""Compiled SQL must agree with exact rational arithmetic within its contract."""

import random
import sqlite3
from fractions import Fraction

import pytest

from lang2sql.tools.fixed_decimal_sql import _MAX_EXACT_INTEGER
from lang2sql.tools.integer_ratio_sql import IntegerRatioSQL, integer_ratio_ctes


def execute(rows, multiplier=100):
    with sqlite3.connect(":memory:") as conn:
        conn.execute("CREATE TABLE ratio_input(group_id, numerator, denominator)")
        conn.executemany("INSERT INTO ratio_input VALUES (?, ?, ?)", rows)
        return conn.execute("WITH " + integer_ratio_ctes(multiplier)
                            + " SELECT group_id, status, ratio_value FROM ratio_result").fetchall()


@pytest.mark.parametrize("multiplier", [1, 100, 10000, _MAX_EXACT_INTEGER])
def test_ratios_match_fraction_without_intermediate_rounding(multiplier):
    rng = random.Random(913)
    bound = _MAX_EXACT_INTEGER // multiplier
    rows = []
    for i in range(1000):
        n = rng.randint(-bound, bound)
        d = rng.randint(1, _MAX_EXACT_INTEGER) * (-1 if i % 2 else 1)
        rows.append((i, float(n) if i % 3 else n, float(d) if i % 4 else d))
    result = execute(rows, multiplier)
    assert all(status == "ok" and value == float(Fraction(int(rows[i][1]) * multiplier, int(rows[i][2])))
               for i, status, value in result)


def test_real_integral_counts_and_scaling_order():
    result = execute([(1, 1.0, 3.0), (2, -1, 3), (3, 0, -3)])
    assert result == [(1, "ok", float(Fraction(100, 3))),
                      (2, "ok", float(Fraction(-100, 3))), (3, "ok", 0.0)]
    assert result[0][2] != 1.0 / 3.0 * 100.0


def test_invalid_rows_are_retained_with_null_results():
    bound = _MAX_EXACT_INTEGER // 100
    rows = [(1, None, 3), (2, 1, None), (3, 1, 0), (4, "1", 3),
            (5, 1.5, 3), (6, 1, 2.5), (7, bound + 1, 3),
            (8, -(2**63), 1), (9, 1, 2**53), (10, float("inf"), 1),
            (11, 1, b"3"), (12, bound, _MAX_EXACT_INTEGER)]
    result = execute(rows)
    assert [r[1] for r in result] == ["null_input", "null_input", "zero_denominator",
        "invalid_input", "invalid_input", "invalid_input", "overflow", "overflow",
        "overflow", "overflow", "invalid_input", "ok"]
    assert all(r[2] is None for r in result[:-1])
    assert result[-1][2] == float(Fraction(bound * 100, _MAX_EXACT_INTEGER))


@pytest.mark.parametrize("multiplier", [True, 0, -1, 1.5, "100", 2**53])
def test_invalid_settings(multiplier):
    with pytest.raises(ValueError):
        integer_ratio_ctes(multiplier)


@pytest.mark.asyncio
async def test_tool_compiles_without_database_or_model():
    result = await IntegerRatioSQL().run({"dialect": "sqlite", "multiplier": 100}, None)
    assert integer_ratio_ctes(100) in result.content
    with pytest.raises(ValueError):
        await IntegerRatioSQL().run({"dialect": "oracle", "multiplier": 100}, None)
