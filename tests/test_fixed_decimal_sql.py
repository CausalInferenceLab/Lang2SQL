"""Check compiled SQL against Decimal, without model calls or custom SQLite UDFs."""

import random
import sqlite3
from decimal import ROUND_HALF_UP, Decimal

import pytest

from lang2sql.tools.fixed_decimal_sql import FixedDecimalSQL, fixed_decimal_ctes


def execute(rows, input_scale=1, output_scale=1, aggregate="avg"):
    with sqlite3.connect(":memory:") as conn:
        conn.execute("CREATE TABLE decimal_input(group_id, scaled_value)")
        conn.executemany("INSERT INTO decimal_input VALUES (?, ?)", rows)
        return conn.execute(
            "WITH "
            + fixed_decimal_ctes(input_scale, output_scale, aggregate)
            + " SELECT group_id, n, invalid_count, status, rounded_units, rounded_value FROM decimal_result ORDER BY group_id"
        ).fetchall()


@pytest.mark.parametrize(
    ("values", "units"),
    [
        ([11, 12], 12),
        ([-11, -12], -12),
        ([11, -12], -1),
        ([10, 12], 11),
        ([None, 12], 12),
    ],
)
def test_midpoints_nulls_and_signs(values, units):
    assert execute([(1, v) for v in values]) == [
        (1, len([v for v in values if v is not None]), 0, "ok", units, units / 10)
    ]


def test_invalid_input_and_overflow_never_partially_aggregate():
    result = execute(
        [
            (1, None),
            (2, "12"),
            (2, 10),
            (3, 2**63 - 1),
            (4, -(2**63)),
            (5, 2**52),
            (5, 2**52),
            (6, 12.5),
            (7, 10),
        ]
    )
    assert [r[3] for r in result] == [
        "empty",
        "invalid_input",
        "overflow",
        "overflow",
        "overflow",
        "invalid_input",
        "ok",
    ]
    assert all(r[4:] == (None, None) for r in result[:-1])
    assert result[-1][4:] == (10, 1.0)


@pytest.mark.parametrize("aggregate", ["avg", "sum"])
@pytest.mark.parametrize(
    ("input_scale", "output_scale"), [(0, 0), (0, 6), (1, 2), (2, 0), (6, 6)]
)
def test_seeded_grouped_values_against_decimal(aggregate, input_scale, output_scale):
    rng = random.Random(51)
    rows = []
    expectations = {}
    for group in range(100):
        vals = [rng.randrange(-100000, 100000) for _ in range(rng.randrange(1, 30))]
        rows.extend((group, v) for v in vals)
        value = Decimal(sum(vals)) / 10**input_scale
        if aggregate == "avg":
            value /= len(vals)
        rounded = value.quantize(
            Decimal(1).scaleb(-output_scale), rounding=ROUND_HALF_UP
        )
        expectations[group] = int(rounded * 10**output_scale)
    result = execute(rows, input_scale, output_scale, aggregate)
    assert all(r[3] == "ok" and r[4] == expectations[r[0]] for r in result)


@pytest.mark.parametrize(
    "args", [(-1, 2, "avg"), (1, 7, "avg"), (True, 2, "avg"), (1, 2, "median")]
)
def test_invalid_compiler_settings(args):
    with pytest.raises(ValueError):
        fixed_decimal_ctes(*args)


@pytest.mark.asyncio
async def test_tool_uses_compiler_without_io():
    args = {
        "dialect": "sqlite",
        "input_scale": 1,
        "output_scale": 2,
        "aggregate": "avg",
    }
    result = await FixedDecimalSQL().run(args, None)
    assert fixed_decimal_ctes(1, 2, "avg") in result.content
    with pytest.raises(ValueError):
        await FixedDecimalSQL().run({**args, "dialect": "oracle"}, None)
