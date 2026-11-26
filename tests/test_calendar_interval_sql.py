"""Execute compiled SQL on plain SQLite: no custom functions or model required."""

import sqlite3

import pytest

from lang2sql.tools.calendar_interval_sql import CALENDAR_CTES, CalendarIntervalSQL


@pytest.mark.parametrize(
    ("start", "end", "expected"),
    [
        ("2020-06-20", "2023-02-10", (1, 2, 7, 21)),
        ("2023-02-10", "2020-06-20", (-1, 2, 7, 21)),
        ("2020-02-29", "2021-02-28", (1, 1, 0, 0)),
        ("2020-02-29", "2021-02-27", (1, 0, 11, 29)),
        ("2021-01-31", "2021-02-28", (1, 0, 1, 0)),
        ("2020-01-31", "2020-03-30", (1, 0, 1, 30)),
        ("2024-04-30", "2024-04-30", (1, 0, 0, 0)),
        ("9998-12-31", "9999-12-31", (1, 1, 0, 0)),
        ("0001-01-01", "0002-01-01", (1, 1, 0, 0)),
        (None, "2024-01-01", (None,) * 4),
        ("2023-02-29", "2024-01-01", (None,) * 4),
        ("2024-01-01", "2024-04-31", (None,) * 4),
        ("2024-01-01 00:00:00", "2024-02-01", (None,) * 4),
        ("nonsense", "2024-02-01", (None,) * 4),
        ("0000-01-01", "2024-01-01", (None,) * 4),
    ],
)
def test_calendar_components(start, end, expected):
    with sqlite3.connect(":memory:") as conn:
        rows = conn.execute(
            "WITH calendar_input(row_id, start_date, end_date) AS (VALUES (7, ?, ?)), "
            + CALENDAR_CTES
            + " SELECT * FROM calendar_interval",
            (start, end),
        ).fetchall()
    assert rows == [(7, *expected)]


@pytest.mark.asyncio
async def test_runtime_dispatch_and_input_validation():
    from lang2sql.core.identity import Identity
    from lang2sql.tenancy.tool_runtime import build_tool_context

    ctx = build_tool_context(
        Identity(user_id="test", guild_id="test"), explorer=object()
    )
    try:
        result = await ctx.tools.dispatch(
            "calendar_interval_sql", {"dialect": "sqlite"}, ctx, "call"
        )
        assert not result.is_error
        assert result.call_id == "call"
        assert CALENDAR_CTES in result.content
        result = await ctx.tools.dispatch(
            "calendar_interval_sql", {"dialect": "oracle"}, ctx, "bad"
        )
        assert result.is_error
        assert "ValueError" in result.content
    finally:
        ctx.store.close()


@pytest.mark.asyncio
async def test_no_database_access_or_model_required():
    result = await CalendarIntervalSQL().run({"dialect": "sqlite"}, None)
    assert not result.is_error
