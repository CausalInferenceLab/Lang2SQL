"""Record recovery events without changing the product loop."""

from contextvars import ContextVar
from dataclasses import asdict

from sqlalchemy import event

from lang2sql.harness.loop import _SQL_WITHHELD
from lang2sql.harness.sql_candidate import is_database_query, sql_from_answer


def tool_choice(name):
    return {"type": "function", "function": {"name": name}} if name else "auto"


def response_fields(completion, tools):
    """Auxiliary calls share the budget but cannot produce a harness SQL candidate."""
    return {
        "harness": {"explore_schema", "run_sql"} <= {tool.name for tool in tools},
        "content": completion.content,
        "tool_calls": [asdict(call) for call in completion.tool_calls],
        "finish_reason": completion.finish_reason,
    }


class RecoveryTrace:
    def __init__(self, explorer):
        self.dispatches = []
        self.db_executions = []
        active_call = ContextVar("benchmark_tool_call", default=None)

        @event.listens_for(explorer._engine, "before_cursor_execute")
        def record_execution(_connection, _cursor, sql, _parameters, _context, _many):
            call_id = active_call.get()
            if call_id is not None:
                self.db_executions.append({"call_id": call_id, "sql": sql})

        self.active_call = active_call

    def attach(self, tools):
        dispatch = tools.dispatch

        async def record_dispatch(name, args, ctx, call_id):
            token = self.active_call.set(call_id) if name == "run_sql" else None
            try:
                result = await dispatch(name, args, ctx, call_id)
            finally:
                if token is not None:
                    self.active_call.reset(token)
            self.dispatches.append({"name": name, "arguments": args, **asdict(result)})
            return result

        tools.dispatch = record_dispatch

    def summarize(self, calls, transcript, final_sql, *, submitted):
        """Follow response order and start with the benchmark's empty schema context."""
        by_call = {event["call_id"]: event for event in self.dispatches}
        withheld_ids = {
            message["tool_call_id"]
            for message in transcript
            if message["role"] == "tool" and message["content"] == _SQL_WITHHELD
        }
        turn_events = []
        columns = set()
        for number, call in enumerate(calls, 1):
            if not call.get("harness"):
                continue
            requested = call.get("tool_calls", [])
            forced = isinstance(call["tool_choice"], dict)
            withheld_run = [item["id"] for item in requested if item["id"] in withheld_ids]
            withheld_final = (
                call["status"] == "ok"
                and not forced
                and not columns
                and not requested
                and is_database_query(sql_from_answer(call["content"], final_sql))
            )
            provided = sorted({
                table
                for item in requested
                for table in by_call.get(item["id"], {}).get("tables_with_columns", ())
            })
            if withheld_run or withheld_final or forced or provided:
                turn_events.append({
                    "api_call": number,
                    "forced_schema_request": forced,
                    "withheld_run_sql": withheld_run,
                    "withheld_final_sql": withheld_final,
                    "tables_with_columns": provided,
                    "same_response_columns": bool(withheld_run and provided),
                })
            columns.update(provided)
        withheld = any(e["withheld_run_sql"] or e["withheld_final_sql"] for e in turn_events)
        return {
            "turn_events": turn_events,
            "rewritten_submission": bool(withheld and columns and submitted),
            "db_executions": self.db_executions,
        }
