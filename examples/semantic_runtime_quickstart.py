"""Run a fully local, SQL-free Lang2SQLRuntime aggregate example.

Run from an installed package, or from this repository with:
    uv run python examples/semantic_runtime_quickstart.py
"""

from __future__ import annotations

import asyncio
import base64
from decimal import Decimal
import secrets
import sqlite3
import tempfile
from pathlib import Path
from typing import TypeVar

from lang2sql import (
    AggregateKind,
    Blocked,
    CallContext,
    CandidateRequest,
    CandidateSet,
    Capability,
    Connected,
    ConnectionInput,
    ConnectRequest,
    DateEndpoint,
    DateWindowInput,
    ExecuteRequest,
    ExecutionReady,
    FeedbackApplied,
    FeedbackRequest,
    FilterInput,
    FilterOperation,
    Lang2SQLRuntime,
    LiteralInput,
    PlanReady,
    PlanRequest,
    QueryDraft,
    ReviewRequired,
    ValueKind,
)

T = TypeVar("T")


def _seed_database(path: Path) -> None:
    """Create deterministic local data; the runtime never receives SQL."""
    with sqlite3.connect(path) as connection:
        connection.executescript("""
            CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY,
                amount NUMERIC NOT NULL,
                status TEXT NOT NULL,
                ordered_on DATE NOT NULL
            );
            INSERT INTO orders VALUES
                (1, 10, 'paid', '2025-01-01'),
                (2, 20, 'paid', '2025-01-31'),
                (3, 40, 'paid', '2025-02-01'),
                (4, 80, 'pending', '2025-01-15');
            """)


def _require(value: object, expected_type: type[T]) -> T:
    if not isinstance(value, expected_type):
        raise RuntimeError(f"runtime did not reach {expected_type.__name__}: {value}")
    return value


async def run() -> int:
    """Return the safe aggregate while ensuring all local state is removed."""
    with tempfile.TemporaryDirectory(prefix="lang2sql-quickstart-") as directory:
        workdir = Path(directory)
        database = workdir / "orders.sqlite"
        _seed_database(database)
        # An explicit process-local key keeps this demo independent from any
        # deployment key that may exist in the caller's environment.
        secret_key = base64.urlsafe_b64encode(secrets.token_bytes(32))
        runtime = Lang2SQLRuntime.local(
            path=str(workdir / "runtime.sqlite"), secret_key=secret_key
        )
        try:
            context = CallContext(
                scope="quickstart",
                actor_id="local_user",
                conversation_id="quickstart-run",
                capabilities=frozenset(
                    {Capability.CONNECT, Capability.QUERY, Capability.REVIEW_ANY}
                ),
            )
            connected = _require(
                await runtime.connect(
                    ConnectRequest(context, ConnectionInput(f"sqlite:///{database}"))
                ),
                Connected,
            )

            # This disposable fixture is explicitly public. Production hosts must
            # show every allowed review choice to an authorized person instead.
            for review in connected.reviews:
                if review.kind == "public_data_scope":
                    choice = "confirm_public"
                elif review.kind == "dimension_disclosure":
                    choice = "public_grouped"
                else:
                    raise RuntimeError(f"unexpected connection review: {review.kind}")
                _require(
                    await runtime.feedback(
                        FeedbackRequest(context, review.review_id, choice)
                    ),
                    FeedbackApplied,
                )

            question = (
                "total amount where status is paid ordered on "
                "from 2025-01-01 to 2025-02-01"
            )
            candidates = _require(
                await runtime.candidates(CandidateRequest(context, question)),
                CandidateSet,
            )
            metric = next(
                item for item in candidates.metrics if item.grounded_phrase == "amount"
            )
            status = next(
                item
                for item in candidates.filter_dimensions
                if item.grounded_phrase == "status"
            )
            ordered_on = next(
                item
                for item in candidates.time_dimensions
                if item.grounded_phrase == "ordered on"
            )
            planned: object = await runtime.plan(
                PlanRequest(
                    context,
                    QueryDraft(
                        question=question,
                        source=candidates.source,
                        candidate_token=candidates.candidate_token,
                        metric_id=metric.metric_id,
                        metric_phrase="amount",
                        aggregate=AggregateKind.SUM,
                        filters=(
                            FilterInput(
                                dimension_id=status.dimension_id,
                                dimension_phrase="status",
                                operator=FilterOperation.EQ,
                                operator_phrase="is",
                                values=(
                                    LiteralInput(ValueKind.STRING, "paid", "paid"),
                                ),
                            ),
                        ),
                        time_window=DateWindowInput(
                            dimension_id=ordered_on.dimension_id,
                            dimension_phrase="ordered on",
                            range_phrase="from 2025-01-01 to 2025-02-01",
                            start=DateEndpoint("2025-01-01", "2025-01-01"),
                            end=DateEndpoint("2025-02-01", "2025-02-01"),
                        ),
                    ),
                )
            )
            while isinstance(planned, ReviewRequired):
                # These are explicit semantic choices, not inferred SQL or an
                # automatic expansion of the disclosure boundary.
                choice = "sum" if planned.review.kind == "metric" else "confirm"
                feedback = await runtime.feedback(
                    FeedbackRequest(context, planned.review.review_id, choice)
                )
                if isinstance(feedback, Blocked) or feedback.next is None:
                    raise RuntimeError(f"plan review failed: {feedback}")
                planned = feedback.next

            plan = _require(planned, PlanReady).plan
            executed = _require(
                await runtime.execute(ExecuteRequest(context, plan)),
                ExecutionReady,
            )
            value = executed.rows[0][0]
            if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
                raise RuntimeError(
                    f"expected a numeric aggregate, got {type(value).__name__}"
                )
            return int(value)
        finally:
            runtime.close()


def main() -> None:
    total = asyncio.run(run())
    print(f"SUCCESS total_paid_amount={total}")


if __name__ == "__main__":
    main()
