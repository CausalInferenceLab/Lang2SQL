"""render — agent answer → :class:`OutboundMessage` (pure, no discord import).

v4.1 §4.1 sets the response rule: a small result goes back as text, a large one
(> :data:`MAX_INLINE_ROWS`) is attached as a CSV with a short text summary so a
Discord message never balloons past what a human skims. Keeping this pure means
``bot.py`` only has to turn an :class:`OutboundMessage` into a ``discord.File``
or a plain reply — all the threshold logic is unit-tested here.
"""

from __future__ import annotations

import csv
import io
from collections.abc import Sequence
from typing import Any

from ...core.ports.frontend import OutboundMessage

# Above this many rows (or text lines) we attach a CSV instead of inlining.
MAX_INLINE_ROWS = 50


def render_answer(
    text: str,
    rows: Sequence[Sequence[Any]] | None = None,
    *,
    header: Sequence[str] | None = None,
    file_name: str = "result.csv",
) -> OutboundMessage:
    """Render an agent answer, attaching a CSV when it's too big to inline.

    Two oversized shapes trigger an attachment:

    * structured ``rows`` longer than :data:`MAX_INLINE_ROWS` — serialised to
      CSV (with ``header`` if given) and replaced by a one-line summary; or
    * a plain ``text`` answer with more than :data:`MAX_INLINE_ROWS` lines —
      written verbatim into a ``.csv``/text attachment.

    Anything smaller is returned as plain ``text``.
    """
    if rows is not None and len(rows) > MAX_INLINE_ROWS:
        payload = _rows_to_csv(rows, header)
        summary = f"{len(rows)} rows — attached as {file_name}."
        if text.strip():
            summary = f"{text.strip()}\n{summary}"
        return OutboundMessage(
            text=summary,
            file_bytes=payload.encode("utf-8"),
            file_name=file_name,
        )

    if rows is not None:
        # Small structured result: inline as text, CSV-formatted for legibility.
        body = _rows_to_csv(rows, header).rstrip("\n")
        text_block = f"{text.strip()}\n{body}" if text.strip() else body
        return OutboundMessage(text=text_block)

    lines = text.splitlines()
    if len(lines) > MAX_INLINE_ROWS:
        summary = f"Result is {len(lines)} lines — attached as {file_name}."
        return OutboundMessage(
            text=summary,
            file_bytes=text.encode("utf-8"),
            file_name=file_name,
        )

    return OutboundMessage(text=text)


def _rows_to_csv(rows: Sequence[Sequence[Any]], header: Sequence[str] | None) -> str:
    """Serialise ``rows`` (optionally with a ``header``) to a CSV string."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    if header is not None:
        writer.writerow(list(header))
    for row in rows:
        writer.writerow(list(row))
    return buf.getvalue()
