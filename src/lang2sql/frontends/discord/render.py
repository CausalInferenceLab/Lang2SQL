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
import re
import unicodedata
from collections.abc import Sequence
from typing import Any

from ...core.ports.frontend import OutboundMessage

# Above this many rows (or text lines) we attach a CSV instead of inlining.
MAX_INLINE_ROWS = 50
MAX_DISCORD_TEXT = 1900


def sanitize_discord_text(value: Any, *, max_length: int = 512) -> str:
    """Render untrusted DB/user metadata as inert, bounded Discord text."""

    if value is None:
        return ""
    if isinstance(value, bytes):
        value = "hex:" + value.hex()
    text = "".join(
        " " if unicodedata.category(character).startswith("C") else character
        for character in str(value)
    )
    text = re.sub(r"\s+", " ", text).strip()
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    text = text.replace("@", "@\u200b")
    text = re.sub(r"([\\`*_{}\[\]()#+\-.!|~=])", r"\\\1", text)
    return text[: max(0, int(max_length))]


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
    if rows is not None and (
        len(rows) > MAX_INLINE_ROWS or _structured_rows_need_attachment(rows, header)
    ):
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
        # Small structured result stays readable while DB labels remain inert
        # inside Discord Markdown.
        body = _rows_to_markdown(rows, header)
        text_block = f"{text.strip()}\n{body}" if text.strip() else body
        if len(text_block) > MAX_DISCORD_TEXT:
            payload = _rows_to_csv(rows, header)
            return OutboundMessage(
                text=f"{len(rows)} rows — attached as {file_name}.",
                file_bytes=payload.encode("utf-8"),
                file_name=file_name,
            )
        return OutboundMessage(text=text_block)

    lines = text.splitlines()
    if len(lines) > MAX_INLINE_ROWS or len(text) > MAX_DISCORD_TEXT:
        summary = f"Result is {len(lines)} lines — attached as {file_name}."
        return OutboundMessage(
            text=summary,
            file_bytes=text.encode("utf-8"),
            file_name=file_name,
        )

    return OutboundMessage(text=text)


def _structured_rows_need_attachment(
    rows: Sequence[Sequence[Any]], header: Sequence[str] | None
) -> bool:
    """Keep the inline preview bounded without truncating full cell values."""

    values: list[Any] = [*(header or ()), *(cell for row in rows for cell in row)]
    text_size = 0
    for value in values:
        rendered = (
            "hex:" + value.hex() if isinstance(value, bytes) else str(value or "")
        )
        if len(rendered) > 512:
            return True
        text_size += len(rendered)
    return text_size > MAX_DISCORD_TEXT


def _rows_to_csv(rows: Sequence[Sequence[Any]], header: Sequence[str] | None) -> str:
    """Serialise ``rows`` (optionally with a ``header``) to a CSV string."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    if header is not None:
        writer.writerow([_csv_cell(value) for value in header])
    for row in rows:
        writer.writerow([_csv_cell(value) for value in row])
    return buf.getvalue()


def _csv_cell(value: Any) -> Any:
    """Keep spreadsheet programs from treating DB strings as formulas."""

    if value is None:
        return ""
    if isinstance(value, bytes):
        return "hex:" + value.hex()
    if not isinstance(value, str):
        return value
    if value.lstrip().startswith(("=", "+", "-", "@")):
        return "'" + value
    return value


def _rows_to_markdown(
    rows: Sequence[Sequence[Any]], header: Sequence[str] | None
) -> str:
    width = len(header) if header is not None else (len(rows[0]) if rows else 0)
    headings = (
        list(header)
        if header is not None
        else [f"column_{index + 1}" for index in range(width)]
    )
    lines = [" | ".join(_markdown_cell(item) for item in headings)]
    lines.append(" | ".join("---" for _ in headings))
    lines.extend(" | ".join(_markdown_cell(item) for item in row) for row in rows)
    return "\n".join(lines)


def _markdown_cell(value: Any) -> str:
    return sanitize_discord_text(value)
