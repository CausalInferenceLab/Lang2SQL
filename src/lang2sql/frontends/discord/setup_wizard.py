"""``/setup`` — a zero-DSN connection wizard for non-developers.

The user never sees a SQLAlchemy URL or an env file. They run ``/setup``, pick
their database from a dropdown, and fill a short form. We assemble the DSN,
test the connection by listing tables, and store the credentials encrypted via
:class:`EncryptedSecrets` keyed by the guild scope. The next message in that
guild transparently uses the new database.

Discord coupling lives only here and in ``bot.py``: the actual register-and-
test logic is :meth:`CommandHandlers.register_db_for_guild` (pure, testable).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import discord
from discord import ui

from ...adapters.db.dsn_builder import FIELD_SCHEMA, SUPPORTED_DB_TYPES
from .session_router import to_identity

if TYPE_CHECKING:
    from .commands import CommandHandlers


# Per-DB human labels surfaced in the dropdown.
_LABELS: dict[str, str] = {
    "sqlite": "SQLite (file)",
    "postgresql": "PostgreSQL",
    "mysql": "MySQL",
    "snowflake": "Snowflake",
    "bigquery": "BigQuery",
    "duckdb": "DuckDB (file)",
    "d1": "Cloudflare D1",
}


class _ConnectionFormModal(ui.Modal):
    """The per-DB-type form. Fields come from :data:`FIELD_SCHEMA`.

    Discord modals cap at 5 :class:`ui.TextInput` rows, which matches our
    widest schema (Postgres/MySQL/Snowflake). Passwords/tokens are plain text
    inputs — Discord has no masked input style — but the form is ephemeral so
    only the user sees what they typed.
    """

    def __init__(
        self,
        db_type: str,
        handlers: "CommandHandlers",
        ctx_factory,
    ) -> None:
        super().__init__(title=f"Connect to {_LABELS.get(db_type, db_type)}")
        self._db_type = db_type
        self._handlers = handlers
        self._ctx_factory = ctx_factory  # () -> InteractionContext
        self._inputs: dict[str, ui.TextInput] = {}
        for name, placeholder, required, _masked in FIELD_SCHEMA[db_type]:
            inp: ui.TextInput = ui.TextInput(
                label=name,
                placeholder=placeholder,
                required=required,
                style=discord.TextStyle.short,
                max_length=200,
            )
            self._inputs[name] = inp
            self.add_item(inp)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        # Connection test can take a few seconds; defer so Discord doesn't
        # timeout the interaction. Ephemeral so only the user sees the result.
        await interaction.response.defer(ephemeral=True, thinking=True)
        fields = {name: inp.value for name, inp in self._inputs.items()}
        identity = to_identity(self._ctx_factory(interaction))
        result = await self._handlers.register_db_for_guild(
            identity, self._db_type, fields
        )
        await interaction.followup.send(result.text, ephemeral=True)


class _DbTypeSelect(ui.Select):
    """Step 1 dropdown — pick which DB type to connect."""

    def __init__(self, handlers: "CommandHandlers", ctx_factory) -> None:
        options = [
            discord.SelectOption(label=_LABELS[t], value=t) for t in SUPPORTED_DB_TYPES
        ]
        super().__init__(
            placeholder="Choose your database…",
            options=options,
            min_values=1,
            max_values=1,
        )
        self._handlers = handlers
        self._ctx_factory = ctx_factory

    async def callback(self, interaction: discord.Interaction) -> None:
        # Opening a modal *is* the response to this select interaction.
        await interaction.response.send_modal(
            _ConnectionFormModal(self.values[0], self._handlers, self._ctx_factory)
        )


class _SetupView(ui.View):
    """Holds the DB-type dropdown. Auto-times out after 2 minutes."""

    def __init__(self, handlers: "CommandHandlers", ctx_factory) -> None:
        super().__init__(timeout=120.0)
        self.add_item(_DbTypeSelect(handlers, ctx_factory))


async def start_setup_flow(
    interaction: discord.Interaction,
    handlers: "CommandHandlers",
    ctx_factory,
) -> None:
    """Entry point bot.py wires to ``/setup`` — surfaces the picker ephemerally."""
    await interaction.response.send_message(
        "연결할 DB 종류를 고른 뒤 접속 정보를 입력해 주세요. "
        "입력 내용은 비공개로 처리되고 암호화해 저장합니다.",
        view=_SetupView(handlers, ctx_factory),
        ephemeral=True,
    )
