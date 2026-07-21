"""bot.py — the ONLY discord.py-aware module in the frontend (Phase 1).

It is the thin shell described in v4.1 §2.1: receive a Discord interaction or
mention, translate it to an :class:`InteractionContext` →
:class:`Identity` (session_router), call a :class:`CommandHandlers` method, and
deliver the resulting :class:`OutboundMessage` natively (plain reply, or a
``discord.File`` upload when render attached a CSV).

Import-safety contract (tested): importing this module must not require a token
or any network access — only :func:`run` connects to the gateway. So discord.py
is imported at module load (it's a pure library import), but the client is
constructed and the token is read only inside :func:`run`.
"""

from __future__ import annotations

import io
import logging
import os

import discord
from discord import app_commands

from ...core.ports.frontend import OutboundMessage
from ...tenancy.concierge import ContextConcierge
from .commands import CommandHandlers
from .session_router import InteractionContext, to_identity

logger = logging.getLogger(__name__)

TOKEN_ENV = "DISCORD_BOT_TOKEN"
_DISCORD_CONTENT_LIMIT = 1900  # Discord hard limit is 2000; 100-char safety margin


def _is_direct_user_mention(message: discord.Message, user_id: int | None) -> bool:
    """Accept only an explicit user mention, never ``@everyone``/``@here``.

    ``discord.User.mentioned_in`` deliberately treats mention-everyone as a
    match. That behavior is useful for normal bots but violates this bot's
    opt-in-only message contract.
    """

    return user_id is not None and user_id in message.raw_mentions


def _interaction_context(interaction: discord.Interaction) -> InteractionContext:
    """Extract frontend-neutral coordinates from a slash-command interaction."""
    channel = interaction.channel
    thread_id: str | None = None
    channel_id: str | None = None
    if isinstance(channel, discord.Thread):
        thread_id = str(channel.id)
        channel_id = str(channel.parent_id) if channel.parent_id else None
    elif channel is not None:
        channel_id = str(channel.id)

    is_admin = False
    perms = getattr(interaction, "permissions", None)
    if perms is not None:
        is_admin = bool(perms.administrator)

    return InteractionContext(
        user_id=str(interaction.user.id),
        guild_id=str(interaction.guild_id) if interaction.guild_id else None,
        channel_id=channel_id,
        thread_id=thread_id,
        is_admin=is_admin,
    )


def _message_context(message: discord.Message) -> InteractionContext:
    """Extract coordinates from a plain message (an @mention or thread reply)."""
    channel = message.channel
    thread_id: str | None = None
    channel_id: str | None = None
    if isinstance(channel, discord.Thread):
        thread_id = str(channel.id)
        channel_id = str(channel.parent_id) if channel.parent_id else None
    elif channel is not None:
        channel_id = str(channel.id)

    is_admin = False
    author = message.author
    guild_perms = getattr(author, "guild_permissions", None)
    if guild_perms is not None:
        is_admin = bool(guild_perms.administrator)

    return InteractionContext(
        user_id=str(author.id),
        guild_id=str(message.guild.id) if message.guild else None,
        channel_id=channel_id,
        thread_id=thread_id,
        is_admin=is_admin,
    )


def _to_sendable(message: OutboundMessage) -> tuple[str, discord.File | None]:
    """Turn an :class:`OutboundMessage` into (content, optional file) for send."""
    if message.file_bytes is not None:
        file = discord.File(
            io.BytesIO(message.file_bytes),
            filename=message.file_name or "result.csv",
        )
        return message.text, file
    return message.text, None


def _build_send_kwargs(out: OutboundMessage) -> dict:
    """Build channel.send kwargs — omits 'file' key when there is no attachment."""
    content, file = _to_sendable(out)
    text = content or "(empty)"
    if len(text) > _DISCORD_CONTENT_LIMIT:
        if file is None:
            file = discord.File(io.BytesIO(text.encode()), filename="response.txt")
            text = "_(응답이 너무 길어 파일로 첨부합니다)_"
        else:
            text = text[:_DISCORD_CONTENT_LIMIT] + "\n…(truncated)"
    kwargs: dict = {"content": text}
    if file is not None:
        kwargs["file"] = file
    return kwargs


class Lang2SQLBot(discord.Client):
    """Discord client wiring slash commands + @mentions to the harness."""

    def __init__(self, handlers: CommandHandlers) -> None:
        intents = discord.Intents.default()
        intents.message_content = True  # needed to read @mention text
        super().__init__(intents=intents)
        # discord.Client already owns a private ``_handlers`` mapping.  A
        # distinct name avoids overwriting gateway internals.
        self._command_handlers = handlers
        self.tree = app_commands.CommandTree(self)
        self._register_commands()

    async def setup_hook(self) -> None:
        # Sync only when LANG2SQL_SYNC_COMMANDS=true (e.g. after adding/removing commands).
        # Skipping sync on every restart avoids Discord rate limits during dev.
        if os.environ.get("LANG2SQL_SYNC_COMMANDS", "").lower() == "true":
            await self.tree.sync()
            logger.info("slash commands synced")

    def _register_commands(self) -> None:
        tree = self.tree
        handlers = self._command_handlers

        @tree.command(
            name="setup",
            description="Connect a database with a guided form (no DSN needed)",
        )
        async def setup(interaction: discord.Interaction) -> None:
            from .setup_wizard import (
                start_setup_flow,
            )  # local import — discord-only path

            await start_setup_flow(interaction, handlers, _interaction_context)

        @tree.command(
            name="ingest",
            description="문서에서 비즈니스 용어 후보 추출 (ref: 파일명, content: 텍스트 직접 입력)",
        )
        async def ingest(
            interaction: discord.Interaction,
            ref: str = "",
            content: str = "",
        ) -> None:
            await self._run(
                interaction,
                handlers.ingest(
                    to_identity(_interaction_context(interaction)),
                    ref=ref or None,
                    content=content or None,
                ),
            )

        @tree.command(
            name="confirm_ingest",
            description="ingest로 추출한 후보를 시멘틱 레이어에 등록",
        )
        async def confirm_ingest(
            interaction: discord.Interaction,
            ref: str,
            accept: str = "all",
            layer: str = "channel",
        ) -> None:
            await self._run(
                interaction,
                handlers.confirm_ingest(
                    to_identity(_interaction_context(interaction)),
                    ref=ref,
                    accept=accept,
                    layer=layer,
                ),
            )

        @tree.command(name="remember", description="Remember a fact for future turns")
        async def remember(interaction: discord.Interaction, text: str) -> None:
            await self._run(
                interaction,
                handlers.remember(to_identity(_interaction_context(interaction)), text),
            )

        @tree.command(
            name="enrich",
            description="LLM으로 DB 컬럼 메타데이터 자동 보강 (clear=True로 초기화)",
        )
        async def enrich(
            interaction: discord.Interaction, table: str = "", clear: bool = False
        ) -> None:
            await self._run(
                interaction,
                handlers.enrich(
                    to_identity(_interaction_context(interaction)),
                    table=table,
                    clear=clear,
                ),
            )

        @tree.command(
            name="term_custom",
            description="비즈니스 용어 등록·조회·삭제 (action: show / remove, term: 용어명)",
        )
        async def term_custom(
            interaction: discord.Interaction,
            action: str = "",
            term: str = "",
            layer: str = "member",
        ) -> None:
            ident = to_identity(_interaction_context(interaction))
            if action == "show":
                await self._run(interaction, handlers.term_custom(ident, list_all=True))
            elif action == "remove":
                await self._run(
                    interaction,
                    handlers.term_custom(ident, term=term, layer=layer, remove=True),
                )
            else:
                from .term_wizard import start_term_add_flow

                await start_term_add_flow(interaction, handlers, _interaction_context)

        @tree.command(
            name="org_setup",
            description="조직(전사) 또는 팀(채널) 등록 + DB 스캔으로 비즈니스 용어 자동 추출",
        )
        async def org_setup(
            interaction: discord.Interaction,
            org: str = "",
            team: str = "",
            clear: bool = False,
        ) -> None:
            await self._run(
                interaction,
                handlers.org_setup(
                    to_identity(_interaction_context(interaction)),
                    org=org,
                    team=team,
                    clear=clear,
                ),
            )

        @tree.command(
            name="semantic_status",
            description="DB 연결 후 자동 준비와 현재 확인 대기 상태 보기",
        )
        async def semantic_status(interaction: discord.Interaction) -> None:
            await self._run(
                interaction,
                handlers.semantic_status(
                    to_identity(_interaction_context(interaction))
                ),
            )

        @tree.command(
            name="semantic_reset",
            description="사람이 확인한 의미 연결을 초기화 (관리자, confirm 필요)",
        )
        async def semantic_reset(
            interaction: discord.Interaction, confirm: bool = False
        ) -> None:
            await self._run(
                interaction,
                handlers.semantic_reset(
                    to_identity(_interaction_context(interaction)), confirm=confirm
                ),
            )

        @tree.command(
            name="semantic_review",
            description="질문 표현과 DB 컬럼·집계 연결을 확인하고 원래 질문 재개",
        )
        @app_commands.choices(
            aggregate=[
                app_commands.Choice(name="합계 (SUM)", value="sum"),
                app_commands.Choice(name="평균 (AVG)", value="avg"),
                app_commands.Choice(name="최솟값 (MIN)", value="min"),
                app_commands.Choice(name="최댓값 (MAX)", value="max"),
                app_commands.Choice(name="개수 (COUNT)", value="count"),
                app_commands.Choice(name="표현과 컬럼 연결 확인", value="confirm"),
                app_commands.Choice(name="이 후보 사용 안 함", value="reject"),
            ]
        )
        async def semantic_review(
            interaction: discord.Interaction,
            aggregate: app_commands.Choice[str],
        ) -> None:
            await self._run(
                interaction,
                handlers.semantic_review(
                    to_identity(_interaction_context(interaction)), aggregate.value
                ),
            )

        @tree.command(name="audit_me", description="Show your recent activity")
        async def audit_me(interaction: discord.Interaction) -> None:
            await self._run(
                interaction,
                handlers.audit_me(to_identity(_interaction_context(interaction))),
            )

        @tree.command(name="help", description="Lang2SQL 사용 방법 안내")
        async def help(interaction: discord.Interaction) -> None:
            await self._run(interaction, handlers.help())

    async def _run(self, interaction: discord.Interaction, coro) -> None:
        """Await a handler coroutine and reply with its OutboundMessage."""
        await interaction.response.defer(thinking=True)
        try:
            message = await coro
            kwargs = _build_send_kwargs(message)
            await interaction.followup.send(**kwargs)
        except Exception as exc:
            import traceback

            traceback.print_exc()
            try:
                await interaction.followup.send(
                    content=f"❌ 요청을 처리하지 못했습니다. (오류 유형: {type(exc).__name__})"
                )
            except Exception:
                pass

    async def on_message(self, message: discord.Message) -> None:
        """Treat an explicit @mention as a free-form query."""
        if message.author == self.user:
            return
        bot_user_id = self.user.id if self.user is not None else None
        if not _is_direct_user_mention(message, bot_user_id):
            return

        text = message.content
        if self.user is not None:
            text = text.replace(f"<@{self.user.id}>", "")
            text = text.replace(f"<@!{self.user.id}>", "").strip()
        if not text:
            return

        identity = to_identity(_message_context(message))
        try:
            out = await self._command_handlers.query(identity, text)
            kwargs = _build_send_kwargs(out)
            await message.channel.send(**kwargs)
        except Exception as exc:
            import traceback

            traceback.print_exc()
            await message.channel.send(
                content=f"❌ 요청을 처리하지 못했습니다. (오류 유형: {type(exc).__name__})"
            )


def run() -> None:
    """Entry point for the ``lang2sql-bot`` script: connect and serve.

    Reads the token from :data:`TOKEN_ENV`; raises a clear error if it's unset
    so a misconfigured deploy fails loudly rather than hanging on the gateway.
    """
    token = os.environ.get(TOKEN_ENV)
    if not token:
        raise RuntimeError(
            f"{TOKEN_ENV} is not set; export your Discord bot token to run the bot."
        )
    data_path = os.environ.get("LANG2SQL_DATA_PATH", "lang2sql_data.db")
    handlers = CommandHandlers(ContextConcierge(path=data_path))
    client = Lang2SQLBot(handlers)
    client.run(token)
