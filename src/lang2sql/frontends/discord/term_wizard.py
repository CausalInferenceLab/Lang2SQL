"""term_wizard.py — /term_custom 등록 폼 (2단계 UI).

Step 1: Select — 전사(org) / 채널·팀(channel) / 개인(member) 선택
Step 2: Modal — 용어명·정의·동의어 입력

채널이 팀 경계 역할을 하므로 entity 직접 입력 불필요.
setup_wizard.py 패턴 동일: Select 선택 → Modal 응답.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import discord
from discord import ui

from .session_router import to_identity

if TYPE_CHECKING:
    from .commands import CommandHandlers


_LAYER_OPTIONS = [
    discord.SelectOption(
        label="전사 (Guild) — 회사 공통 정의",
        value="org",
        description="모든 채널에서 기본값으로 사용",
    ),
    discord.SelectOption(
        label="채널 (팀) — 이 채널 전용 정의",
        value="team",
        description="다른 채널과 충돌 없이 이 채널에서만 유효",
    ),
    discord.SelectOption(
        label="개인 — 나만 사용하는 정의",
        value="user",
        description="전사·채널 정의를 조용히 덮어씀",
    ),
]


class _TermModal(ui.Modal, title="비즈니스 용어 등록"):
    term = ui.TextInput(
        label="용어명",
        placeholder="예: 활성고객",
        required=True,
        max_length=100,
    )
    definition = ui.TextInput(
        label="정의",
        placeholder="예: 최근 30일 내 로그인한 users",
        required=True,
        style=discord.TextStyle.paragraph,
        max_length=500,
    )
    synonyms = ui.TextInput(
        label="동의어 (쉼표 구분, 선택)",
        placeholder="예: active user, 활성화고객",
        required=False,
        max_length=200,
    )

    def __init__(self, layer: str, handlers: "CommandHandlers", ctx_factory) -> None:
        super().__init__()
        self._layer = layer
        self._handlers = handlers
        self._ctx_factory = ctx_factory

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            identity = to_identity(self._ctx_factory(interaction))
            result = await self._handlers.term_custom(
                identity,
                term=self.term.value.strip(),
                definition=self.definition.value.strip(),
                layer=self._layer,
                synonyms=self.synonyms.value.strip(),
            )
            await interaction.followup.send(result.text, ephemeral=True)
        except Exception as exc:
            try:
                await interaction.followup.send(f"❌ 오류: {exc}", ephemeral=True)
            except Exception:
                pass


class _LayerSelect(ui.Select):
    def __init__(self, handlers: "CommandHandlers", ctx_factory) -> None:
        super().__init__(
            placeholder="적용 범위를 선택하세요…",
            options=_LAYER_OPTIONS,
            min_values=1,
            max_values=1,
        )
        self._handlers = handlers
        self._ctx_factory = ctx_factory

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_modal(
            _TermModal(self.values[0], self._handlers, self._ctx_factory)
        )


class _LayerSelectView(ui.View):
    def __init__(self, handlers: "CommandHandlers", ctx_factory) -> None:
        super().__init__(timeout=120.0)
        self.add_item(_LayerSelect(handlers, ctx_factory))


async def start_term_add_flow(
    interaction: discord.Interaction,
    handlers: "CommandHandlers",
    ctx_factory,
) -> None:
    """bot.py의 /term_custom 커맨드에서 호출 — 범위 선택 → 용어 등록 모달."""
    await interaction.response.send_message(
        "용어를 등록할 **범위**를 선택하세요.\n"
        "- **전사**: 모든 채널에서 기본값\n"
        "- **채널(팀)**: 이 채널에서만 유효 (다른 채널과 충돌 없음)\n"
        "- **개인**: 나만 사용하는 정의 (전사·채널 정의를 덮어씀)",
        view=_LayerSelectView(handlers, ctx_factory),
        ephemeral=True,
    )
