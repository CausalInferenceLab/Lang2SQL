"""CommandHandlers — the V1 Discord command surface, free of discord.py.

Each public method takes plain arguments (an :class:`Identity` plus strings) and
returns an :class:`OutboundMessage`, so the whole command layer is unit-testable
without a gateway connection. ``bot.py`` is the only module that knows about
slash commands, embeds, and ``discord.File``; it parses an interaction, picks a
handler here, and renders the result.

The handlers drive the harness through a :class:`ContextConcierge`: every call
builds a fresh :class:`HarnessContext` for the identity (restoring its session),
then either runs the agent loop (``query``) or invokes a single ctx-aware tool /
port directly (the structured commands). Running the real tools — rather than
re-implementing their logic — keeps federation scoping, audit logging, and
memory writes identical to what the agent itself does.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from ...adapters.db import build_explorer
from ...adapters.db.dsn_builder import assemble
from ...core.identity import Identity
from ...core.ports.frontend import OutboundMessage
from ...core.types import Role
from ...harness.loop import agent_loop
from ...semantic.service import review_scope_key
from ...tenancy.concierge import ContextConcierge
from .render import render_answer


class CommandHandlers:
    """Async command methods returning :class:`OutboundMessage` (discord-free)."""

    def __init__(self, concierge: ContextConcierge) -> None:
        self._concierge = concierge

    async def query(self, identity: Identity, text: str) -> OutboundMessage:
        """Run a natural-language question through the agent loop, then render.

        The loop mutates the in-context :class:`Session`; we persist it back
        through the concierge store afterwards so the next message in the same
        thread/DM continues the conversation (tiebreaker #4).
        """
        ctx = await self._concierge.build_context(identity, user_text=text)
        blocked_column = self._concierge.semantic.blocked_column_in_question(
            identity.kv_scope, text
        )
        if blocked_column:
            return OutboundMessage(
                text=(
                    "BLOCKED (policy_blocked_column): 질문에 기본 차단 컬럼 "
                    f"`{blocked_column}`이 포함되어 있습니다. 이 경로에서는 "
                    "등록이나 실행으로 우회하지 않습니다."
                )
            )
        governed_mode = "semantic_query" in {item.name for item in ctx.tools.specs()}
        pre_loop_len = len(ctx.session.history())
        answer = await agent_loop(ctx, text)

        history = ctx.session.history()
        current_turn = history[pre_loop_len:]

        call_id_to_sql: dict[str, str] = {
            tc.id: tc.arguments["sql"]
            for msg in current_turn
            if msg.role == Role.ASSISTANT and msg.tool_calls
            for tc in msg.tool_calls
            if tc.name == "run_sql" and "sql" in tc.arguments
        }

        sql_queries: list[str] = []
        sql_results: list[str] = []
        semantic_results: list[str] = []
        clarification_results: list[str] = []
        for msg in current_turn:
            if msg.role != Role.TOOL or not msg.content:
                continue
            if msg.name == "semantic_query":
                semantic_results.append(msg.content)
                continue
            if msg.name == "ask_user":
                clarification_results.append(msg.content)
                continue
            if msg.name != "run_sql":
                continue
            sql = call_id_to_sql.get(msg.tool_call_id or "")
            if sql and ("row(s):" in msg.content or "(0 rows)" in msg.content):
                sql_queries.append(sql)
                sql_results.append(msg.content)

        ctx.session.compress()
        await self._concierge.store.save(identity.session_key(), ctx.session)

        # In governed mode the tool output is the authoritative result.  Using
        # it directly prevents the final prose model from changing readiness,
        # dropping a clarification, or rewriting the compiled SQL.
        if semantic_results:
            return render_answer(semantic_results[-1])
        if governed_mode and clarification_results:
            return render_answer(_safe_clarification(clarification_results[-1]))
        if governed_mode:
            return OutboundMessage(
                text=(
                    "BLOCKED (semantic_tool_not_called): 검토 가능한 query slots가 "
                    "생성되지 않았습니다. 지표와 분류 기준을 더 구체적으로 말해 주세요."
                )
            )

        suffix = ""
        if sql_queries:
            suffix += "\n\n**SQL:**\n```sql\n" + "\n\n".join(sql_queries) + "\n```"
        if sql_results:
            suffix += "\n\n**결과:**\n```\n" + "\n\n".join(sql_results) + "\n```"
        return render_answer(answer + suffix)

    async def remember(self, identity: Identity, text: str) -> OutboundMessage:
        """Persist a user fact via the memory service (manual ``/remember``)."""
        ctx = await self._concierge.build_context(identity)
        result = await ctx.tools.dispatch(
            "remember", {"text": text}, ctx, "cmd:remember"
        )
        return OutboundMessage(text=result.content)

    async def audit_me(self, identity: Identity) -> OutboundMessage:
        """List the caller's recent audited actions, newest first."""
        ctx = await self._concierge.build_context(identity)
        if ctx.audit is None:
            return OutboundMessage(text="Audit log unavailable.")
        events = await ctx.audit.query(identity.user_id)
        if not events:
            return OutboundMessage(text="No audited activity for you yet.")
        lines = ["Your recent activity:"]
        for event in events:
            lines.append(f"- {_fmt_ts(event.ts)} {event.action} @ {event.scope}")
        return OutboundMessage(text="\n".join(lines))

    async def register_db_for_guild(
        self,
        identity: Identity,
        db_type: str,
        fields: dict[str, str],
    ) -> OutboundMessage:
        """The /setup wizard's commit step (non-developer entry point).

        Takes the wizard's per-field inputs (no DSN literals), assembles the
        DSN, tests the connection by listing tables once, and on success
        stores the DSN (+ any out-of-band token) under the guild's scope via
        :class:`EncryptedSecrets`. The next ``build_context`` for this guild
        will use this DB transparently.
        """
        if identity.guild_id and not identity.is_admin:
            return OutboundMessage(
                text="❌ 서버 DB 연결은 관리자만 설정할 수 있습니다."
            )
        try:
            spec = assemble(db_type, fields)
        except ValueError as exc:
            # assemble() errors contain only field names or a DB type, never
            # submitted credential values, so this detail is safe and useful.
            return OutboundMessage(text=f"⚠️ Setup error: {exc}")

        try:
            explorer = build_explorer(spec.dsn, extras=spec.extras)
            tables = await explorer.list_tables()
        except ModuleNotFoundError as exc:
            return OutboundMessage(
                text=(
                    f"⚠️ Connection driver not installed for {db_type}. "
                    f"Ask an admin to run `uv sync --extra {db_type}`.\n"
                    f"(오류 유형: {type(exc).__name__})"
                )
            )
        except Exception as exc:  # surface what the DB said, but stay user-friendly
            return OutboundMessage(
                text=(
                    f"❌ Couldn't connect to {db_type}. "
                    f"(오류 유형: {type(exc).__name__})\n"
                    "Common causes: wrong host/port, network/firewall, "
                    "wrong credentials, or read permission missing."
                )
            )

        return await self._store_connection_and_onboard(
            identity=identity,
            db_type=db_type,
            dsn=spec.dsn,
            extras=spec.extras,
            explorer=explorer,
            table_count=len(tables),
        )

    async def enrich(
        self, identity: Identity, table: str = "", clear: bool = False
    ) -> OutboundMessage:
        """Run EnrichSchema tool: sample DB columns and LLM-infer descriptions."""
        if self._concierge.semantic.load(identity.kv_scope) is not None:
            return OutboundMessage(
                text=(
                    "`/enrich`의 원시 값 샘플링은 semantic first-connect 모드에서 "
                    "비활성화됩니다. 구조 메타데이터만 사용하는 자동 준비 결과를 "
                    "`/semantic_status`에서 확인해 주세요."
                )
            )
        ctx = await self._concierge.build_context(identity)
        result = await ctx.tools.dispatch(
            "enrich_schema", {"table": table, "clear": clear}, ctx, "cmd:enrich"
        )
        return OutboundMessage(text=result.content)

    async def org_setup(
        self, identity: Identity, org: str = "", team: str = "", clear: bool = False
    ) -> OutboundMessage:
        """조직(전사) 또는 팀(채널) 등록 + DB 스캔으로 비즈니스 용어 자동 추출."""
        if self._concierge.semantic.load(identity.kv_scope) is not None:
            return OutboundMessage(
                text=(
                    "`/org_setup`의 자동 샘플 추론은 semantic first-connect 모드에서 "
                    "비활성화됩니다. 필요한 업무 의미는 실제 질문에서 한 번씩 "
                    "확인합니다."
                )
            )
        ctx = await self._concierge.build_context(identity)
        result = await ctx.tools.dispatch(
            "org_setup",
            {"org": org, "team": team, "clear": clear},
            ctx,
            "cmd:org_setup",
        )
        return OutboundMessage(text=result.content)

    async def term_custom(
        self,
        identity: Identity,
        term: str = "",
        definition: str = "",
        layer: str = "member",
        synonyms: str = "",
        inferred: bool = False,
        scan: bool = False,
        remove: bool = False,
        list_all: bool = False,
    ) -> OutboundMessage:
        """채널(팀)/전사/개인 계층 비즈니스 용어 사전 관리."""
        ctx = await self._concierge.build_context(identity)
        result = await ctx.tools.dispatch(
            "term_custom",
            {
                "term": term,
                "definition": definition,
                "layer": layer,
                "synonyms": synonyms,
                "inferred": inferred,
                "scan": scan,
                "remove": remove,
                "list": list_all,
            },
            ctx,
            "cmd:term_custom",
        )
        return OutboundMessage(text=result.content)

    async def connect(self, identity: Identity, dsn: str) -> OutboundMessage:
        """Connect a DSN through the same encrypted first-connect path as /setup."""

        if identity.guild_id and not identity.is_admin:
            return OutboundMessage(
                text="❌ 서버 DB 연결은 관리자만 설정할 수 있습니다."
            )
        dsn = dsn.strip()
        if not dsn:
            return OutboundMessage(text="Provide a database connection string.")
        try:
            explorer = build_explorer(dsn)
            tables = await explorer.list_tables()
        except Exception as exc:
            return OutboundMessage(
                text=(f"❌ DB에 연결할 수 없습니다. (오류 유형: {type(exc).__name__})")
            )
        return await self._store_connection_and_onboard(
            identity=identity,
            db_type="database",
            dsn=dsn,
            extras={},
            explorer=explorer,
            table_count=len(tables),
        )

    async def semantic_status(self, identity: Identity) -> OutboundMessage:
        return OutboundMessage(
            text=self._concierge.semantic.status_text(
                identity.kv_scope,
                review_scope_key(identity.session_key(), identity.user_id),
            )
        )

    async def semantic_reset(
        self, identity: Identity, confirm: bool = False
    ) -> OutboundMessage:
        if identity.guild_id and not identity.is_admin:
            return OutboundMessage(
                text="❌ 의미 검토 초기화는 관리자만 할 수 있습니다."
            )
        if not confirm:
            return OutboundMessage(
                text=(
                    "⚠️ 사람이 확인한 모든 표현·집계 연결을 초기화합니다. "
                    "실행하려면 `confirm:true`로 다시 호출해 주세요."
                )
            )
        outcome = self._concierge.semantic.reset_reviews(identity.kv_scope)
        return OutboundMessage(text=outcome.message)

    async def semantic_review(
        self, identity: Identity, aggregate: str
    ) -> OutboundMessage:
        """Apply the one pending metric decision and resume its original question."""

        normalized = aggregate.strip().lower()
        outcome = self._concierge.semantic.confirm_pending(
            identity.kv_scope,
            review_scope_key(identity.session_key(), identity.user_id),
            normalized,
            reviewer_id=identity.user_id,
        )
        if (
            outcome.status != "confirmed"
            or not outcome.question
            or normalized == "reject"
        ):
            return OutboundMessage(text=outcome.message)
        # Resume the exact reviewed draft directly. Re-running the LLM here
        # would let it pick a different metric after the user approved one.
        ctx = await self._concierge.build_context(identity)
        result = await ctx.tools.dispatch(
            "semantic_query",
            outcome.tool_args,
            ctx,
            "cmd:semantic_review:resume",
        )
        return OutboundMessage(text=f"✅ {outcome.message}\n\n{result.content}")

    async def _store_connection_and_onboard(
        self,
        *,
        identity: Identity,
        db_type: str,
        dsn: str,
        extras: dict[str, str],
        explorer,
        table_count: int,
    ) -> OutboundMessage:
        scope = identity.kv_scope
        try:
            # Build the complete candidate before changing the active DSN or
            # catalog. A failed scan leaves the working connection untouched.
            summary = await self._concierge.semantic.inspect(scope, explorer)
        except Exception as exc:
            return OutboundMessage(
                text=(
                    f"⚠️ **{db_type}** 연결 확인 후 의미 준비에 실패했습니다. "
                    "기존 연결은 변경하지 않았습니다. `/setup`을 다시 실행해 주세요. "
                    f"(오류 유형: {type(exc).__name__})"
                )
            )

        try:
            self._concierge.activate_connection(
                scope=scope,
                dsn=dsn,
                extras=extras,
                catalog=summary.catalog,
            )
        except Exception as exc:
            return OutboundMessage(
                text=(
                    "⚠️ 새 연결을 원자적으로 활성화하지 못했습니다. 기존 연결은 "
                    f"변경되지 않았습니다. (오류 유형: {type(exc).__name__})"
                )
            )

        return OutboundMessage(
            text=(
                f"✅ **{db_type}** 연결 완료 — 테이블 {table_count}개를 읽었습니다.\n"
                f"- 선언된 안전 조인 {summary.declared_join_count}개 자동 등록\n"
                f"- 개인정보 의심 컬럼 {summary.blocked_column_count}개 기본 차단\n"
                f"- 물리 구조 검토 질문 0개\n"
                "업무 지표는 지금 전부 묻지 않습니다. 바로 질문하면 필요한 의미만 "
                "한 번 확인하고 이후 재사용합니다. `/semantic_status`에서 상태를 볼 수 있습니다."
            )
        )

    async def ingest(
        self,
        identity: Identity,
        ref: str | None = None,
        content: str | None = None,
    ) -> OutboundMessage:
        """Propose semantic definitions extracted from a document.

        Lists the candidates via the ``ingest_doc`` tool; confirmation buttons
        are ``bot.py``'s job (V1 just surfaces the list, keeping the human in
        the loop before anything enters the semantic layer).
        """
        ctx = await self._concierge.build_context(identity)
        args: dict[str, str] = {}
        if ref:
            args["ref"] = ref
        if content:
            args["content"] = content
        result = await ctx.tools.dispatch("ingest_doc", args, ctx, "cmd:ingest")
        return OutboundMessage(text=result.content)

    async def confirm_ingest(
        self,
        identity: Identity,
        ref: str,
        accept: str = "all",
        layer: str = "channel",
    ) -> OutboundMessage:
        """ingest_doc로 추출한 후보를 검토 후 시멘틱 레이어에 등록."""
        ctx = await self._concierge.build_context(identity)
        result = await ctx.tools.dispatch(
            "confirm_ingest",
            {"ref": ref, "accept": accept, "layer": layer},
            ctx,
            "cmd:confirm_ingest",
        )
        return OutboundMessage(text=result.content)

    async def help(self) -> OutboundMessage:
        """사용 방법 안내."""
        text = """\
**Lang2SQL 사용 가이드**

**📊 질문하기**
모든 채널, thread, DM에서 봇을 명시적으로 멘션해 질문하세요.
> @Lang2SQL 이번 달 매출 상위 고객 10명 알려줘

**🗄️ DB 연결** (관리자)
`/setup` — 안내에 따라 DB 접속 정보 입력

Discord에는 credential-bearing DSN을 직접 받는 `/connect`를 노출하지 않습니다.

연결 직후 PK/FK·컬럼 타입·개인정보 의심 컬럼은 자동 정리됩니다.
업무 지표는 실제 질문에 등장할 때만 `/semantic_review`로 한 번 확인합니다.
`/semantic_status` — 현재 준비 상태 확인
`/semantic_reset confirm:true` — 사람이 확인한 의미 연결 초기화 (관리자)

**📖 비즈니스 용어 등록**
`/ingest content:월매출은 SUM(orders.amount), 활성고객은 30일 내 로그인`
→ 후보 추출 후 아래 커맨드로 확정
`/confirm_ingest ref:inline:xxxx accept:all layer:channel`

`/term_custom` — 용어 직접 등록 (위저드)
`/term_custom action:show` — 등록된 용어 조회
`/org_setup org:회사명` — DB 스캔으로 용어 자동 추출

semantic first-connect 활성화 후에는 raw-value sampling을 쓰는 `/org_setup`과
`/enrich`가 안전상 비활성화됩니다.

**🏷️ 용어 우선순위**
개인(member) > 채널(channel) > 전사(guild)
같은 채널 안에서 등록한 정의가 전사 정의보다 우선 적용됩니다.

**🔧 기타**
`/enrich` — legacy raw 모드에서만 DB 컬럼 설명 자동 보강
`/remember text:...` — 사실 저장
`/audit_me` — 내 활동 이력 조회
`/help` — 이 도움말"""
        return OutboundMessage(text=text)


def _safe_clarification(content: str) -> str:
    """Never echo model-generated SQL through the clarification channel."""

    if re.search(
        r"```|;|\b(select|with|from|join|where|insert|update|delete|drop)\b",
        content,
        re.IGNORECASE,
    ):
        return (
            "NEEDS CLARIFICATION: 요청을 검토 가능한 지표·분류 슬롯으로 "
            "확정하지 못했습니다. SQL 형태가 아닌 업무 의미로 다시 말해 주세요."
        )
    return content


def _fmt_ts(ts: float) -> str:
    """Format an epoch timestamp as a short UTC string for audit listings."""
    if not ts:
        return "?"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
