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

from sqlalchemy.exc import NoSuchModuleError

from ...adapters.db import build_explorer
from ...adapters.db.factory import canonicalize_connection
from ...adapters.db.dsn_builder import assemble
from ...core.identity import Identity
from ...core.ports.explorer import close_explorer
from ...core.ports.frontend import OutboundMessage
from ...core.types import Message, Role
from ...harness.loop import agent_loop
from ...semantic.catalog import DimensionDisclosureTier, PendingReview
from ...semantic.service import StewardAssertion, review_scope_key
from ...tenancy.concierge import ContextConcierge
from .render import render_answer, sanitize_discord_text

_ACTION_TOKEN_DISPLAY_RE = re.compile(r"^[A-Za-z0-9_-]{16,96}$")


class CommandHandlers:
    """Async command methods returning :class:`OutboundMessage` (discord-free)."""

    def __init__(
        self,
        concierge: ContextConcierge,
        *,
        query_channel_ids: frozenset[str] | set[str] | None = None,
    ) -> None:
        self._concierge = concierge
        # A connected database is not automatically public to every Discord
        # member. Deployments must deliberately opt parent channels into query
        # access; Discord threads inherit their parent channel identifier.
        self._query_channel_ids = frozenset(query_channel_ids or ())

    def _semantic_audit_blocker(self) -> OutboundMessage | None:
        # Governance state and its audit event must share one transaction.
        # External audit adapters cannot provide that atomicity in this V1 seam.
        if self._concierge.audit is self._concierge.store:
            return None
        return OutboundMessage(
            text="BLOCKED: 의미 검토 변경에는 기본 원자적 audit 저장소가 필요합니다."
        )

    def _semantic_result_is_current(self, identity: Identity, ctx) -> bool:
        """Recheck the exact catalog stamp immediately before Discord render."""

        current_catalog = self._concierge.semantic.load(identity.kv_scope)
        current_stamp = (
            (
                current_catalog.source_id,
                current_catalog.connection_generation,
                current_catalog.fingerprint,
                current_catalog.review_revision,
                current_catalog.version,
                current_catalog.classification_policy_version,
            )
            if current_catalog is not None
            else ()
        )
        return bool(ctx.semantic_result_stamp) and (
            current_stamp == ctx.semantic_result_stamp
        )

    @staticmethod
    def _discard_semantic_result(ctx) -> None:
        ctx.semantic_result_ready = False
        ctx.semantic_result_headers = ()
        ctx.semantic_result_rows = []
        ctx.semantic_result_stamp = ()

    async def query(self, identity: Identity, text: str) -> OutboundMessage:
        """Run a natural-language question through the agent loop, then render.

        The loop mutates the in-context :class:`Session`; we persist it back
        through the concierge store afterwards so the next message in the same
        thread/DM continues the conversation (tiebreaker #4).
        """
        if (
            identity.guild_id
            and not identity.is_admin
            and identity.effective_channel_id not in self._query_channel_ids
        ):
            return OutboundMessage(
                text=(
                    "BLOCKED (guild_query_access): 이 봇은 기본적으로 Discord 서버 "
                    "관리자만 연결 DB를 질의할 수 있습니다. 일반 구성원의 질의는 "
                    "운영자가 `LANG2SQL_DISCORD_QUERY_CHANNEL_IDS`에 현재 상위 "
                    "채널 ID를 명시한 경우에만 허용됩니다."
                )
            )
        ctx = await self._concierge.build_context(identity, user_text=text)
        blocked_column = self._concierge.semantic.blocked_column_in_question(
            identity.kv_scope, text
        )
        if blocked_column:
            if ctx.session.discard_transient():
                await self._concierge.store.save(identity.session_key(), ctx.session)
            return OutboundMessage(
                text=(
                    "BLOCKED (policy_blocked_column): 질문에 기본 차단 컬럼 "
                    f"{_display(blocked_column)}이 포함되어 있습니다. 이 경로에서는 "
                    "등록이나 실행으로 우회하지 않습니다."
                )
            )
        if ctx.semantic_attention_state and ctx.semantic_attention_state != "ready":
            if ctx.session.discard_transient():
                await self._concierge.store.save(identity.session_key(), ctx.session)
            return OutboundMessage(
                text=(
                    "NEEDS CLARIFICATION (semantic_candidate_scope): "
                    + sanitize_discord_text(
                        ctx.semantic_attention_message, max_length=1500
                    )
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

        persisted_clarification = (
            _safe_clarification(clarification_results[-1])
            if governed_mode and clarification_results
            else ""
        )
        # A suspended clarification stores only the exact sanitized text shown
        # to the user.  Model prose emitted beside ask_user is discarded, and
        # the transient state survives for one real user response only.
        ctx.session.compress(preserve_tool_content=not persisted_clarification)
        if persisted_clarification:
            ctx.session.add(
                Message(
                    role=Role.ASSISTANT,
                    content=persisted_clarification,
                    transient=True,
                )
            )
        await self._concierge.store.save(identity.session_key(), ctx.session)

        # In reviewed-query mode the tool output is authoritative. Using it
        # directly prevents the final prose model from changing readiness,
        # dropping a clarification, or rewriting the compiled SQL.
        if semantic_results:
            if ctx.semantic_result_ready:
                if not self._semantic_result_is_current(identity, ctx):
                    self._discard_semantic_result(ctx)
                    return OutboundMessage(
                        text=(
                            "BLOCKED (semantic_result_stale_before_render): 결과 표시 "
                            "직전에 DB 연결 또는 의미·공개 상태가 바뀌어 결과를 "
                            "폐기했습니다. 질문을 다시 실행해 주세요."
                        )
                    )
                return render_answer(
                    f"READY: {ctx.semantic_result_message}",
                    ctx.semantic_result_rows,
                    header=ctx.semantic_result_headers,
                )
            # SemanticService emits a fixed clarification template and an
            # opaque server-owned review ID; DB/user phrases are shown only in
            # the separately escaped steward queue.
            return render_answer(semantic_results[-1])
        if governed_mode and clarification_results:
            return render_answer(persisted_clarification)
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
        result = await ctx.tools.dispatch_direct(
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

        explorer = None
        try:
            explorer = build_explorer(spec.dsn, extras=spec.extras)
            tables = await explorer.list_tables()
        except (ModuleNotFoundError, NoSuchModuleError) as exc:
            if explorer is not None:
                close_explorer(explorer)
            return OutboundMessage(
                text=(
                    f"⚠️ Connection driver not installed for {db_type}. "
                    f"Ask an admin to run `uv sync --extra {db_type}`.\n"
                    f"(오류 유형: {type(exc).__name__})"
                )
            )
        except Exception as exc:  # surface what the DB said, but stay user-friendly
            if explorer is not None:
                close_explorer(explorer)
            cause = (
                "파일 절대경로가 실제로 존재하는지, 봇 프로세스에 읽기 권한이 "
                "있는지, 유효한 DB 파일인지 확인해 주세요."
                if db_type in {"sqlite", "duckdb"}
                else (
                    "Common causes: wrong host/port, network/firewall, "
                    "wrong credentials, or read permission missing."
                )
            )
            return OutboundMessage(
                text=(
                    f"❌ Couldn't connect to {db_type}. "
                    f"(오류 유형: {type(exc).__name__})\n" + cause
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
                    "`/enrich`의 원시 값 샘플링은 연결 즉시 의미 준비형 질의 모드에서 "
                    "비활성화됩니다. 구조 메타데이터만 사용하는 자동 준비 결과를 "
                    "`/semantic_status`에서 확인해 주세요."
                )
            )
        ctx = await self._concierge.build_context(identity)
        result = await ctx.tools.dispatch_direct(
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
                    "`/org_setup`의 자동 샘플 추론은 연결 즉시 의미 준비형 질의 모드에서 "
                    "비활성화됩니다. 필요한 업무 의미는 실제 질문에서 한 번씩 "
                    "확인합니다."
                )
            )
        ctx = await self._concierge.build_context(identity)
        result = await ctx.tools.dispatch_direct(
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
        result = await ctx.tools.dispatch_direct(
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
        explorer = None
        try:
            explorer = build_explorer(dsn)
            tables = await explorer.list_tables()
        except Exception as exc:
            if explorer is not None:
                close_explorer(explorer)
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
        if identity.guild_id and not identity.is_admin:
            return OutboundMessage(
                text="❌ DB 의미 준비 상태는 관리자만 볼 수 있습니다."
            )
        return render_answer(
            self._concierge.semantic.status_text(
                identity.kv_scope,
                review_scope_key(identity.session_key(), identity.user_id),
            ),
            file_name="semantic-status.txt",
        )

    async def semantic_candidates(
        self,
        identity: Identity,
        page: int = 1,
        search: str = "",
        state: str = "pending",
    ) -> OutboundMessage:
        """List metadata-only dimensions awaiting grouped-value review."""

        if identity.guild_id and not identity.is_admin:
            return OutboundMessage(
                text="❌ 값 공개 검토 차원은 관리자만 볼 수 있습니다."
            )
        catalog_snapshot, candidates = (
            self._concierge.semantic.dimension_candidate_snapshot(
                identity.kv_scope, include_released=True
            )
        )
        normalized_state = state.strip().lower()
        if normalized_state not in {"pending", "released", "all"}:
            return OutboundMessage(text="state는 pending, released, all 중 하나입니다.")
        if normalized_state == "pending":
            candidates = [item for item in candidates if not item.raw_output_allowed]
        elif normalized_state == "released":
            candidates = [item for item in candidates if item.raw_output_allowed]
        needle = search.strip().lower()
        if needle:
            candidates = [
                item
                for item in candidates
                if needle
                in " ".join((item.id, item.label, item.classification_evidence)).lower()
            ]
        if not candidates:
            return OutboundMessage(text="조건에 맞는 값 공개 검토 차원이 없습니다.")
        page_size = 20
        page_count = (len(candidates) + page_size - 1) // page_size
        page = max(1, int(page))
        if page > page_count:
            return OutboundMessage(text=f"페이지 범위는 1~{page_count}입니다.")
        start = (page - 1) * page_size
        shown = candidates[start : start + page_size]
        lines = [
            f"**관리자 값 공개 검토 — {page}/{page_count} 페이지**",
            "아래 항목은 값 샘플을 읽지 않고 컬럼 메타데이터만으로 분류됐습니다.",
        ]
        for candidate in shown:
            release_state = "released" if candidate.raw_output_allowed else "pending"
            candidate_token = self._concierge.semantic.issue_dimension_action_token(
                identity.kv_scope,
                candidate.id,
                "dimension_set_tier",
                expected_catalog=catalog_snapshot,
            )
            if not candidate_token:
                return OutboundMessage(
                    text=(
                        "후보를 표시하는 동안 DB 연결 또는 의미 검토 상태가 "
                        "바뀌었습니다. `/semantic_candidates`를 다시 실행해 주세요."
                    )
                )
            revoke_text = ""
            if candidate.raw_output_allowed:
                revoke_token = self._concierge.semantic.issue_dimension_action_token(
                    identity.kv_scope,
                    candidate.id,
                    "dimension_revoke",
                    expected_catalog=catalog_snapshot,
                )
                if not revoke_token:
                    return OutboundMessage(
                        text=(
                            "후보를 표시하는 동안 DB 연결 또는 의미 검토 상태가 "
                            "바뀌었습니다. `/semantic_candidates`를 다시 실행해 주세요."
                        )
                    )
                revoke_text = f" / revoke_token: {_display_token(revoke_token)}"
            lines.append(
                f"- candidate_token: {_display_token(candidate_token)}{revoke_text} / "
                "action: set_tier"
                f"{' or revoke' if candidate.raw_output_allowed else ''} / 차원: "
                f"{_display(candidate.id)} — {release_state} / 등급: "
                f"{_display(candidate.disclosure_tier.value)} / 타입: "
                f"{_display(candidate.data_type)} / 근거: "
                f"{_display(candidate.classification_evidence)}"
            )
        lines.append(
            "`page`, `search`, `state`로 전체 후보를 탐색할 수 있습니다. 공개가 "
            "적절한 등급은 `/semantic_release candidate_token:...`으로 설정하거나 "
            "변경합니다. 철회는 `/semantic_revoke candidate_token:...`에 표시된 "
            "`revoke_token`을 사용합니다. 토큰은 15분 동안 현재 연결·검토 "
            "상태에만 유효하고, 값 공개와 질문 표현 연결은 별도 결정입니다."
        )
        return render_answer(
            "\n".join(lines), file_name="semantic-dimension-candidates.txt"
        )

    async def semantic_dimension_candidates(
        self,
        identity: Identity,
        page: int = 1,
        search: str = "",
        state: str = "all",
    ) -> OutboundMessage:
        """Browse non-blocked dimension metadata without reading DB values."""

        if identity.guild_id and not identity.is_admin:
            return OutboundMessage(text="❌ 분류 후보는 관리자만 검토할 수 있습니다.")
        catalog_snapshot, candidates = (
            self._concierge.semantic.dimension_mapping_candidate_snapshot(
                identity.kv_scope
            )
        )
        normalized_state = state.strip().lower()
        if normalized_state not in {"unmapped", "mapped", "all"}:
            return OutboundMessage(text="state는 unmapped, mapped, all 중 하나입니다.")
        if normalized_state == "unmapped":
            candidates = [item for item in candidates if not item.alias_reviewers]
        elif normalized_state == "mapped":
            candidates = [item for item in candidates if item.alias_reviewers]
        needle = search.strip().lower()
        if needle:
            candidates = [
                item
                for item in candidates
                if needle
                in " ".join(
                    (
                        item.id,
                        item.label,
                        item.data_type,
                        item.classification_evidence,
                    )
                ).lower()
            ]
        if not candidates:
            return OutboundMessage(text="조건에 맞는 비차단 분류 차원이 없습니다.")
        page_size = 20
        page_count = (len(candidates) + page_size - 1) // page_size
        page = max(1, int(page))
        if page > page_count:
            return OutboundMessage(text=f"페이지 범위는 1~{page_count}입니다.")
        start = (page - 1) * page_size
        lines = [
            f"**관리자 비차단 분류 차원 — {page}/{page_count} 페이지**",
            "DB 값은 읽지 않았습니다. 타입·물리 이름·분류 근거만 보여 줍니다.",
        ]
        for dimension in candidates[start : start + page_size]:
            mapping_token = self._concierge.semantic.issue_dimension_action_token(
                identity.kv_scope,
                dimension.id,
                "dimension_map",
                expected_catalog=catalog_snapshot,
            )
            if not mapping_token:
                return OutboundMessage(
                    text=(
                        "후보를 표시하는 동안 DB 연결 또는 의미 검토 상태가 "
                        "바뀌었습니다. `/semantic_dimension_candidates`를 다시 실행해 주세요."
                    )
                )
            lines.append(
                f"- mapping_token: {_display_token(mapping_token)} / 분류: "
                f"{_display(dimension.id)} / 타입: {_display(dimension.data_type)} / "
                f"공개 정책: {_display(dimension.review_policy.value)} / 근거: "
                f"{_display(dimension.classification_evidence)} / 관리자 표현: "
                f"{len(dimension.alias_reviewers)}개"
            )
        lines.append(
            "물리 이름이 불투명하면 `mapping_token`을 "
            "`/semantic_dimension_map candidate_token:... phrase:...`에 사용하세요. "
            "토큰은 15분 동안 현재 연결·관련 분류 상태에만 유효합니다. 이 명령은 "
            "표현만 연결하며 그룹 값 공개를 승인하지 않습니다."
        )
        return render_answer("\n".join(lines), file_name="semantic-dimensions.txt")

    async def semantic_dimension_map(
        self,
        identity: Identity,
        candidate_token: str,
        phrase: str,
        confirm: bool = False,
    ) -> OutboundMessage:
        """Steward-bind a business phrase to one opaque dimension candidate."""

        if identity.guild_id and not identity.is_admin:
            return OutboundMessage(text="❌ 분류 표현 연결은 관리자만 할 수 있습니다.")
        assertion = StewardAssertion(
            scope=identity.kv_scope,
            reviewer_id=identity.user_id,
            authorized=identity.is_admin or not identity.guild_id,
        )
        if not confirm:
            armed = self._concierge.semantic.arm_dimension_mapping(
                identity.kv_scope, candidate_token, phrase, assertion
            )
            if armed.status != "confirmed":
                return OutboundMessage(
                    text="BLOCKED: " + sanitize_discord_text(armed.message)
                )
            return OutboundMessage(
                text=(
                    "⚠️ 업무 분류 표현 "
                    f"{_display(phrase)}을 선택한 분류 후보 "
                    f"{_display_token(candidate_token)}에 연결합니다. DB 값은 "
                    "읽지 않고 그룹 값 공개도 승인하지 않습니다. 이 토큰은 지금 "
                    "표시한 표현에 묶였습니다. 같은 mapping_token과 표현으로 "
                    "`confirm:true`를 호출하세요."
                )
            )
        audit_blocker = self._semantic_audit_blocker()
        if audit_blocker is not None:
            return audit_blocker
        outcome = self._concierge.semantic.map_dimension_phrase(
            identity.kv_scope,
            candidate_token,
            phrase,
            assertion,
            audit_scope=identity.session_key(),
            require_armed_payload=True,
        )
        prefix = "✅ " if outcome.status == "confirmed" else "BLOCKED: "
        return OutboundMessage(text=prefix + sanitize_discord_text(outcome.message))

    async def semantic_metric_candidates(
        self,
        identity: Identity,
        page: int = 1,
        search: str = "",
        state: str = "all",
    ) -> OutboundMessage:
        """Browse numeric measure candidates without sampling database rows."""

        if identity.guild_id and not identity.is_admin:
            return OutboundMessage(text="❌ 지표 후보는 관리자만 검토할 수 있습니다.")
        catalog_snapshot, candidates = (
            self._concierge.semantic.metric_candidate_snapshot(identity.kv_scope)
        )
        normalized_state = state.strip().lower()
        if normalized_state not in {"unmapped", "mapped", "all"}:
            return OutboundMessage(text="state는 unmapped, mapped, all 중 하나입니다.")
        if normalized_state == "unmapped":
            candidates = [item for item in candidates if not item.alias_reviewers]
        elif normalized_state == "mapped":
            candidates = [item for item in candidates if item.alias_reviewers]
        needle = search.strip().lower()
        if needle:
            candidates = [
                item
                for item in candidates
                if needle
                in " ".join(
                    (
                        item.id,
                        item.label,
                        item.data_type,
                        item.classification_evidence,
                    )
                ).lower()
            ]
        if not candidates:
            return OutboundMessage(text="조건에 맞는 수치 지표 후보가 없습니다.")
        page_size = 20
        page_count = (len(candidates) + page_size - 1) // page_size
        page = max(1, int(page))
        if page > page_count:
            return OutboundMessage(text=f"페이지 범위는 1~{page_count}입니다.")
        start = (page - 1) * page_size
        lines = [
            f"**관리자 수치 지표 후보 — {page}/{page_count} 페이지**",
            "DB 값은 읽지 않았습니다. 타입·nullable·물리 이름만 보여 줍니다.",
        ]
        for metric in candidates[start : start + page_size]:
            candidate_token = self._concierge.semantic.issue_metric_action_token(
                identity.kv_scope,
                metric.id,
                expected_catalog=catalog_snapshot,
            )
            if not candidate_token:
                return OutboundMessage(
                    text=(
                        "후보를 표시하는 동안 DB 연결 또는 의미 검토 상태가 "
                        "바뀌었습니다. `/semantic_metric_candidates`를 다시 실행해 주세요."
                    )
                )
            aggregates = ",".join(item.value for item in metric.allowed_aggregates)
            lines.append(
                f"- candidate_token: {_display_token(candidate_token)} / 지표: "
                f"{_display(metric.id)} / 타입: {_display(metric.data_type)} / "
                f"nullable: {str(metric.nullable).lower()} / 허용 집계: "
                f"{_display(aggregates)} / 근거: "
                f"{_display(metric.classification_evidence)} / 관리자 표현: "
                f"{len(metric.alias_reviewers)}개"
            )
        lines.append(
            "`search`로 물리 컬럼을 찾고 `/semantic_metric_map candidate_token:...`으로 "
            "업무 표현만 연결하세요. 토큰은 15분 동안 현재 연결·검토 상태에만 "
            "유효하고, 집계 방식은 실제 질문에서 별도 확인됩니다."
        )
        return render_answer("\n".join(lines), file_name="semantic-metrics.txt")

    async def semantic_metric_map(
        self,
        identity: Identity,
        candidate_token: str,
        phrase: str,
        confirm: bool = False,
    ) -> OutboundMessage:
        """Steward-bind a business phrase to one typed metric candidate."""

        if identity.guild_id and not identity.is_admin:
            return OutboundMessage(text="❌ 지표 표현 연결은 관리자만 할 수 있습니다.")
        assertion = StewardAssertion(
            scope=identity.kv_scope,
            reviewer_id=identity.user_id,
            authorized=identity.is_admin or not identity.guild_id,
        )
        if not confirm:
            armed = self._concierge.semantic.arm_metric_mapping(
                identity.kv_scope, candidate_token, phrase, assertion
            )
            if armed.status != "confirmed":
                return OutboundMessage(
                    text="BLOCKED: " + sanitize_discord_text(armed.message)
                )
            return OutboundMessage(
                text=(
                    "⚠️ 업무 표현 "
                    f"{_display(phrase)}을 선택한 지표 후보 "
                    f"{_display_token(candidate_token)}에 연결합니다. "
                    "DB 값은 읽지 않으며 SUM/AVG 같은 집계 의미도 승인하지 "
                    "않습니다. 이 토큰은 지금 표시한 표현에 묶였습니다. 같은 "
                    "candidate_token과 표현으로 `confirm:true`를 호출하세요."
                )
            )
        audit_blocker = self._semantic_audit_blocker()
        if audit_blocker is not None:
            return audit_blocker
        outcome = self._concierge.semantic.map_metric_phrase(
            identity.kv_scope,
            candidate_token,
            phrase,
            assertion,
            audit_scope=identity.session_key(),
            require_armed_payload=True,
        )
        prefix = "✅ " if outcome.status == "confirmed" else "BLOCKED: "
        return OutboundMessage(text=prefix + sanitize_discord_text(outcome.message))

    async def semantic_reviews(
        self,
        identity: Identity,
        page: int = 1,
        search: str = "",
    ) -> OutboundMessage:
        """Show the bounded, metadata-only guild steward approval queue."""

        if identity.guild_id and not identity.is_admin:
            return OutboundMessage(
                text="❌ 의미 검토 대기열은 관리자만 볼 수 있습니다."
            )
        queue = self._concierge.semantic.pending_review_queue(identity.kv_scope)
        needle = search.strip().lower()
        if needle:
            queue = [
                pair
                for pair in queue
                if needle
                in " ".join(
                    (
                        pair[1].review_id,
                        pair[1].requester_id,
                        *_pending_review_subject(pair[1]),
                        pair[1].review_kind,
                    )
                ).lower()
            ]
        if not queue:
            return OutboundMessage(
                text="현재 연결에 확인 대기 중인 의미 검토가 없습니다."
            )
        page_size = 20
        page_count = (len(queue) + page_size - 1) // page_size
        page = max(1, int(page))
        if page > page_count:
            return OutboundMessage(text=f"페이지 범위는 1~{page_count}입니다.")
        start = (page - 1) * page_size
        lines = [f"**관리자 의미 검토 대기열 — {page}/{page_count} 페이지**"]
        for _review_scope, pending in queue[start : start + page_size]:
            object_id, phrase = _pending_review_subject(pending)
            retained_constraints = []
            if pending.constraint_filter_count:
                retained_constraints.append(f"필터 {pending.constraint_filter_count}개")
            if pending.constraint_has_time_window:
                retained_constraints.append("DATE 기간창 1개")
            constraint_text = ", ".join(retained_constraints) or "추가 조건 없음"
            lines.append(
                f"- ID: {_display_token(pending.review_id)} / 요청자: "
                f"{_display(pending.requester_id)} / 종류: "
                f"{_display(pending.review_kind)} / 대상: "
                f"{_display(object_id)} / 표현: "
                f"{_display(phrase)} / 유지되는 typed 조건: "
                f"{_display(constraint_text)} / 선택: "
                f"{_display(','.join([*pending.allowed_choices, 'reject']))}"
            )
        lines.append(
            "`/semantic_review review_id:... aggregate:...`로 승인하거나 거절하세요. "
            "다른 사용자의 질문은 승인만 저장되며 관리자 채널에서 실행되지 않습니다."
        )
        return render_answer("\n".join(lines), file_name="semantic-reviews.txt")

    async def semantic_release(
        self,
        identity: Identity,
        candidate_token: str,
        disclosure_tier: str = DimensionDisclosureTier.CONTROLLED_GROUPED.value,
        confirm: bool = False,
    ) -> OutboundMessage:
        """Admin/steward gate for displaying grouped dimension values."""

        if identity.guild_id and not identity.is_admin:
            return OutboundMessage(
                text="❌ 차원 값 공개 승인은 관리자만 할 수 있습니다."
            )
        candidate_token = candidate_token.strip()
        if not candidate_token:
            return OutboundMessage(text="공개 검토할 candidate_token이 필요합니다.")
        assertion = StewardAssertion(
            scope=identity.kv_scope,
            reviewer_id=identity.user_id,
            authorized=identity.is_admin or not identity.guild_id,
            public_data_confirmed=(
                disclosure_tier == DimensionDisclosureTier.PUBLIC_GROUPED.value
            ),
        )
        if not confirm:
            armed = self._concierge.semantic.arm_dimension_release(
                identity.kv_scope,
                candidate_token,
                disclosure_tier,
                assertion,
            )
            if armed.status != "confirmed":
                return OutboundMessage(
                    text="BLOCKED: " + sanitize_discord_text(armed.message)
                )
            tier_explanation = (
                "최소 그룹 5개 보호를 유지합니다"
                if disclosure_tier == DimensionDisclosureTier.CONTROLLED_GROUPED.value
                else "최소 그룹 보호를 제거하므로 공개·비개인 범주에만 사용합니다"
            )
            return OutboundMessage(
                text=(
                    f"⚠️ 후보 토큰 {_display_token(candidate_token)}을 승인하면 선택한 "
                    "컬럼의 그룹 값이 "
                    f"Discord 결과에 표시될 수 있습니다. `{disclosure_tier}`는 "
                    f"{tier_explanation}. 값 내용과 조직 정책을 확인한 뒤 "
                    "같은 candidate_token·등급으로 `confirm:true`를 다시 호출하세요. "
                    "public_grouped는 먼저 `/semantic_public_data`로 연결 전체를 "
                    "공개 데이터로 확인해야 합니다. 이 승인은 질문 표현의 의미 "
                    "연결과는 별개입니다."
                )
            )
        audit_blocker = self._semantic_audit_blocker()
        if audit_blocker is not None:
            return audit_blocker
        outcome = self._concierge.semantic.release_dimension_with_token(
            identity.kv_scope,
            candidate_token,
            assertion,
            disclosure_tier=disclosure_tier,
            audit_scope=identity.session_key(),
            require_armed_payload=True,
        )
        prefix = "✅ " if outcome.status == "confirmed" else "BLOCKED: "
        return OutboundMessage(text=prefix + sanitize_discord_text(outcome.message))

    async def semantic_public_data(
        self,
        identity: Identity,
        enable: bool = True,
        confirm: bool = False,
        action_token: str = "",
    ) -> OutboundMessage:
        """Confirm or revoke a dataset-wide public/non-personal assertion."""

        if identity.guild_id and not identity.is_admin:
            return OutboundMessage(
                text="❌ 공개 데이터 범위 변경은 관리자만 할 수 있습니다."
            )
        if not confirm:
            action = "확인" if enable else "철회"
            action_kind = "public_data_confirm" if enable else "public_data_revoke"
            action_token = self._concierge.semantic.issue_catalog_action_token(
                identity.kv_scope, action_kind
            )
            if not action_token:
                return OutboundMessage(
                    text="현재 연결의 공개 데이터 범위를 변경할 수 없습니다. `/setup` 또는 연결 상태를 확인해 주세요."
                )
            return OutboundMessage(
                text=(
                    f"⚠️ 현재 연결 전체의 차원 라벨과 지표 값이 공개·비개인 "
                    f"데이터라는 범위를 {action}합니다. action_token: "
                    f"{_display_token(action_token)}. 이 토큰과 `confirm:true`로 다시 "
                    "호출하세요. 토큰은 15분 동안 현재 연결·검토 상태에만 "
                    "유효합니다. 일부 컬럼만 공개라면 활성화하지 마세요."
                )
            )
        audit_blocker = self._semantic_audit_blocker()
        if audit_blocker is not None:
            return audit_blocker
        assertion = StewardAssertion(
            scope=identity.kv_scope,
            reviewer_id=identity.user_id,
            authorized=identity.is_admin or not identity.guild_id,
            public_data_confirmed=enable,
        )
        outcome = self._concierge.semantic.set_public_data_scope_with_token(
            identity.kv_scope,
            action_token,
            assertion,
            enable=enable,
            audit_scope=identity.session_key(),
        )
        prefix = "✅ " if outcome.status == "confirmed" else "BLOCKED: "
        return OutboundMessage(text=prefix + sanitize_discord_text(outcome.message))

    async def semantic_revoke(
        self,
        identity: Identity,
        candidate_token: str,
        confirm: bool = False,
    ) -> OutboundMessage:
        """Revoke an earlier grouped-value disclosure decision."""

        if identity.guild_id and not identity.is_admin:
            return OutboundMessage(
                text="❌ 차원 값 공개 철회는 관리자만 할 수 있습니다."
            )
        candidate_token = candidate_token.strip()
        if not candidate_token:
            return OutboundMessage(text="공개 철회할 candidate_token이 필요합니다.")
        if not confirm:
            return OutboundMessage(
                text=(
                    f"⚠️ 후보 토큰 {_display_token(candidate_token)}으로 선택한 차원의 "
                    "그룹 값 공개를 철회합니다. 실행 중인 결과도 상태 변경을 "
                    "감지하면 폐기됩니다. `confirm:true`로 다시 호출하세요."
                )
            )
        audit_blocker = self._semantic_audit_blocker()
        if audit_blocker is not None:
            return audit_blocker
        outcome = self._concierge.semantic.revoke_dimension_with_token(
            identity.kv_scope,
            candidate_token,
            StewardAssertion(
                scope=identity.kv_scope,
                reviewer_id=identity.user_id,
                authorized=identity.is_admin or not identity.guild_id,
            ),
            audit_scope=identity.session_key(),
        )
        prefix = "✅ " if outcome.status == "confirmed" else "BLOCKED: "
        return OutboundMessage(text=prefix + sanitize_discord_text(outcome.message))

    async def semantic_reset(
        self,
        identity: Identity,
        confirm: bool = False,
        action_token: str = "",
    ) -> OutboundMessage:
        if identity.guild_id and not identity.is_admin:
            return OutboundMessage(
                text="❌ 의미 검토 초기화는 관리자만 할 수 있습니다."
            )
        if not confirm:
            action_token = self._concierge.semantic.issue_catalog_action_token(
                identity.kv_scope, "review_reset"
            )
            if not action_token:
                return OutboundMessage(
                    text="현재 연결의 의미 검토를 초기화할 수 없습니다. `/setup` 또는 연결 상태를 확인해 주세요."
                )
            return OutboundMessage(
                text=(
                    "⚠️ 사람이 확인한 모든 표현·집계 연결, 문자열 차원 공개 "
                    "승인, 연결 전체의 공개 데이터 범위를 초기화합니다. 물리 "
                    "PK/FK와 기본 차단 정책은 유지됩니다. "
                    f"action_token: {_display_token(action_token)}. 실행하려면 이 "
                    "토큰과 `confirm:true`로 다시 호출해 주세요. 토큰은 15분 동안 "
                    "현재 연결·검토 상태에만 유효합니다."
                )
            )
        audit_blocker = self._semantic_audit_blocker()
        if audit_blocker is not None:
            return audit_blocker
        outcome = self._concierge.semantic.reset_reviews_with_token(
            identity.kv_scope,
            action_token,
            StewardAssertion(
                scope=identity.kv_scope,
                reviewer_id=identity.user_id,
                authorized=identity.is_admin or not identity.guild_id,
            ),
            audit_scope=identity.session_key(),
        )
        prefix = "✅ " if outcome.status == "confirmed" else "BLOCKED: "
        return OutboundMessage(text=prefix + sanitize_discord_text(outcome.message))

    async def semantic_review(
        self, identity: Identity, aggregate: str, review_id: str = ""
    ) -> OutboundMessage:
        """Apply one pending metric/dimension decision and resume when complete."""

        if identity.guild_id and not identity.is_admin:
            return OutboundMessage(
                text=(
                    "❌ 길드 전체 의미 연결은 관리자만 승인할 수 있습니다. "
                    "관리자에게 `/semantic_reviews` 대기열 확인을 요청해 주세요."
                )
            )
        audit_blocker = self._semantic_audit_blocker()
        if audit_blocker is not None:
            return audit_blocker
        normalized = aggregate.strip().lower()
        try:
            if review_id.strip():
                outcome = self._concierge.semantic.confirm_pending_by_id(
                    identity.kv_scope,
                    review_id.strip(),
                    normalized,
                    reviewer_id=identity.user_id,
                    authorized=identity.is_admin or not identity.guild_id,
                    audit_scope=identity.session_key(),
                )
            else:
                outcome = self._concierge.semantic.confirm_pending(
                    identity.kv_scope,
                    review_scope_key(identity.session_key(), identity.user_id),
                    normalized,
                    reviewer_id=identity.user_id,
                    audit_scope=identity.session_key(),
                )
        except Exception:
            return OutboundMessage(
                text=(
                    "BLOCKED: 의미 검토와 감사 기록을 원자적으로 저장하지 "
                    "못했습니다. 검토 요청은 유지되므로 잠시 후 다시 시도해 주세요."
                )
            )
        if (
            outcome.status != "confirmed"
            or not outcome.question
            or normalized == "reject"
        ):
            return OutboundMessage(text=sanitize_discord_text(outcome.message))
        if outcome.requester_id != identity.user_id:
            return OutboundMessage(
                text=(
                    "✅ 의미 연결 승인을 저장했습니다. 다른 사용자의 DB 결과는 이 "
                    "채널에서 실행하거나 표시하지 않았습니다. 원 요청자가 질문을 "
                    "다시 보내면 승인된 연결을 사용합니다."
                )
            )
        # Resume the exact reviewed draft directly. Re-running the LLM here
        # would let it pick a different metric after the user approved one.
        ctx = await self._concierge.build_context(identity, user_text=outcome.question)
        if (
            ctx.source_id != outcome.source_id
            or ctx.connection_generation != outcome.connection_generation
        ):
            return OutboundMessage(
                text=(
                    "BLOCKED: DB 연결이 검토 직후 바뀌어 이전 질문을 실행하지 "
                    "않았습니다. 질문을 다시 실행해 주세요."
                )
            )
        ctx.trusted_reviewed_question = outcome.question
        result = await ctx.tools.dispatch(
            "semantic_query",
            outcome.tool_args,
            ctx,
            "cmd:semantic_review:resume",
        )
        prefix = f"✅ {sanitize_discord_text(outcome.message)}"
        if ctx.semantic_result_ready:
            if not self._semantic_result_is_current(identity, ctx):
                self._discard_semantic_result(ctx)
                return OutboundMessage(
                    text=(
                        f"{prefix}\n\nBLOCKED "
                        "(semantic_result_stale_before_render): 결과 표시 직전에 "
                        "DB 연결 또는 의미·공개 상태가 바뀌어 결과를 폐기했습니다. "
                        "질문을 다시 실행해 주세요."
                    )
                )
            return render_answer(
                f"{prefix}\n\nREADY: {ctx.semantic_result_message}",
                ctx.semantic_result_rows,
                header=ctx.semantic_result_headers,
            )
        return OutboundMessage(text=f"{prefix}\n\n{result.content}")

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
        dsn = canonicalize_connection(dsn)
        try:
            # Build the complete candidate before changing the active DSN or
            # catalog. A failed scan leaves the working connection untouched.
            expected_generation = self._concierge.connection_generation(scope)
            if expected_generation < 0:
                raise ValueError("invalid persisted connection generation")
            candidate_source_id = self._concierge.source_identity(scope, dsn, extras)
            active_binding = self._concierge.connection_binding(scope)
            summary = await self._concierge.semantic.inspect(
                scope,
                explorer,
                carry_source_id=(
                    candidate_source_id
                    if active_binding
                    and active_binding.managed_credentials
                    and active_binding.source_id == candidate_source_id
                    else ""
                ),
            )
        except Exception as exc:
            close_explorer(explorer)
            return OutboundMessage(
                text=(
                    f"⚠️ **{_display(db_type)}** 연결 확인 후 의미 준비에 실패했습니다. "
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
                expected_generation=expected_generation,
            )
        except Exception as exc:
            close_explorer(explorer)
            return OutboundMessage(
                text=(
                    "⚠️ 새 연결을 원자적으로 활성화하지 못했습니다. 기존 연결은 "
                    f"변경되지 않았습니다. (오류 유형: {type(exc).__name__})"
                )
            )

        try:
            execution_supported = bool(
                getattr(explorer, "governed_execution_supported", lambda: False)()
            )
        finally:
            close_explorer(explorer)
        execution_line = (
            "- 검증된 statement timeout 실행 지원: 활성"
            if execution_supported
            else "- 메타데이터 연결만 완료: 이 DB 방언의 안전한 statement 취소가 "
            "검증되지 않아 질문 실행은 차단됨"
        )
        enrichment_line = (
            f"- 연결 즉시 Enrich 후보: {summary.enriched_object_count}개 객체 "
            f"(상태: {summary.enrichment_status})"
        )
        if summary.enrichment_reason:
            enrichment_line += f" — 사유: {summary.enrichment_reason}"
        return OutboundMessage(
            text=(
                f"✅ **{_display(db_type)}** 연결 완료 — 테이블 {table_count}개를 읽었습니다.\n"
                f"- 선언된 안전 조인 {summary.declared_join_count}개 자동 등록\n"
                f"- 민감·식별자·비지원 컬럼 {summary.blocked_column_count}개 기본 차단\n"
                f"{enrichment_line}\n"
                f"- 물리 구조 검토 질문 0개\n"
                f"- 관리자 값 공개 검토 대기 차원 "
                f"{len(self._concierge.semantic.release_candidates(scope))}개\n"
                f"{execution_line}\n"
                "- 현재 typed 질의 지원: 집계, 그룹, public 차원의 exact EQ/IN "
                "필터, native DATE의 명시적 `[start,end)` 기간\n"
                "- 계속 확인이 필요한 요청: 상대 기간, OR/NOT, 자유 텍스트 검색, "
                "timezone 미검토 timestamp, 자유 수식\n"
                "업무 지표는 지금 전부 묻지 않습니다. 먼저 `/semantic_status`를 "
                "확인하세요. 공개 검토 대기 차원이 있으면 관리자가 "
                "`/semantic_candidates search:<물리 이름>`에서 토큰을 복사해 "
                "`/semantic_release`의 경고 단계와 확인 단계를 거칩니다. 이후 "
                "질문에 필요한 지표·분류 표현만 독립적으로 확인해 재사용합니다.\n"
                "- Discord 질의 권한: 기본은 관리자만 허용; 일반 구성원에게 "
                "열 채널은 운영자가 `LANG2SQL_DISCORD_QUERY_CHANNEL_IDS`에 명시"
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
        result = await ctx.tools.dispatch_direct("ingest_doc", args, ctx, "cmd:ingest")
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
        result = await ctx.tools.dispatch_direct(
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
DM 또는 허용된 Discord 서버 채널/thread에서 봇을 명시적으로 멘션해 질문하세요.
> @Lang2SQL 이번 달 매출 상위 고객 10명 알려줘

**🗄️ DB 연결** (관리자)
`/setup` — 안내에 따라 DB 접속 정보 입력

Discord에는 credential-bearing DSN을 직접 받는 `/connect`를 노출하지 않습니다.

연결 직후 PK/FK·컬럼 타입·개인정보 의심 컬럼은 자동 정리됩니다.
업무 지표와 분류 표현은 실제 질문에 등장할 때 각각 독립적으로 확인될 수 있습니다.
`/semantic_status` — 현재 준비 상태 확인
`/semantic_candidates search:...` — 문자열 차원 후보 토큰 보기 (관리자)
`/semantic_release candidate_token:... disclosure_tier:... confirm:false` → 같은 값으로 `confirm:true`
`/semantic_dimension_candidates search:...` — 비차단 분류 차원 토큰 보기 (관리자)
`/semantic_dimension_map candidate_token:<mapping_token> phrase:... confirm:false` → 같은 값으로 `confirm:true`
`/semantic_metric_candidates search:...` — 수치 지표 후보 토큰 보기 (관리자)
`/semantic_metric_map candidate_token:... phrase:... confirm:false` → 같은 값으로 `confirm:true`
`/semantic_reviews` → `/semantic_review review_id:... aggregate:...`
`/semantic_public_data enable:true confirm:false` → 발급된 action_token으로 `confirm:true`
`/semantic_candidates state:released search:...`의 `revoke_token`을 복사해
`/semantic_revoke candidate_token:... confirm:false` → 같은 토큰으로 `confirm:true`
`/semantic_reset confirm:false` → 발급된 action_token으로 `confirm:true`

후보·행동 토큰은 15분 동안 현재 연결·검토 상태에만 유효합니다. Discord 서버
질의는 기본적으로 관리자만 가능하며, 운영자가
`LANG2SQL_DISCORD_QUERY_CHANNEL_IDS`에 쉼표로 명시한 상위 채널에서만 일반
구성원에게 열립니다. thread는 상위 채널 정책을 따릅니다. 이 설정은 DB의
row/column 권한 정책을 대신하지 않습니다.

**🔧 기타**
`/ingest`, `/confirm_ingest`, `/term_custom` — 검토형 비즈니스 용어 등록
연결 즉시 의미 준비형 질의에서는 raw-value sampling `/org_setup`, `/enrich` 비활성화
`/remember text:...` — 사실 저장
`/audit_me` — 내 활동 이력 조회
`/help` — 이 도움말"""
        return OutboundMessage(text=text)


def _pending_review_subject(pending: PendingReview) -> tuple[str, str]:
    """Use one subject projection for both queue filtering and rendering."""

    if pending.review_kind == "dimension" and pending.dimension_bindings:
        return (
            pending.dimension_bindings[0].get("dimension_id", ""),
            pending.dimension_bindings[0].get("phrase", ""),
        )
    return pending.metric_id, pending.metric_phrase


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
    return sanitize_discord_text(content, max_length=1500)


def _display(value: object, max_length: int = 160) -> str:
    """Sanitize one untrusted metadata field without changing its stored value."""

    return sanitize_discord_text(value, max_length=max_length)


def _display_token(value: object) -> str:
    """Render only server-created copy-safe tokens without Markdown escaping."""

    raw = str(value)
    if not _ACTION_TOKEN_DISPLAY_RE.fullmatch(raw):
        return "invalid-server-token"
    return raw


def _fmt_ts(ts: float) -> str:
    """Format an epoch timestamp as a short UTC string for audit listings."""
    if not ts:
        return "?"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
