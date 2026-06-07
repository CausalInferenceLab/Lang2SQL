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

from datetime import datetime, timezone

from ...adapters.db import build_explorer
from ...adapters.db.dsn_builder import assemble
from ...core.identity import Identity
from ...core.ports.frontend import OutboundMessage
from ...core.types import Role
from ...harness.loop import agent_loop
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
        for msg in current_turn:
            if msg.role != Role.TOOL or msg.name != "run_sql" or not msg.content:
                continue
            sql = call_id_to_sql.get(msg.tool_call_id or "")
            if sql and ("row(s):" in msg.content or "(0 rows)" in msg.content):
                sql_queries.append(sql)
                sql_results.append(msg.content)

        ctx.session.compress()
        await self._concierge.store.save(identity.session_key(), ctx.session)

        suffix = ""
        if sql_queries:
            suffix += "\n\n**SQL:**\n```sql\n" + "\n\n".join(sql_queries) + "\n```"
        if sql_results:
            suffix += "\n\n**결과:**\n```\n" + "\n\n".join(sql_results) + "\n```"
        return render_answer(answer + suffix)

    async def define_metric(
        self,
        identity: Identity,
        name: str,
        definition: str,
        scope: str | None = None,
    ) -> OutboundMessage:
        """Register one definition at the current (or requested) scope.

        Delegates to the ctx-aware ``define_metric`` tool, which writes through
        the :class:`ScopeResolverPort` and records an audit event. ``scope`` is
        ``"channel"`` (default) or ``"guild"`` per the tool's schema.
        """
        ctx = await self._concierge.build_context(identity)
        args: dict[str, str] = {"name": name, "definition": definition}
        if scope:
            args["scope"] = scope
        result = await ctx.tools.dispatch("define_metric", args, ctx, "cmd:define_metric")
        return OutboundMessage(text=result.content)

    async def remember(self, identity: Identity, text: str) -> OutboundMessage:
        """Persist a user fact via the memory service (manual ``/remember``)."""
        ctx = await self._concierge.build_context(identity)
        result = await ctx.tools.dispatch("remember", {"text": text}, ctx, "cmd:remember")
        return OutboundMessage(text=result.content)

    async def semantic_show(self, identity: Identity) -> OutboundMessage:
        """Show the effective semantic layer for this scope chain."""
        ctx = await self._concierge.build_context(identity)
        if ctx.scope_resolver is None:
            return OutboundMessage(text="Semantic layer unavailable.")
        layer = await ctx.scope_resolver.effective_layer(identity)
        rendered = layer.render()
        if not rendered:
            return OutboundMessage(text="No definitions apply in this scope yet.")
        return OutboundMessage(text=f"Definitions in effect here:\n{rendered}")

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
        try:
            spec = assemble(db_type, fields)
        except ValueError as exc:
            return OutboundMessage(text=f"⚠️ Setup error: {exc}")

        try:
            explorer = build_explorer(spec.dsn, extras=spec.extras)
            tables = await explorer.list_tables()
        except ModuleNotFoundError as exc:
            return OutboundMessage(
                text=(
                    f"⚠️ Connection driver not installed for {db_type}. "
                    f"Ask an admin to run `uv sync --extra {db_type}`.\n"
                    f"(details: {exc})"
                )
            )
        except Exception as exc:  # surface what the DB said, but stay user-friendly
            return OutboundMessage(
                text=(
                    f"❌ Couldn't connect to {db_type}: {type(exc).__name__}: {exc}.\n"
                    "Common causes: wrong host/port, network/firewall, "
                    "wrong credentials, or read permission missing."
                )
            )

        scope = identity.guild_id or f"dm:{identity.user_id}"
        await self._concierge.secrets.set(scope, "db_dsn", spec.dsn)
        for k, v in spec.extras.items():
            await self._concierge.secrets.set(scope, f"db_extras.{k}", v)
        # Bust any cached explorer for this scope so the next turn picks it up.
        self._concierge.forget_explorer(scope)

        return OutboundMessage(
            text=(
                f"✅ Connected to **{db_type}** — found **{len(tables)} table(s)**. "
                "Your credentials are stored encrypted; you can `/semantic_show` "
                "or just ask a question now."
            )
        )

    async def enrich(self, identity: Identity, table: str = "", clear: bool = False) -> OutboundMessage:
        """Run EnrichSchema tool: sample DB columns and LLM-infer descriptions."""
        ctx = await self._concierge.build_context(identity)
        result = await ctx.tools.dispatch(
            "enrich_schema", {"table": table, "clear": clear}, ctx, "cmd:enrich"
        )
        return OutboundMessage(text=result.content)

    async def org_setup(
        self, identity: Identity, org: str = "", team: str = "", clear: bool = False
    ) -> OutboundMessage:
        """조직(전사) 또는 팀(채널) 등록 + DB 스캔으로 비즈니스 용어 자동 추출."""
        ctx = await self._concierge.build_context(identity)
        result = await ctx.tools.dispatch(
            "org_setup", {"org": org, "team": team, "clear": clear}, ctx, "cmd:org_setup"
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
                "term": term, "definition": definition, "layer": layer,
                "synonyms": synonyms, "inferred": inferred, "scan": scan,
                "remove": remove, "list": list_all,
            },
            ctx,
            "cmd:term_custom",
        )
        return OutboundMessage(text=result.content)

    async def connect(self, identity: Identity, dsn: str) -> OutboundMessage:
        """V1 stub: stash a DB DSN keyed by guild/DM in the concierge kv store.

        There is no secrets *port* on :class:`HarnessContext` in V1, so this
        does not yet encrypt or wire the DSN into the explorer — it simply
        records it (so a later run can pick it up) and acknowledges. The real
        encrypted-secrets path lands in the tenancy work (task #2); this keeps
        the command present and documented without overreaching.
        """
        dsn = dsn.strip()
        if not dsn:
            return OutboundMessage(text="Provide a database connection string.")
        scope = identity.guild_id or f"dm:{identity.user_id}"
        self._concierge.store.kv_set(scope, "dsn", dsn)
        return OutboundMessage(
            text=(
                "Connection string saved for this workspace (V1 stub — not yet "
                "encrypted or live; queries still run against the configured DB)."
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


def _fmt_ts(ts: float) -> str:
    """Format an epoch timestamp as a short UTC string for audit listings."""
    if not ts:
        return "?"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
