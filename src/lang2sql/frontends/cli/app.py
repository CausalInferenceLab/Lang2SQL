"""CLI frontend — the developer-facing driver of the harness.

Per v4.1 the CLI is a dev tool (Discord is the Phase 1 product frontend). It
assembles a real :class:`HarnessContext` via the :class:`ContextConcierge`
(Codex CLI authentication; no automatic offline fallback) and runs one
turn through ``agent_loop`` — the harness's end-to-end smoke test.

Run:  python -m lang2sql.frontends.cli.app "your question"
"""

from __future__ import annotations

import asyncio
import sys

from ...core.identity import Identity
from ...harness.loop import agent_loop
from ...tenancy.concierge import ContextConcierge


async def _run(user_text: str) -> str:
    concierge = ContextConcierge()
    identity = Identity(user_id="cli-user", guild_id="cli", channel_id="cli-main")
    ctx = await concierge.build_context(identity)
    return await agent_loop(ctx, user_text)


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    user_text = " ".join(argv) or "list the tables"
    print(asyncio.run(_run(user_text)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
