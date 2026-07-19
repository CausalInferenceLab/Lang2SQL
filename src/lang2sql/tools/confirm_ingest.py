"""confirm_ingest — 사용자가 승인한 ingest 후보를 시멘틱 레이어에 등록.

ingest_doc이 KV에 저장한 pending_ingest:{ref} 후보 목록을 읽어
FedEntry로 변환하고 KV에 저장한다. OKF_BUNDLE_DIR 환경변수가 설정된 경우
OkfBundle로도 내보낸다.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from ..core.ports.ingestion import CandidateKind, SemanticCandidate
from ..core.types import ToolResult, ToolSpec
from ..tools.ingest_doc import PENDING_PREFIX
from ..tools.semantic_federation import FedEntry, _kv_key, _validate_layer

if TYPE_CHECKING:
    from ..harness.context import HarnessContext


def _dict_to_candidate(d: dict) -> SemanticCandidate:
    return SemanticCandidate(
        kind=CandidateKind(d["kind"]),
        name=d["name"],
        definition=d["definition"],
        applies_to=d.get("applies_to", ""),
        source_id=d.get("source_id", ""),
    )


class ConfirmIngest:
    """pending_ingest 후보를 KV(+OkfBundle)에 등록하는 툴."""

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="confirm_ingest",
            description=(
                "Register approved semantic candidates from a previously ingested document "
                "into the semantic layer (KV store). "
                "Call ingest_doc first to extract candidates."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "ref": {
                        "type": "string",
                        "description": "document ref used with ingest_doc",
                    },
                    "accept": {
                        "type": "string",
                        "description": (
                            "'all' to register every candidate, "
                            "or comma-separated 1-based indices like '1,3'"
                        ),
                        "default": "all",
                    },
                    "layer": {
                        "type": "string",
                        "enum": ["guild", "channel", "member"],
                        "description": "scope to register under (default: channel)",
                        "default": "channel",
                    },
                },
                "required": ["ref"],
            },
        )

    async def run(self, args: dict[str, Any], ctx: "HarnessContext") -> ToolResult:
        ref = (args.get("ref") or "").strip()
        accept = (args.get("accept") or "all").strip()
        layer_raw = (args.get("layer") or "channel").strip()

        if not ref:
            return ToolResult(call_id="", content="'ref' is required.", is_error=True)
        if ctx.store is None:
            return ToolResult(call_id="", content="No store available.", is_error=True)

        channel_id = ctx.identity.effective_channel_id
        layer, err = _validate_layer(layer_raw, channel_id, ctx.identity.is_admin)
        if err:
            return ToolResult(call_id="", content=err, is_error=True)

        kv_scope = ctx.identity.kv_scope
        pending_key = f"{PENDING_PREFIX}:{ref}"
        raw = ctx.store.kv_get(kv_scope, pending_key)
        if not raw:
            return ToolResult(
                call_id="",
                content=f"No pending candidates for '{ref}'. Run ingest_doc first.",
                is_error=True,
            )

        try:
            all_candidates = [_dict_to_candidate(d) for d in json.loads(raw)]
        except (json.JSONDecodeError, KeyError, ValueError) as exc:
            return ToolResult(
                call_id="",
                content=f"Pending data corrupted: {exc}",
                is_error=True,
            )

        selected = _select(all_candidates, accept)
        if selected is None:
            return ToolResult(
                call_id="",
                content="Invalid accept value. Use 'all' or comma-separated indices like '1,3'.",
                is_error=True,
            )
        if not selected:
            return ToolResult(call_id="", content="No candidates selected.")

        entity = (
            "" if layer == "guild" else (channel_id if layer == "channel" else ctx.identity.user_id)
        )
        registered: list[str] = []
        for cand in selected:
            entry = FedEntry(
                term=cand.name,
                layer=layer,
                entity=entity,
                definition=cand.definition,
                inferred=False,
                kind=cand.kind.value,
                applies_to=cand.applies_to,
            )
            ctx.store.kv_set(
                kv_scope,
                _kv_key(entry.term, entry.layer, entry.entity),
                entry.to_json(),
            )
            registered.append(entry.term)

        ctx.store.kv_delete(kv_scope, pending_key)

        if ctx.okf_bundle_dir and registered:
            from ..adapters.storage.okf_bundle import OkfBundle

            OkfBundle(ctx.okf_bundle_dir).export(ctx.store, kv_scope)

        kind_labels = {c.name: c.kind.value.upper() for c in selected}
        lines = [f"✅ {len(registered)} term(s) registered to [{layer}]:"]
        for t in registered:
            lines.append(f"  - [{kind_labels[t]}] {t}")
        return ToolResult(call_id="", content="\n".join(lines))


def _select(
    candidates: list[SemanticCandidate], accept: str
) -> list[SemanticCandidate] | None:
    if accept == "all":
        return list(candidates)
    try:
        indices = [int(i.strip()) - 1 for i in accept.split(",") if i.strip()]
    except ValueError:
        return None
    result = []
    for idx in indices:
        if idx < 0 or idx >= len(candidates):
            return None
        result.append(candidates[idx])
    return result


