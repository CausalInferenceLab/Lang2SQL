"""Shared governed execution kernel for tools and the public library facade."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..core.ports.audit import AuditEvent, AuditPort
from ..core.ports.explorer import (
    ExplorerPort,
    QueryTimedOutError,
    QueryTimeoutUnsupportedError,
    accepts_bound_parameters,
    accepts_statement_timeout,
)
from ..core.ports.safety import SafetyContext, SafetyPipelinePort, Verdict
from .service import (
    QueryOutcome,
    SemanticService,
    decode_semantic_query_rows,
    enforce_metric_disclosure_output,
    enforce_released_dimension_output,
    semantic_query_headers,
)


@dataclass(frozen=True)
class GovernedExecutionResult:
    status: str
    code: str
    message: str
    headers: tuple[str, ...] = ()
    rows: tuple[tuple[Any, ...], ...] = ()
    stamp: tuple[str, int, str, int, int, int] | tuple[()] = ()

    @property
    def ready(self) -> bool:
        return self.status == "ready"


def _catalog_matches_outcome(catalog: object, outcome: QueryOutcome) -> bool:
    return bool(
        catalog is not None
        and getattr(catalog, "source_id", None) == outcome.source_id
        and getattr(catalog, "connection_generation", None)
        == outcome.connection_generation
        and getattr(catalog, "fingerprint", None) == outcome.catalog_fingerprint
        and getattr(catalog, "review_revision", None) == outcome.catalog_review_revision
        and getattr(catalog, "version", None) == outcome.catalog_version
        and getattr(catalog, "classification_policy_version", None)
        == outcome.classification_policy_version
    )


async def execute_governed_semantic(
    *,
    service: SemanticService,
    scope: str,
    explorer: ExplorerPort,
    safety: SafetyPipelinePort,
    outcome: QueryOutcome,
    actor: str,
    audit_scope: str,
    audit: AuditPort | None,
    row_limit: int,
) -> GovernedExecutionResult:
    """Execute exactly one prepared semantic plan and recheck every state gate."""

    if (
        outcome.status != "ready"
        or outcome.plan is None
        or outcome.prepared is None
        or outcome.prepared.plan_hash != outcome.plan.plan_hash
    ):
        return GovernedExecutionResult(
            "blocked",
            "semantic_plan_invalid",
            "검토된 의미 계획과 실행 템플릿의 결합을 확인하지 못했습니다.",
        )

    bounded_limit = max(1, min(int(row_limit), 1000))
    safety_context = SafetyContext(row_limit=bounded_limit)
    decision = safety.evaluate(outcome.prepared.sql, safety_context)
    if decision.verdict != Verdict.PASS:
        return GovernedExecutionResult(
            "blocked",
            "safety_blocked",
            f"{decision.layer}: {decision.reason}",
        )
    if not _catalog_matches_outcome(service.load(scope), outcome):
        return GovernedExecutionResult(
            "blocked",
            "connection_stale_pre_execute",
            "DB 연결 또는 의미 검토 상태가 실행 직전에 바뀌었습니다.",
        )
    if not accepts_statement_timeout(explorer):
        return GovernedExecutionResult(
            "blocked",
            "query_timeout_unsupported",
            "DB adapter가 검증된 statement timeout 계약을 구현하지 않았습니다.",
        )
    try:
        parameters = outcome.prepared.parameter_mapping()
    except ValueError:
        return GovernedExecutionResult(
            "blocked",
            "semantic_parameters_invalid",
            "검토된 typed 값을 실행 파라미터로 변환하지 못했습니다.",
        )
    if parameters and not accepts_bound_parameters(explorer):
        return GovernedExecutionResult(
            "blocked",
            "bound_parameters_unsupported",
            "DB adapter가 값과 SQL을 분리하는 실행 계약을 구현하지 않았습니다.",
        )

    try:
        if parameters:
            rows = await explorer.execute(
                decision.sql,
                bounded_limit,
                timeout_seconds=safety_context.timeout_seconds,
                parameters=parameters,
            )
        else:
            # Compatibility is safe only for plans that contain no values.
            rows = await explorer.execute(
                decision.sql,
                bounded_limit,
                timeout_seconds=safety_context.timeout_seconds,
            )
    except QueryTimedOutError:
        return GovernedExecutionResult(
            "blocked",
            "query_timeout",
            "검토된 질의가 실행 제한 시간을 넘었습니다.",
        )
    except QueryTimeoutUnsupportedError:
        return GovernedExecutionResult(
            "blocked",
            "query_timeout_unsupported",
            "연결된 DB에서 안전한 statement 취소를 검증하지 못했습니다.",
        )
    except Exception as exc:
        if audit is not None:
            recorded = await _record_audit(
                audit,
                AuditEvent(
                    actor=actor,
                    action="semantic_query_failed",
                    scope=audit_scope,
                    detail={
                        "metric_id": outcome.metric_id,
                        "aggregate": outcome.aggregate,
                        "dimension_ids": outcome.dimension_ids,
                        "sql": decision.sql,
                        "plan_hash": outcome.prepared.plan_hash,
                        "parameter_kinds": outcome.prepared.audit_detail()[
                            "parameter_kinds"
                        ],
                        "error_type": type(exc).__name__,
                    },
                ),
            )
            if not recorded:
                return _audit_write_failed()
        return GovernedExecutionResult(
            "blocked",
            "query_execution_failed",
            "DB가 검토된 질의를 실행하지 못했습니다. 상세는 audit에만 남깁니다.",
        )

    current_catalog = service.load(scope)
    if not _catalog_matches_outcome(current_catalog, outcome):
        return GovernedExecutionResult(
            "blocked",
            "semantic_catalog_changed",
            "실행 중 DB 또는 의미·공개 검토 상태가 바뀌어 결과를 폐기했습니다.",
        )
    assert current_catalog is not None
    rows, metric_blocker = enforce_metric_disclosure_output(
        current_catalog,
        outcome.metric_id,
        outcome.aggregate,
        outcome.dimension_ids,
        rows,
    )
    if metric_blocker:
        if not await _audit_output_block(
            audit,
            actor,
            audit_scope,
            outcome,
            metric_blocker,
        ):
            return _audit_write_failed()
        return GovernedExecutionResult(
            "blocked",
            metric_blocker,
            "비공개 지표 집계의 최소 기여 행 수 정책을 통과하지 못했습니다.",
        )
    rows, release_blocker = enforce_released_dimension_output(
        current_catalog, outcome.dimension_ids, rows
    )
    if release_blocker:
        if not await _audit_output_block(
            audit,
            actor,
            audit_scope,
            outcome,
            release_blocker,
        ):
            return _audit_write_failed()
        return GovernedExecutionResult(
            "blocked",
            release_blocker,
            "공개 차원 결과가 그룹 크기·범주 수·표시 길이 정책을 통과하지 못했습니다.",
        )
    rows, layout_blocker = decode_semantic_query_rows(
        current_catalog, outcome.dimension_ids, rows
    )
    if layout_blocker:
        return GovernedExecutionResult(
            "blocked",
            layout_blocker,
            "실행 결과 열 구성이 semantic output 계약과 일치하지 않습니다.",
        )
    if audit is not None:
        recorded = await _record_audit(
            audit,
            AuditEvent(
                actor=actor,
                action="semantic_query",
                scope=audit_scope,
                detail={
                    "metric_id": outcome.metric_id,
                    "aggregate": outcome.aggregate,
                    "dimension_ids": outcome.dimension_ids,
                    "sql": decision.sql,
                    "plan_hash": outcome.prepared.plan_hash,
                    "parameter_kinds": outcome.prepared.audit_detail()[
                        "parameter_kinds"
                    ],
                },
            ),
        )
        if not recorded:
            return _audit_write_failed()
    publish_catalog = service.load(scope)
    if not _catalog_matches_outcome(publish_catalog, outcome):
        return GovernedExecutionResult(
            "blocked",
            "semantic_catalog_changed_before_publish",
            "audit 또는 게시 직전에 의미·공개 상태가 바뀌어 결과를 폐기했습니다.",
        )
    assert publish_catalog is not None
    headers = semantic_query_headers(publish_catalog, outcome.dimension_ids)
    rendered_rows = tuple(tuple(row[header] for header in headers) for row in rows)
    stamp = (
        publish_catalog.source_id,
        publish_catalog.connection_generation,
        publish_catalog.fingerprint,
        publish_catalog.review_revision,
        publish_catalog.version,
        publish_catalog.classification_policy_version,
    )
    return GovernedExecutionResult(
        "ready",
        "ready",
        outcome.message,
        headers=headers,
        rows=rendered_rows,
        stamp=stamp,
    )


async def _audit_output_block(
    audit: AuditPort | None,
    actor: str,
    audit_scope: str,
    outcome: QueryOutcome,
    reason: str,
) -> bool:
    if audit is None:
        return True
    return await _record_audit(
        audit,
        AuditEvent(
            actor=actor,
            action="semantic_query_output_blocked",
            scope=audit_scope,
            detail={
                "metric_id": outcome.metric_id,
                "dimension_ids": outcome.dimension_ids,
                "reason": reason,
            },
        ),
    )


async def _record_audit(audit: AuditPort, event: AuditEvent) -> bool:
    """Return false instead of leaking audit-adapter failures or result rows."""

    try:
        await audit.record(event)
    except Exception:
        return False
    return True


def _audit_write_failed() -> GovernedExecutionResult:
    return GovernedExecutionResult(
        "blocked",
        "audit_write_failed",
        "실행 결과의 audit 기록을 저장하지 못해 결과를 게시하지 않았습니다.",
    )
