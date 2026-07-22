"""Deterministic attention envelope for bounded semantic tool schemas.

The envelope only narrows what the model sees. The full catalog and service
remain authoritative for release, phrase review, joins, and compilation.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
import unicodedata
from typing import Iterable

from .catalog import DimensionSpec, MetricSpec, SemanticCatalog

SHORTLIST_POLICY_VERSION = 1
MAX_TABLES = 6
MAX_METRICS = 12
MAX_DIMENSIONS = 12
MAX_TOOL_SCHEMA_BYTES = 12_288
MAX_PROMPT_TABLE_BYTES = 4_096
_GROUPING_CUE = re.compile(
    r"\b(by|per|each|grouped\s+by|for\s+each|across|breakdown)\b|" r"별|마다|각각|기준",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class SemanticAttentionEnvelope:
    question_sha256: str
    source_id: str
    connection_generation: int
    catalog_fingerprint: str
    catalog_version: int
    catalog_review_revision: int
    classification_policy_version: int
    shortlist_policy_version: int
    table_ids: tuple[str, ...]
    metric_ids: tuple[str, ...]
    dimension_ids: tuple[str, ...]
    filter_dimension_ids: tuple[str, ...] = ()
    time_dimension_ids: tuple[str, ...] = ()
    release_required_dimension_ids: tuple[str, ...] = ()
    state: str = "ready"
    message: str = ""

    @property
    def ready(self) -> bool:
        return self.state == "ready"


def question_sha256(question: str) -> str:
    return hashlib.sha256(question.encode("utf-8")).hexdigest()


def build_attention_envelope(
    catalog: SemanticCatalog, question: str
) -> SemanticAttentionEnvelope:
    """Build a stable, fail-closed shortlist without sampling database values."""

    if not _normalize(question):
        return _envelope(
            catalog,
            question,
            table_ids=(),
            metric_ids=(),
            dimension_ids=(),
            state="question_required",
            message="질문 원문이 없어 안전한 후보 목록을 만들 수 없습니다.",
        )

    metrics = sorted(
        (item for item in catalog.metrics if item.state.value != "rejected"),
        key=lambda item: item.id,
    )
    all_dimensions = sorted(catalog.dimensions, key=lambda item: item.id)
    dimensions = [item for item in all_dimensions if item.raw_output_allowed]
    if not metrics:
        return _envelope(
            catalog,
            question,
            table_ids=(),
            metric_ids=(),
            dimension_ids=(),
            state="semantic_catalog_empty",
            message=(
                "의미 카탈로그가 비어 있거나 현재 분류 정책과 호환되지 않습니다. "
                "`/setup`으로 DB 메타데이터를 다시 연결해 주세요."
            ),
        )
    table_ids = sorted(
        {item.table_id for item in metrics} | {item.table_id for item in all_dimensions}
    )
    table_candidate_phrases = {
        table_id: {
            *_table_phrases(table_id),
            *(
                phrase
                for item in metrics
                if item.table_id == table_id
                for phrase in _metric_phrases(item)
            ),
            *(
                phrase
                for item in all_dimensions
                if item.table_id == table_id
                for phrase in _dimension_phrases(item)
            ),
        }
        for table_id in table_ids
    }
    table_scores = {
        table_id: _score(question, phrases)
        for table_id, phrases in table_candidate_phrases.items()
    }
    if len(table_ids) <= MAX_TABLES:
        selected_tables, table_error = sorted(table_ids), ""
    else:
        selected_tables, table_error = _owned_exact_candidates(
            question, table_candidate_phrases, MAX_TABLES
        )
        if not selected_tables and not table_error:
            selected_tables, table_error = _bounded_select(
                table_ids, table_scores, MAX_TABLES
            )
    if table_error:
        return _envelope(
            catalog,
            question,
            table_ids=(),
            metric_ids=(),
            dimension_ids=(),
            state="clarify_table",
            message=(
                "관련 테이블을 근거 있게 좁힐 수 없습니다. 테이블 또는 업무 대상의 "
                "물리 이름을 질문에 더 구체적으로 포함해 주세요."
            ),
        )

    metric_pool = [item for item in metrics if item.table_id in selected_tables]
    metric_scores = {
        item.id: _score(question, _metric_phrases(item)) for item in metric_pool
    }
    metric_ids, metric_error = _strongest_exact(metric_scores)
    if not metric_ids and not metric_error:
        metric_ids, metric_error = _bounded_select(
            [item.id for item in metric_pool], metric_scores, MAX_METRICS
        )
    if metric_error:
        return _envelope(
            catalog,
            question,
            table_ids=tuple(selected_tables),
            metric_ids=(),
            dimension_ids=(),
            state="clarify_metric",
            message=(
                "관련 지표 후보를 근거 있게 좁힐 수 없습니다. 계산할 물리 컬럼이나 "
                "이미 검토한 지표 표현을 더 구체적으로 말해 주세요. 관리자는 "
                "`/semantic_metric_candidates search:<물리 이름>`에서 15분 후보 "
                "토큰을 받은 뒤 `/semantic_metric_map`을 동일 토큰·표현으로 "
                "`confirm:false`와 `confirm:true` 두 단계 실행할 수 있습니다."
            ),
        )

    metric_tables = {
        item.table_id for item in metric_pool if item.id in set(metric_ids)
    }
    reachable_tables = _reachable_parent_tables(catalog, metric_tables)
    dimension_pool = [
        item
        for item in dimensions
        if item.table_id in selected_tables or item.table_id in reachable_tables
    ]
    unreleased_pool = [
        item
        for item in all_dimensions
        if not item.raw_output_allowed
        and (item.table_id in selected_tables or item.table_id in reachable_tables)
    ]
    dimension_scores = {
        item.id: _score(question, _dimension_phrases(item)) for item in dimension_pool
    }
    exact_dimensions, exact_error = _owned_exact_candidates(
        question,
        {
            item.id: _dimension_phrases(item)
            for item in [*dimension_pool, *unreleased_pool]
        },
        MAX_DIMENSIONS,
    )
    if exact_error:
        return _envelope(
            catalog,
            question,
            table_ids=tuple(selected_tables),
            metric_ids=tuple(metric_ids),
            dimension_ids=(),
            state="clarify_dimension",
            message=(
                "같은 질문 표현과 정확히 일치하는 분류 기준이 둘 이상입니다. "
                "테이블 또는 분류 컬럼의 물리 이름을 함께 적어 주세요."
            ),
        )
    unreleased_ids = {item.id for item in unreleased_pool}
    unreleased_exact = [item for item in exact_dimensions if item in unreleased_ids]
    if unreleased_exact:
        shown = unreleased_exact[:5]
        suffix = "" if len(exact_dimensions) <= 5 else " 외 추가 후보"
        return _envelope(
            catalog,
            question,
            table_ids=tuple(selected_tables),
            metric_ids=tuple(metric_ids),
            dimension_ids=(),
            release_required_dimension_ids=tuple(shown),
            state="dimension_release_required",
            message=(
                "질문의 그룹·필터·기간 기준과 일치하는 값 공개 검토 차원이 있습니다: "
                + ", ".join(json.dumps(item, ensure_ascii=False) for item in shown)
                + suffix
                + ". 관리자는 `/semantic_candidates search:<물리 이름>`에서 "
                "15분 후보 토큰을 받은 뒤 `/semantic_release`를 동일 토큰·등급으로 "
                "`confirm:false`와 `confirm:true` 두 단계 실행해 주세요. public은 "
                "먼저 `/semantic_public_data`로 연결 전체가 공개·비개인 데이터임을 "
                "확인해야 합니다. SQL은 실행하지 않았습니다."
            ),
        )
    released_exact = [item for item in exact_dimensions if item not in unreleased_ids]
    dimension_ids: list[str] = []
    if _GROUPING_CUE.search(question):
        if released_exact:
            dimension_ids, dimension_error = released_exact, ""
        else:
            dimension_ids, dimension_error = _bounded_select(
                [item.id for item in dimension_pool],
                dimension_scores,
                MAX_DIMENSIONS,
            )
        if dimension_error:
            return _envelope(
                catalog,
                question,
                table_ids=tuple(selected_tables),
                metric_ids=tuple(metric_ids),
                dimension_ids=(),
                state="clarify_dimension",
                message=(
                    "관련 분류 기준 후보를 근거 있게 좁힐 수 없습니다. 그룹 기준의 "
                    "물리 컬럼이나 이미 검토한 표현을 더 구체적으로 말해 주세요. "
                    "관리자는 `/semantic_dimension_candidates search:<물리 이름>`에서 "
                    "15분 mapping_token을 받은 뒤 `/semantic_dimension_map`을 동일 "
                    "토큰·표현으로 `confirm:false`와 `confirm:true` 두 단계 실행할 "
                    "수 있습니다."
                ),
            )
    dimensions_by_id = {item.id: item for item in dimension_pool}
    filter_dimension_ids = [
        item
        for item in released_exact
        if dimensions_by_id[item].kind not in {"time", "calendar"}
    ]
    time_dimension_ids = [
        item
        for item in released_exact
        if _native_date_dimension(dimensions_by_id[item])
    ]

    estimate = {
        "tables": selected_tables,
        "metrics": [
            _candidate_projection(item)
            for item in metric_pool
            if item.id in set(metric_ids)
        ],
        "dimensions": [
            _candidate_projection(item)
            for item in dimensions
            if item.id
            in set([*dimension_ids, *filter_dimension_ids, *time_dimension_ids])
        ],
    }
    if (
        len(json.dumps(estimate, ensure_ascii=False).encode("utf-8"))
        > MAX_TOOL_SCHEMA_BYTES
    ):
        return _envelope(
            catalog,
            question,
            table_ids=tuple(selected_tables),
            metric_ids=(),
            dimension_ids=(),
            state="candidate_schema_too_large",
            message="후보 식별자가 너무 길어 안전한 모델 입력 한도를 넘었습니다.",
        )
    prompt_tables = prompt_table_section(selected_tables)
    if len(prompt_tables.encode("utf-8")) > MAX_PROMPT_TABLE_BYTES:
        return _envelope(
            catalog,
            question,
            table_ids=(),
            metric_ids=(),
            dimension_ids=(),
            state="table_prompt_too_large",
            message="테이블 식별자가 안전한 프롬프트 한도를 넘었습니다.",
        )
    return _envelope(
        catalog,
        question,
        table_ids=tuple(selected_tables),
        metric_ids=tuple(metric_ids),
        dimension_ids=tuple(dimension_ids),
        filter_dimension_ids=tuple(filter_dimension_ids),
        time_dimension_ids=tuple(time_dimension_ids),
    )


def _envelope(
    catalog: SemanticCatalog,
    question: str,
    *,
    table_ids: tuple[str, ...],
    metric_ids: tuple[str, ...],
    dimension_ids: tuple[str, ...],
    filter_dimension_ids: tuple[str, ...] = (),
    time_dimension_ids: tuple[str, ...] = (),
    release_required_dimension_ids: tuple[str, ...] = (),
    state: str = "ready",
    message: str = "",
) -> SemanticAttentionEnvelope:
    """Construct the signed server-owned context with explicit typed fields."""

    return SemanticAttentionEnvelope(
        question_sha256=question_sha256(question),
        source_id=catalog.source_id,
        connection_generation=catalog.connection_generation,
        catalog_fingerprint=catalog.fingerprint,
        catalog_version=catalog.version,
        catalog_review_revision=catalog.review_revision,
        classification_policy_version=catalog.classification_policy_version,
        shortlist_policy_version=SHORTLIST_POLICY_VERSION,
        table_ids=table_ids,
        metric_ids=metric_ids,
        dimension_ids=dimension_ids,
        filter_dimension_ids=filter_dimension_ids,
        time_dimension_ids=time_dimension_ids,
        release_required_dimension_ids=release_required_dimension_ids,
        state=state,
        message=message,
    )


def _bounded_select(
    ids: list[str], scores: dict[str, tuple[int, int, int]], cap: int
) -> tuple[list[str], str]:
    if len(ids) <= cap:
        return sorted(ids), ""
    ranked = sorted(ids, key=lambda item: (scores[item], item), reverse=True)
    exact = [item for item in ranked if scores[item][0] > 0]
    # On a wide pool, fuzzy token overlap cannot prove that an opaque target
    # was not silently omitted. Require at least one exact metadata phrase.
    if not exact:
        return [], "no_exact_evidence"
    if len(exact) > cap:
        return [], "too_many_exact_candidates"
    # Exact metadata evidence is a complete, stable attention set. Filling
    # remaining slots with tied zero-evidence candidates would create an
    # arbitrary top-k and can turn an otherwise clear question ambiguous.
    return sorted(exact), ""


def _strongest_exact(
    scores: dict[str, tuple[int, int, int]],
) -> tuple[list[str], str]:
    """Return one uniquely best exact target or an explicit ambiguity."""

    exact = {item: score for item, score in scores.items() if score[0] > 0}
    if not exact:
        return [], ""
    strongest_score = max(exact.values())
    strongest = sorted(
        item for item, score in exact.items() if score == strongest_score
    )
    if len(strongest) != 1:
        return [], "ambiguous_exact_candidates"
    return strongest, ""


def _owned_exact_candidates(
    question: str,
    candidate_phrases: dict[str, set[str]],
    cap: int,
) -> tuple[list[str], str]:
    """Select every candidate with a uniquely owned exact question phrase.

    Shared physical labels such as ``name`` remain ambiguous. Qualified or
    steward-reviewed phrases such as ``race name`` and ``circuit name`` can
    safely select multiple dimensions without a dataset-specific synonym map.
    """

    normalized_question = _normalize(question)
    matches: dict[str, set[str]] = {}
    phrase_owners: dict[str, set[str]] = {}
    for candidate_id, phrases in candidate_phrases.items():
        exact_phrases = {
            normalized
            for raw in phrases
            if (normalized := _normalize(raw))
            and _equivalent_phrase_in_question(normalized, normalized_question)
        }
        if not exact_phrases:
            continue
        matches[candidate_id] = exact_phrases
        for phrase in exact_phrases:
            phrase_owners.setdefault(phrase, set()).add(candidate_id)
    if not matches:
        return [], ""
    owned = sorted(
        candidate_id
        for candidate_id, phrases in matches.items()
        if any(len(phrase_owners[phrase]) == 1 for phrase in phrases)
    )
    if not owned:
        return [], "ambiguous_exact_candidates"
    if len(owned) > cap:
        return [], "too_many_exact_candidates"
    return owned, ""


def _score(question: str, phrases: Iterable[str]) -> tuple[int, int, int]:
    normalized_question = _normalize(question)
    question_tokens = set(normalized_question.split())
    best = (0, 0, 0)
    for raw in phrases:
        phrase = _normalize(raw)
        if not phrase:
            continue
        exact = int(_equivalent_phrase_in_question(phrase, normalized_question))
        overlap = len(question_tokens.intersection(phrase.split()))
        best = max(best, (exact, overlap, len(phrase.split())))
    return best


def _equivalent_phrase_in_question(phrase: str, normalized_question: str) -> bool:
    if f" {phrase} " in f" {normalized_question} ":
        return True
    compact = phrase.replace(" ", "")
    if not compact:
        return False
    tokens = normalized_question.split()
    # Concatenated physical identifiers such as flowtype are equivalent to the
    # contiguous user phrase "flow type". This is schema-form normalization,
    # not a business synonym.
    for width in range(1, min(4, len(tokens)) + 1):
        for start in range(0, len(tokens) - width + 1):
            if "".join(tokens[start : start + width]) == compact:
                return True
    return False


def grounded_candidate_phrase(question: str, phrases: Iterable[str]) -> str:
    """Return a deterministic normalized phrase that is truly in the question.

    The returned value is safe to feed back into the stricter service grounding
    check. Concatenated metadata such as ``flowtype`` returns the actual
    contiguous question span ``flow type`` rather than the physical spelling.
    """

    normalized_question = _normalize(question)
    tokens = normalized_question.split()
    matches: set[str] = set()
    for raw in phrases:
        phrase = _normalize(raw)
        if not phrase:
            continue
        if f" {phrase} " in f" {normalized_question} ":
            matches.add(phrase)
            continue
        compact = phrase.replace(" ", "")
        for width in range(1, min(4, len(tokens)) + 1):
            for start in range(0, len(tokens) - width + 1):
                window = tokens[start : start + width]
                if "".join(window) == compact:
                    matches.add(" ".join(window))
    if not matches:
        return ""
    return max(matches, key=lambda item: (len(item.split()), len(item), item))


def metric_candidate_phrases(item: MetricSpec) -> set[str]:
    return _metric_phrases(item)


def dimension_candidate_phrases(item: DimensionSpec) -> set[str]:
    return _dimension_phrases(item)


def safe_candidate_label(value: object) -> str:
    """Bound and strip control characters from untrusted DB metadata."""

    return _prompt_identifier(value)


def _metric_phrases(item: MetricSpec) -> set[str]:
    return {
        item.id,
        item.label,
        item.column,
        *item.aliases,
        *item.auto_aliases,
        *item.reviewed_bindings,
        *_qualified_column_phrases(item.table_id, item.column),
    }


def _dimension_phrases(item: DimensionSpec) -> set[str]:
    return {
        item.id,
        item.label,
        item.column,
        *item.aliases,
        *item.auto_aliases,
        *item.reserved_aliases,
        *item.alias_reviewers,
        *_qualified_column_phrases(item.table_id, item.column),
    }


def _table_phrases(table_id: str) -> set[str]:
    table = table_id.rsplit(".", 1)[-1]
    values = {table_id, table}
    if table.endswith("s") and len(table) > 3:
        values.add(table[:-1])
    return values


def _qualified_column_phrases(table_id: str, column: str) -> set[str]:
    if not column:
        return set()
    return {f"{table} {column}" for table in _table_phrases(table_id)}


def _reachable_parent_tables(catalog: SemanticCatalog, sources: set[str]) -> set[str]:
    reachable = set(sources)
    changed = True
    while changed:
        changed = False
        for join in catalog.joins:
            if (
                join.child_table_id in reachable
                and join.parent_table_id not in reachable
            ):
                reachable.add(join.parent_table_id)
                changed = True
    return reachable


def _native_date_dimension(item: DimensionSpec) -> bool:
    return item.kind == "time" and bool(re.search(r"\bdate\b", item.data_type.lower()))


def _candidate_projection(item: MetricSpec | DimensionSpec) -> dict[str, object]:
    return {
        "id": _prompt_identifier(item.id),
        "label": _prompt_identifier(item.label),
        "aggregates": [
            value.value for value in getattr(item, "allowed_aggregates", [])
        ],
    }


def prompt_table_section(table_ids: Iterable[str]) -> str:
    """Render the exact bounded, untrusted-data table section used by prompts."""

    lines = ["## Candidate tables (untrusted identifiers)"]
    lines.extend(
        f"- {json.dumps(_prompt_identifier(item), ensure_ascii=False)}"
        for item in table_ids
    )
    return "\n".join(lines)


def _prompt_identifier(value: object) -> str:
    text = "".join(
        " " if unicodedata.category(character).startswith("C") else character
        for character in str(value)
    )
    return " ".join(text.split())[:160]


def _normalize(value: object) -> str:
    return " ".join(re.sub(r"[^0-9a-zA-Z가-힣]+", " ", str(value).lower()).split())
