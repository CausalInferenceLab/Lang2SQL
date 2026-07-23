"""PII-safe, evidence-first semantic onboarding.

Only catalog facts are accepted automatically.  Numeric business measures are
registered as *pending* candidates and are reviewed lazily when a real question
uses one; this is what keeps first-connect review work bounded.
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from ..core.ports.explorer import ExplorerPort
from .catalog import (
    Aggregate,
    CLASSIFICATION_POLICY_VERSION,
    DimensionDisclosureTier,
    DimensionSpec,
    DimensionReviewPolicy,
    JoinSpec,
    MetricExpressionKind,
    MetricSpec,
    ReviewState,
    SemanticCatalog,
    TableSpec,
)

_NUMERIC_TYPE = re.compile(
    r"\b(tinyint|smallint|mediumint|integer|bigint|hugeint|"
    r"u?int(?:8|16|32|64|128)?|int(?:8|16|32|64|128)?|"
    r"numeric|bignumeric|decimal(?:128|256)?|number|real|"
    r"float(?:32|64)?|double(?:\s+precision)?|money)\b",
    re.IGNORECASE,
)
_TIME_TYPE = re.compile(r"\b(date|time|timestamp|datetime)\b", re.IGNORECASE)
_LONG_TEXT_TYPE = re.compile(r"\b(text|clob|json|jsonb|xml|blob)\b", re.IGNORECASE)
_STRING_TYPE = re.compile(
    r"\b(n?varchar2?|n?char|character\s+varying|string|text|clob|citext|enum|set)\b",
    re.IGNORECASE,
)
_BOOLEAN_TYPE = re.compile(r"\b(bool|boolean)\b", re.IGNORECASE)
_STRUCTURED_TYPE = re.compile(
    r"\b(json|jsonb|xml|blob|variant|object|array)\b", re.IGNORECASE
)
_BINARY_TYPE = re.compile(r"\b(binary|varbinary|bytea)\b", re.IGNORECASE)
_IDENTIFIER_TYPE = re.compile(r"\b(uuid|guid)\b", re.IGNORECASE)
_SPATIAL_TYPE = re.compile(r"\b(geometry|geography)\b", re.IGNORECASE)
_USER_TABLE = re.compile(
    r"(user|customer|member|person|employee|contact)", re.IGNORECASE
)
_DIRECT_PII_NAMES = {
    "access_token",
    "account_number",
    "api_key",
    "api_token",
    "auth_token",
    "bank_account",
    "card_number",
    "credit_card_number",
    "client_secret",
    "cookie",
    "credential",
    "credentials",
    "device_id",
    "driver_license_number",
    "email",
    "email_address",
    "home_address",
    "ip_address",
    "mac_address",
    "medical_record_number",
    "national_id",
    "phone",
    "phone_number",
    "mobile",
    "mobile_number",
    "ssn",
    "social_security_number",
    "passport_number",
    "password",
    "password_hash",
    "passwd",
    "private_key",
    "refresh_token",
    "secret",
    "session_token",
    "birth_date",
    "date_of_birth",
    "dob",
    "tax_id",
    "street_address",
    "username",
}
_PERSON_NAME_COLUMNS = {"name", "first_name", "last_name", "full_name"}
_PERSON_ROLE_TOKENS = {
    "applicant",
    "attendee",
    "beneficiary",
    "buyer",
    "cardholder",
    "claimant",
    "client",
    "contact",
    "customer",
    "driver",
    "employee",
    "guest",
    "manager",
    "member",
    "owner",
    "passenger",
    "patient",
    "payer",
    "person",
    "recipient",
    "requester",
    "sender",
    "staff",
    "student",
    "user",
    "worker",
}
_CALENDAR_TOKENS = {"day", "month", "quarter", "week", "year"}
_CALENDAR_QUALIFIERS = {"calendar", "fiscal", "record"}
_CODE_TOKENS = {"beat", "code", "district", "fips", "postal", "ward", "zip"}
_CATEGORICAL_TERMINALS = {"category", "flag", "priority", "status", "tier", "type"}
_BOOLEAN_PREFIXES = {"can", "has", "is", "should"}
_BOOLEAN_TERMINALS = {"disabled", "enabled"}
_COORDINATE_TOKENS = {
    "coordinate",
    "lat",
    "latitude",
    "lng",
    "lon",
    "longitude",
}
_TEMPORAL_NAME_TERMINALS = {"date", "datetime", "time", "timestamp"}
_TEMPORAL_NAME_EXCLUSIONS = {
    "duration",
    "estimate",
    "estimated",
    "hours",
    "minutes",
    "timezone",
    "zone",
}
_PII_SUFFIXES = (
    "_email",
    "_phone",
    "_mobile",
    "_ssn",
    "_passport",
    "_account_number",
    "_card_number",
    "_ip_address",
    "_tax_id",
    "_national_id",
    "_password",
    "_password_hash",
    "_secret",
    "_token",
    "_api_key",
    "_private_key",
)
_FREE_TEXT_NAMES = re.compile(
    r"(^|_)(bio|biography|comment|comments|content|description|details|memo|"
    r"message|notes?|profile|remarks?|text)(_|$)",
    re.IGNORECASE,
)
_NARRATIVE_TOKENS = {
    "answer",
    "bio",
    "biography",
    "body",
    "comment",
    "comments",
    "content",
    "description",
    "details",
    "memo",
    "message",
    "narrative",
    "note",
    "notes",
    "prompt",
    "question",
    "remarks",
    "response",
    "summary",
    "text",
    "transcript",
}
_NARRATIVE_TITLE_TABLE_TOKENS = {
    "article",
    "comment",
    "document",
    "message",
    "post",
    "ticket",
}
_IDENTIFIER_TOKENS = {
    "checksum",
    "guid",
    "hash",
    "identifier",
    "path",
    "uri",
    "url",
    "uuid",
}
_SAFE_DIMENSION_NAMES = re.compile(
    r"(^is_|^has_|^active$|^enabled$|(^|_)(brand|category|channel|city|class|"
    r"code|country|currency|department|destination|division|flag|grade|group|"
    r"industry|kind|language|level|locale|market|method|mode|model|"
    r"platform|product|province|region|role|segment|source|state|status|tier|"
    r"type)(_|$))",
    re.IGNORECASE,
)
_UNIT_SUFFIXES = {
    "kg": "kg",
    "kilogram": "kg",
    "ton": "metric_ton",
    "tons": "metric_ton",
    "tonne": "metric_ton",
    "usd": "USD",
    "krw": "KRW",
    "eur": "EUR",
    "pct": "percent",
    "percent": "percent",
}


@dataclass
class OnboardingSummary:
    table_count: int
    declared_join_count: int
    blocked_column_count: int
    confirmed_metric_count: int
    pending_metric_count: int
    catalog: SemanticCatalog


async def build_catalog(explorer: ExplorerPort) -> OnboardingSummary:
    """Build a semantic catalog without reading raw column values."""

    listed = await explorer.list_tables()
    metadata = await _read_catalog_metadata(explorer)
    tables: list[TableSpec] = []
    metrics: list[MetricSpec] = []
    dimensions: list[DimensionSpec] = []
    blocked_columns: list[str] = []
    physical_snapshot: list[dict[str, Any]] = []
    columns_by_table_id: dict[str, set[str]] = {}
    metadata_tables = metadata.get("tables", {})
    if not isinstance(metadata_tables, Mapping):
        metadata_tables = {}

    for listed_table in listed:
        described = await explorer.describe_table(listed_table.name)
        table_id = _table_id(described.schema, described.name)
        tables.append(
            TableSpec(id=table_id, name=described.name, schema=described.schema)
        )
        columns_by_table_id[table_id] = {column.name for column in described.columns}
        raw_table_meta = metadata_tables.get(described.name, {})
        table_meta = raw_table_meta if isinstance(raw_table_meta, Mapping) else {}
        primary_key = set(_string_sequence(table_meta.get("primary_key", [])))
        foreign_key_columns = {
            column
            for foreign_key in _mapping_sequence(table_meta.get("foreign_keys", []))
            for column in _string_sequence(foreign_key.get("columns", []))
            if column in columns_by_table_id[table_id]
        }

        physical_snapshot.append(
            {
                "table": table_id,
                "columns": [
                    {
                        "name": column.name,
                        "type": column.type,
                        "nullable": column.nullable,
                        "description": column.description,
                    }
                    for column in described.columns
                ],
                "primary_key": sorted(primary_key),
                "foreign_keys": table_meta.get("foreign_keys", []),
            }
        )

        # This is explicitly a physical source-row count, never an inferred
        # business-entity count. COUNT(*) is stable for PK-less, duplicate,
        # all-NULL, and empty sources without inventing a synthetic identifier.
        count_aliases = _source_count_aliases(table_id)
        metrics.append(
            MetricSpec(
                id=f"metric:{table_id}.source_record_count",
                label=f"{table_id} source record count",
                table_id=table_id,
                column="",
                expression_kind=MetricExpressionKind.SOURCE_ROWS,
                aggregate=Aggregate.COUNT,
                state=ReviewState.CONFIRMED,
                allowed_aggregates=[Aggregate.COUNT],
                data_type="source_rows",
                nullable=False,
                classification_evidence="source_record_count_contract",
                source_record_count=True,
                aliases=count_aliases,
                reviewed_bindings={
                    alias: [Aggregate.COUNT.value] for alias in count_aliases
                },
            )
        )

        for column in described.columns:
            column_ref = f"{table_id}.{column.name}"
            if _is_pii_like(described.name, column.name, column.description):
                blocked_columns.append(column_ref)
                continue
            if _is_hard_blocked_text(described.name, column.name, column.type):
                # Unknown long/free text can contain embedded identifiers even
                # when its column name is not a classic PII token. It is safer
                # to omit it than to group by and print raw text values.
                blocked_columns.append(column_ref)
                continue

            is_key = column.name in primary_key or column.name in foreign_key_columns
            is_numeric = bool(_NUMERIC_TYPE.search(column.type or ""))
            if is_key:
                # Keys remain available to declared join metadata but are not
                # selectable measures or raw output dimensions.
                blocked_columns.append(column_ref)
                continue

            physical_aliases = _physical_aliases(table_id, column.name)
            if is_numeric:
                role = _numeric_non_measure_role(column.name)
                if role in {"identifier", "coordinate"}:
                    blocked_columns.append(column_ref)
                    continue
                if role in {"boolean", "calendar", "categorical", "code"}:
                    dimensions.append(
                        DimensionSpec(
                            id=f"dimension:{column_ref}",
                            label=column_ref,
                            table_id=table_id,
                            column=column.name,
                            data_type=column.type,
                            kind=role,
                            review_policy=DimensionReviewPolicy.RELEASE_REQUIRED,
                            classification_evidence=(f"numeric_{role}_metadata_only"),
                            classification_policy_version=(
                                CLASSIFICATION_POLICY_VERSION
                            ),
                            raw_output_allowed=False,
                            disclosure_tier=DimensionDisclosureTier.BLOCKED,
                            aliases=[],
                            reserved_aliases=physical_aliases,
                        )
                    )
                    continue
                metrics.append(
                    MetricSpec(
                        id=f"metric:{column_ref}",
                        label=column_ref,
                        table_id=table_id,
                        column=column.name,
                        unit=_infer_unit(column.name),
                        data_type=column.type,
                        nullable=column.nullable,
                        classification_evidence="numeric_measure_metadata_only",
                        aliases=physical_aliases,
                    )
                )
                continue

            if not _is_supported_dimension_type(column.type):
                blocked_columns.append(column_ref)
                continue
            review_policy, evidence = _dimension_review_policy(column.name, column.type)
            raw_output_allowed = review_policy == DimensionReviewPolicy.AUTO_SAFE
            aliases = physical_aliases if raw_output_allowed else []
            dimensions.append(
                DimensionSpec(
                    id=f"dimension:{column_ref}",
                    label=column_ref,
                    table_id=table_id,
                    column=column.name,
                    data_type=column.type,
                    kind=(
                        "time"
                        if _TIME_TYPE.search(column.type or "")
                        or _is_temporal_name_candidate(column.name)
                        else "categorical"
                    ),
                    review_policy=review_policy,
                    classification_evidence=evidence,
                    classification_policy_version=CLASSIFICATION_POLICY_VERSION,
                    raw_output_allowed=raw_output_allowed,
                    disclosure_tier=(
                        DimensionDisclosureTier.PUBLIC_GROUPED
                        if raw_output_allowed
                        else DimensionDisclosureTier.BLOCKED
                    ),
                    aliases=aliases,
                    reserved_aliases=physical_aliases,
                )
            )

    joins = _build_declared_joins(tables, metadata, columns_by_table_id)
    _remove_ambiguous_auto_aliases(metrics)
    _remove_ambiguous_auto_aliases(dimensions)
    for metric in metrics:
        metric.auto_aliases = list(metric.aliases)
    for dimension in dimensions:
        dimension.auto_aliases = list(dimension.aliases)
    # Adapter enumeration order is not schema identity. Canonicalize only the
    # unordered table/column/FK collections; preserve composite-FK column order.
    canonical_snapshot = [
        {
            **item,
            "columns": sorted(
                item["columns"],
                key=lambda column: (
                    str(column.get("name", "")),
                    str(column.get("type", "")),
                    str(column.get("nullable", "")),
                    str(column.get("description", "")),
                ),
            ),
            "foreign_keys": sorted(
                item["foreign_keys"],
                key=lambda foreign_key: json.dumps(
                    foreign_key, sort_keys=True, separators=(",", ":")
                ),
            ),
        }
        for item in sorted(physical_snapshot, key=lambda item: str(item["table"]))
    ]
    fingerprint = hashlib.sha256(
        json.dumps(canonical_snapshot, sort_keys=True).encode("utf-8")
    ).hexdigest()
    catalog = SemanticCatalog(
        fingerprint=fingerprint,
        tables=tables,
        metrics=metrics,
        dimensions=dimensions,
        joins=joins,
        blocked_columns=sorted(set(blocked_columns)),
        classification_policy_version=CLASSIFICATION_POLICY_VERSION,
    )
    return OnboardingSummary(
        table_count=len(tables),
        declared_join_count=len(joins),
        blocked_column_count=len(catalog.blocked_columns),
        confirmed_metric_count=catalog.confirmed_metric_count,
        pending_metric_count=catalog.pending_metric_count,
        catalog=catalog,
    )


async def _read_catalog_metadata(explorer: ExplorerPort) -> dict[str, Any]:
    method = getattr(explorer, "catalog_metadata", None)
    if method is None:
        return {"tables": {}}
    result = method()
    if hasattr(result, "__await__"):
        result = await result
    return result if isinstance(result, dict) else {"tables": {}}


def _build_declared_joins(
    tables: list[TableSpec],
    metadata: dict[str, Any],
    columns_by_table_id: Mapping[str, set[str]],
) -> list[JoinSpec]:
    candidates_by_name: dict[str, list[TableSpec]] = {}
    for table in tables:
        candidates_by_name.setdefault(table.name, []).append(table)
    by_name = {
        name: candidates[0]
        for name, candidates in candidates_by_name.items()
        if len(candidates) == 1
    }
    metadata_tables = metadata.get("tables", {})
    if not isinstance(metadata_tables, Mapping):
        return []
    joins: list[JoinSpec] = []
    seen: set[tuple[str, str, str, str]] = set()
    for child_name, raw_table_meta in metadata_tables.items():
        if not isinstance(child_name, str) or not isinstance(raw_table_meta, Mapping):
            continue
        child = by_name.get(child_name)
        if child is None:
            continue
        for foreign_key in _mapping_sequence(raw_table_meta.get("foreign_keys", [])):
            columns = _string_sequence(foreign_key.get("columns", []))
            referred_columns = _string_sequence(foreign_key.get("referred_columns", []))
            referred_table = foreign_key.get("referred_table")
            parent = (
                by_name.get(referred_table) if isinstance(referred_table, str) else None
            )
            # Composite joins are deliberately held back from the first slice;
            # missing/guessed identifiers would be equally unsafe.
            if parent is None or len(columns) != 1 or len(referred_columns) != 1:
                continue
            child_column = columns[0]
            parent_column = referred_columns[0]
            if child_column not in columns_by_table_id.get(
                child.id, set()
            ) or parent_column not in columns_by_table_id.get(parent.id, set()):
                continue
            edge = (child.id, child_column, parent.id, parent_column)
            if edge in seen:
                continue
            seen.add(edge)
            joins.append(
                JoinSpec(
                    id=(
                        f"join:{child.id}.{child_column}->"
                        f"{parent.id}.{parent_column}"
                    ),
                    child_table_id=child.id,
                    child_column=child_column,
                    parent_table_id=parent.id,
                    parent_column=parent_column,
                )
            )
    return joins


def _mapping_sequence(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _string_sequence(value: Any) -> list[str]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        return []
    if not all(isinstance(item, str) and item.strip() for item in value):
        return []
    return list(value)


def _table_id(schema: str, name: str) -> str:
    return f"{schema}.{name}" if schema else name


def _identifier_tokens(value: str) -> list[str]:
    """Tokenize snake/camel/Pascal identifiers before applying safety rules."""

    normalized = unicodedata.normalize("NFKC", value)
    normalized = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", normalized)
    normalized = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", normalized)
    normalized = re.sub(r"([A-Za-z])([0-9])", r"\1_\2", normalized)
    normalized = re.sub(r"([0-9])([A-Za-z])", r"\1_\2", normalized)
    return [
        token
        for token in re.sub(r"[^0-9A-Za-z가-힣]+", "_", normalized.lower()).split("_")
        if token
    ]


def _numeric_non_measure_role(column: str) -> str:
    """Classify only strong numeric non-measure names after identifier tokenization."""

    tokens = _identifier_tokens(column)
    if not tokens:
        return ""
    last = tokens[-1]
    if last in {"id", "key", "guid", "uuid"}:
        return "identifier"
    if last == "number" and len(tokens) > 1:
        return "identifier"
    if set(tokens).intersection(_COORDINATE_TOKENS):
        return "coordinate"
    if (
        len(tokens) >= 2
        and tokens[0] in _BOOLEAN_PREFIXES
        and tokens[-1] not in {"count", "number", "total"}
    ) or last in _BOOLEAN_TERMINALS:
        return "boolean"
    if last in _CATEGORICAL_TERMINALS:
        return "categorical"
    if last in _CODE_TOKENS:
        return "code"
    if last in _CALENDAR_TOKENS:
        return "calendar"
    if (
        len(tokens) >= 2
        and tokens[-2] in _CALENDAR_QUALIFIERS
        and last in _CALENDAR_TOKENS
    ):
        return "calendar"
    return ""


def _is_temporal_name_candidate(column: str) -> bool:
    """Recognize metadata-only temporal hints without guessing an encoding."""

    tokens = _identifier_tokens(column)
    if not tokens or set(tokens).intersection(_TEMPORAL_NAME_EXCLUSIONS):
        return False
    if tokens[-1] in _TEMPORAL_NAME_TERMINALS:
        return True
    return len(tokens) >= 2 and tokens[-2:] in (
        ["created", "at"],
        ["updated", "at"],
        ["deleted", "at"],
    )


def _is_supported_dimension_type(data_type: str) -> bool:
    return bool(
        _TIME_TYPE.search(data_type or "")
        or _BOOLEAN_TYPE.search(data_type or "")
        or _STRING_TYPE.search(data_type or "")
    )


def _is_pii_like(table: str, column: str, description: str) -> bool:
    tokens = _identifier_tokens(column)
    normalized = "_".join(tokens)
    compact = "".join(tokens)
    compact_without_numeric_suffix = re.sub(r"\d+$", "", compact)
    table_tokens = set(_identifier_tokens(table))
    if normalized in _DIRECT_PII_NAMES:
        return True
    if normalized.endswith(_PII_SUFFIXES):
        return True
    if any(
        token
        in {
            "email",
            "phone",
            "mobile",
            "ssn",
            "passport",
            "password",
            "secret",
            "token",
        }
        for token in tokens
    ):
        return True
    if any(
        marker in compact
        for marker in (
            "accountnumber",
            "apikey",
            "authtoken",
            "bankaccount",
            "birthyear",
            "birthdate",
            "cardnumber",
            "clientsecret",
            "creditcard",
            "dateofbirth",
            "driverlicense",
            "ipaddress",
            "medicalrecord",
            "nationalid",
            "privatekey",
            "refreshtoken",
            "socialsecurity",
            "taxid",
        )
    ):
        return True
    user_context = (
        bool(_USER_TABLE.search(table))
        or bool(table_tokens.intersection({"patient", "profile"}))
        or bool(
            set(tokens).intersection(
                {"customer", "member", "patient", "person", "profile", "user"}
            )
        )
    )
    person_name = (
        normalized in _PERSON_NAME_COLUMNS
        or compact_without_numeric_suffix.endswith(
            ("displayname", "firstname", "lastname", "fname", "lname")
        )
        or any(token in {"forename", "surname"} for token in tokens)
        or (tokens and tokens[-1] in {"fname", "lname"})
        or (len(tokens) >= 2 and tokens[-2:] in (["f", "name"], ["l", "name"]))
    )
    explicit_person_name = normalized in {"first_name", "last_name", "full_name"}
    role_qualified_name = "name" in tokens and bool(
        set(tokens).intersection(_PERSON_ROLE_TOKENS)
    )
    if explicit_person_name or role_qualified_name or (user_context and person_name):
        return True
    if user_context and any(
        marker in compact
        for marker in (
            "aboutme",
            "homeaddress",
            "location",
            "mailstreet",
            "profileimage",
            "street",
            "streetaddress",
            "websiteurl",
        )
    ):
        return True
    desc = (description or "").lower()
    return any(
        token in desc
        for token in (
            "email address",
            "phone number",
            "personally identifiable",
            "personal data",
            "pii",
        )
    )


def _is_hard_blocked_text(table: str, column: str, data_type: str) -> bool:
    if any(
        pattern.search(data_type or "")
        for pattern in (
            _STRUCTURED_TYPE,
            _BINARY_TYPE,
            _IDENTIFIER_TYPE,
            _SPATIAL_TYPE,
        )
    ):
        # These types can reveal nested payloads or technical identifiers and
        # need a future typed capability, not grouped-label disclosure.
        return True
    if not _STRING_TYPE.search(data_type or "") and not _LONG_TEXT_TYPE.search(
        data_type or ""
    ):
        return False
    column_tokens = _identifier_tokens(column)
    table_tokens = set(_identifier_tokens(table))
    table_tokens.update(
        token[:-1]
        for token in list(table_tokens)
        if token.endswith("s") and len(token) > 3
    )
    compact = "".join(column_tokens)
    if set(column_tokens).intersection(_NARRATIVE_TOKENS):
        return True
    if "title" in column_tokens and table_tokens.intersection(
        _NARRATIVE_TITLE_TABLE_TOKENS
    ):
        return True
    if _FREE_TEXT_NAMES.search("_".join(column_tokens)):
        return True
    if set(column_tokens).intersection(_IDENTIFIER_TOKENS):
        return True
    if column_tokens and column_tokens[-1] == "id":
        return True
    return compact.endswith(("guid", "uuid", "checksum"))


def _dimension_review_policy(
    column: str, data_type: str
) -> tuple[DimensionReviewPolicy, str]:
    if _TIME_TYPE.search(data_type or ""):
        # Exact timestamps can be unique activity identifiers. They need the
        # same disclosure gate and group-size guard as string labels; typed
        # date parsing/bucketing remains a separate future capability.
        return DimensionReviewPolicy.RELEASE_REQUIRED, "native_time_metadata_only"
    if _BOOLEAN_TYPE.search(data_type or ""):
        return DimensionReviewPolicy.AUTO_SAFE, "boolean_type"
    if _STRING_TYPE.search(data_type or "") or _LONG_TEXT_TYPE.search(data_type or ""):
        if _is_temporal_name_candidate(column):
            # The name supports only a temporal role candidate. Format,
            # timezone, parsing, bucketing, and range semantics remain unknown.
            return DimensionReviewPolicy.RELEASE_REQUIRED, "string_time_metadata_only"
        evidence = (
            "categorical_name_metadata_only"
            if _SAFE_DIMENSION_NAMES.search(column)
            else "string_metadata_only"
        )
        # A column name or VARCHAR length describes shape, not disclosure
        # safety. Even plausible categories can contain names or singleton
        # labels, so every string value needs the distinct steward gate.
        return DimensionReviewPolicy.RELEASE_REQUIRED, evidence
    # Unknown vendor-specific types are not evidence of disclosure safety.
    return DimensionReviewPolicy.RELEASE_REQUIRED, "unknown_type_metadata_only"


def _infer_unit(column: str) -> str:
    tokens = [token for token in re.split(r"[^a-zA-Z]+", column.lower()) if token]
    for token in reversed(tokens):
        if token in _UNIT_SUFFIXES:
            return _UNIT_SUFFIXES[token]
    return ""


def _physical_aliases(table_id: str, column: str) -> list[str]:
    """Create only aliases justified by physical names, never business guesses."""

    table = table_id.rsplit(".", 1)[-1]
    table_forms = {table}
    if table.endswith("s") and len(table) > 3:
        table_forms.add(table[:-1])
    return sorted(
        {
            _normalize_alias(column),
            _normalize_alias(f"{table_id} {column}"),
            *{_normalize_alias(f"{table_form} {column}") for table_form in table_forms},
        }
    )


def _source_count_aliases(table_id: str) -> list[str]:
    table = table_id.rsplit(".", 1)[-1]
    table_tokens = _identifier_tokens(table)
    entity = table_tokens[-1] if table_tokens else table
    if entity.endswith("s") and len(entity) > 3:
        entity = entity[:-1]
    return sorted(
        {
            _normalize_alias(f"{table} count"),
            _normalize_alias(f"{table} row count"),
            _normalize_alias(f"{table} source record count"),
            _normalize_alias(f"source {entity} rows"),
            _normalize_alias(f"{entity} source rows"),
        }
    )


def _normalize_alias(value: str) -> str:
    return " ".join(re.sub(r"[^0-9a-zA-Z가-힣]+", " ", value.lower()).split())


def _remove_ambiguous_auto_aliases(items: list[Any]) -> None:
    """Do not auto-trust a bare name shared by multiple catalog entities."""

    owners: dict[str, set[str]] = {}
    for item in items:
        for alias in item.aliases:
            owners.setdefault(alias, set()).add(item.id)
    ambiguous = {alias for alias, entity_ids in owners.items() if len(entity_ids) > 1}
    if not ambiguous:
        return
    for item in items:
        item.aliases = [alias for alias in item.aliases if alias not in ambiguous]
        if hasattr(item, "reviewed_bindings"):
            item.reviewed_bindings = {
                alias: aggregate
                for alias, aggregate in item.reviewed_bindings.items()
                if alias not in ambiguous
            }
