"""PII-safe, evidence-first semantic onboarding.

Only catalog facts are accepted automatically.  Numeric business measures are
registered as *pending* candidates and are reviewed lazily when a real question
uses one; this is what keeps first-connect review work bounded.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

from ..core.ports.explorer import ExplorerPort
from .catalog import (
    Aggregate,
    DimensionSpec,
    JoinSpec,
    MetricSpec,
    ReviewState,
    SemanticCatalog,
    TableSpec,
)


_NUMERIC_TYPE = re.compile(
    r"\b(int|integer|bigint|smallint|numeric|decimal|number|real|float|double|money)\b",
    re.IGNORECASE,
)
_TIME_TYPE = re.compile(r"\b(date|time|timestamp|datetime)\b", re.IGNORECASE)
_LONG_TEXT_TYPE = re.compile(r"\b(text|clob|json|jsonb|xml|blob)\b", re.IGNORECASE)
_ID_NAME = re.compile(r"(^id$|_id$|^id_|_key$|^key$)", re.IGNORECASE)
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
_SAFE_DIMENSION_NAMES = re.compile(
    r"(^is_|^has_|^active$|^enabled$|(^|_)(brand|category|channel|city|class|"
    r"code|country|currency|department|destination|division|flag|grade|group|"
    r"industry|kind|language|level|locale|market|method|mode|model|name|"
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

    for listed_table in listed:
        described = await explorer.describe_table(listed_table.name)
        table_id = _table_id(described.schema, described.name)
        tables.append(
            TableSpec(id=table_id, name=described.name, schema=described.schema)
        )
        table_meta = metadata.get("tables", {}).get(described.name, {})
        primary_key = set(table_meta.get("primary_key", []))
        foreign_key_columns = {
            column
            for foreign_key in table_meta.get("foreign_keys", [])
            for column in foreign_key.get("columns", [])
        }

        physical_snapshot.append(
            {
                "table": table_id,
                "columns": [
                    {
                        "name": column.name,
                        "type": column.type,
                        "nullable": column.nullable,
                    }
                    for column in described.columns
                ],
                "primary_key": sorted(primary_key),
                "foreign_keys": table_meta.get("foreign_keys", []),
            }
        )

        if primary_key:
            # This is explicitly a source-row count, not an inferred business
            # entity count.  A declared non-null PK makes the physical meaning
            # stable enough to auto-confirm.
            pk_column = sorted(primary_key)[0]
            count_aliases = _source_count_aliases(table_id)
            metrics.append(
                MetricSpec(
                    id=f"metric:{table_id}.source_record_count",
                    label=f"{table_id} source record count",
                    table_id=table_id,
                    column=pk_column,
                    aggregate=Aggregate.COUNT,
                    state=ReviewState.CONFIRMED,
                    allowed_aggregates=[Aggregate.COUNT],
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
            if _is_unreviewed_free_text(column.name, column.type):
                # Unknown long/free text can contain embedded identifiers even
                # when its column name is not a classic PII token. It is safer
                # to omit it than to group by and print raw text values.
                blocked_columns.append(column_ref)
                continue

            is_key = column.name in primary_key or column.name in foreign_key_columns
            is_numeric = bool(_NUMERIC_TYPE.search(column.type or ""))
            if is_numeric and not is_key and not _ID_NAME.search(column.name):
                metrics.append(
                    MetricSpec(
                        id=f"metric:{column_ref}",
                        label=column_ref,
                        table_id=table_id,
                        column=column.name,
                        unit=_infer_unit(column.name),
                        aliases=_physical_aliases(table_id, column.name),
                    )
                )

            if not is_key and not is_numeric:
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
                            else "categorical"
                        ),
                        aliases=_physical_aliases(table_id, column.name),
                    )
                )

    joins = _build_declared_joins(tables, metadata)
    _remove_ambiguous_auto_aliases(metrics)
    _remove_ambiguous_auto_aliases(dimensions)
    for metric in metrics:
        metric.auto_aliases = list(metric.aliases)
    for dimension in dimensions:
        dimension.auto_aliases = list(dimension.aliases)
    fingerprint = hashlib.sha256(
        json.dumps(physical_snapshot, sort_keys=True).encode("utf-8")
    ).hexdigest()
    catalog = SemanticCatalog(
        fingerprint=fingerprint,
        tables=tables,
        metrics=metrics,
        dimensions=dimensions,
        joins=joins,
        blocked_columns=sorted(set(blocked_columns)),
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
    tables: list[TableSpec], metadata: dict[str, Any]
) -> list[JoinSpec]:
    by_name = {table.name: table for table in tables}
    joins: list[JoinSpec] = []
    for child_name, table_meta in metadata.get("tables", {}).items():
        child = by_name.get(child_name)
        if child is None:
            continue
        for foreign_key in table_meta.get("foreign_keys", []):
            columns = list(foreign_key.get("columns", []))
            referred_columns = list(foreign_key.get("referred_columns", []))
            parent = by_name.get(str(foreign_key.get("referred_table", "")))
            # Composite joins are deliberately held back from the first slice;
            # silently compiling only one column would be unsafe.
            if parent is None or len(columns) != 1 or len(referred_columns) != 1:
                continue
            joins.append(
                JoinSpec(
                    id=(
                        f"join:{child.id}.{columns[0]}->"
                        f"{parent.id}.{referred_columns[0]}"
                    ),
                    child_table_id=child.id,
                    child_column=columns[0],
                    parent_table_id=parent.id,
                    parent_column=referred_columns[0],
                )
            )
    return joins


def _table_id(schema: str, name: str) -> str:
    return f"{schema}.{name}" if schema else name


def _is_pii_like(table: str, column: str, description: str) -> bool:
    normalized = column.strip().lower()
    if normalized in _DIRECT_PII_NAMES:
        return True
    if normalized.endswith(_PII_SUFFIXES):
        return True
    if normalized in _PERSON_NAME_COLUMNS and _USER_TABLE.search(table):
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


def _infer_unit(column: str) -> str:
    tokens = [token for token in re.split(r"[^a-zA-Z]+", column.lower()) if token]
    for token in reversed(tokens):
        if token in _UNIT_SUFFIXES:
            return _UNIT_SUFFIXES[token]
    return ""


def _is_unreviewed_free_text(column: str, data_type: str) -> bool:
    if _FREE_TEXT_NAMES.search(column):
        return True
    return bool(
        _LONG_TEXT_TYPE.search(data_type or "")
        and not _SAFE_DIMENSION_NAMES.search(column)
    )


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
    return sorted(
        {
            _normalize_alias(f"{table} count"),
            _normalize_alias(f"{table} row count"),
            _normalize_alias(f"{table} source record count"),
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
