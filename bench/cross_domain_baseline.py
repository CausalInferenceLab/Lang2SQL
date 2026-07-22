#!/usr/bin/env python3
"""Measure semantic onboarding behavior across materialized public databases.

The evaluator records structural failure families only. It never samples raw
values, approves a metric, or feeds benchmark metadata into production code.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import time
from typing import Any, Mapping, Sequence

from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer
from lang2sql.semantic.catalog import DimensionReviewPolicy
from lang2sql.semantic.onboarding import build_catalog

import dataset_cache


_NUMERIC_ROLE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("identifier", re.compile(r"(^id$|_id$|^id_|_key$|^key$|_number$)", re.I)),
    (
        "code",
        re.compile(
            r"(^|_)(code|fips|zip|postal|ward|district|beat|flag|status|category)(_.*)?$",
            re.I,
        ),
    ),
    ("calendar", re.compile(r"(^|_)(year|month|quarter|week|day)(_|$)", re.I)),
    (
        "coordinate",
        re.compile(
            r"(^|_)(lat|latitude|lon|lng|longitude|x_coordinate|y_coordinate)(_|$)",
            re.I,
        ),
    ),
    ("boolean", re.compile(r"^(is_|has_|can_|was_|did_|active$|enabled$)", re.I)),
)
_TIME_NAME = re.compile(
    r"(^|_)(date|time|timestamp|datetime|created|updated|observed|recorded|year|month|quarter)(_|$)",
    re.I,
)
_NATIVE_TIME_TYPE = re.compile(r"\b(date|time|timestamp|datetime)\b", re.I)
_NUMERIC_TYPE = re.compile(
    r"\b(int|integer|bigint|smallint|numeric|decimal|number|real|float|double|money|boolean|bool)\b",
    re.I,
)
_PERSON_TABLE = re.compile(
    r"(user|customer|member|person|patient|employee|contact)", re.I
)
_PII_NAME = re.compile(
    r"(^|_)(email|phone|mobile|ssn|passport|password|secret|token|address|birth_date|dob)(_|$)",
    re.I,
)
_PERSON_NAME = re.compile(r"^(name|first_name|last_name|full_name)$", re.I)


def _table_id(schema: str, name: str) -> str:
    return f"{schema}.{name}" if schema else name


def _numeric_role_risk(column_name: str, data_type: str) -> str:
    if not _NUMERIC_TYPE.search(data_type or ""):
        return ""
    for role, pattern in _NUMERIC_ROLE_PATTERNS:
        if pattern.search(column_name):
            return role
    return ""


def _potential_pii(table: str, column: str) -> bool:
    return bool(_PII_NAME.search(column)) or bool(
        _PERSON_TABLE.search(table) and _PERSON_NAME.search(column)
    )


def _lock_hashes(cache_root: Path) -> dict[str, str]:
    lock_path = cache_root / dataset_cache.LOCK_FILENAME
    if not lock_path.exists():
        raise dataset_cache.DatasetCacheError(f"missing source lock: {lock_path}")
    lock = json.loads(lock_path.read_text(encoding="utf-8"))
    return {
        dataset_id: str(record["sha256"])
        for dataset_id, record in lock.get("datasets", {}).items()
    }


def _materialized_paths(
    manifest: Mapping[str, Any], cache_root: Path
) -> dict[str, Path]:
    manifest_records = dataset_cache.declared_databases(manifest)
    by_dataset = {entry["dataset_id"]: entry for entry in manifest["datasets"]}
    paths: dict[str, Path] = {}
    for record in manifest_records:
        entry = by_dataset[record["dataset_id"]]
        materialization = entry["materialization"]
        if materialization["kind"] == "csv_to_sqlite":
            path = cache_root / "materialized" / f"{record['dataset_id']}.sqlite"
        else:
            root = cache_root / "materialized" / record["dataset_id"]
            matches = list(root.rglob(f"{record['db_id']}.sqlite"))
            if len(matches) != 1:
                raise dataset_cache.DatasetCacheError(
                    f"expected one materialized DB for {record['db_id']}, found {len(matches)}"
                )
            path = matches[0]
        if not path.exists():
            raise dataset_cache.DatasetCacheError(
                f"database is not materialized: {record['db_id']} ({path})"
            )
        paths[record["db_id"]] = path
    return paths


async def evaluate_database(record: Mapping[str, str], path: Path) -> dict[str, Any]:
    """Run the unmodified onboarding path and classify structural risks."""

    started = time.perf_counter()
    explorer = SqlAlchemyExplorer(f"sqlite:///{path.resolve()}")
    try:
        summary = await build_catalog(explorer)
        listed = await explorer.list_tables()
        metadata = await explorer.catalog_metadata()
        descriptions = await asyncio.gather(
            *(explorer.describe_table(table.name) for table in listed)
        )
        metric_by_ref = {
            f"{metric.table_id}.{metric.column}": metric
            for metric in summary.catalog.metrics
        }
        dimension_by_ref = {
            f"{dimension.table_id}.{dimension.column}": dimension
            for dimension in summary.catalog.dimensions
        }
        blocked = set(summary.catalog.blocked_columns)
        source_count_tables = {
            metric.table_id
            for metric in summary.catalog.metrics
            if metric.source_record_count
        }

        source_count_missing: list[str] = []
        metric_role_risks: list[dict[str, str]] = []
        string_time_not_typed: list[dict[str, str]] = []
        potential_pii_exposure: list[str] = []
        total_columns = 0
        for described in descriptions:
            table_id = _table_id(described.schema, described.name)
            if table_id not in source_count_tables:
                source_count_missing.append(table_id)
            for column in described.columns:
                total_columns += 1
                reference = f"{table_id}.{column.name}"
                risk = _numeric_role_risk(column.name, column.type)
                metric = metric_by_ref.get(reference)
                if (
                    risk
                    and metric is not None
                    and any(
                        aggregate.value == "sum"
                        for aggregate in metric.allowed_aggregates
                    )
                ):
                    metric_role_risks.append(
                        {
                            "column": reference,
                            "role_evidence": risk,
                            "data_type": column.type,
                        }
                    )
                if (
                    _TIME_NAME.search(column.name)
                    and not _NATIVE_TIME_TYPE.search(column.type or "")
                    and not _NUMERIC_TYPE.search(column.type or "")
                ):
                    dimension = dimension_by_ref.get(reference)
                    observed_role = (
                        "blocked"
                        if reference in blocked
                        else dimension.kind
                        if dimension is not None
                        else "metric"
                        if metric is not None
                        else "missing"
                    )
                    if observed_role != "time":
                        string_time_not_typed.append(
                            {
                                "column": reference,
                                "data_type": column.type,
                                "observed_role": observed_role,
                            }
                        )
                if (
                    _potential_pii(described.name, column.name)
                    and reference not in blocked
                ):
                    potential_pii_exposure.append(reference)

        composite_foreign_keys = [
            {
                "table": table_name,
                "columns": list(foreign_key.get("columns", [])),
                "referred_table": str(foreign_key.get("referred_table", "")),
                "referred_columns": list(foreign_key.get("referred_columns", [])),
            }
            for table_name, table_meta in metadata.get("tables", {}).items()
            for foreign_key in table_meta.get("foreign_keys", [])
            if len(foreign_key.get("columns", [])) > 1
        ]
        return {
            **dict(record),
            "path": str(path),
            "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
            "table_count": summary.table_count,
            "column_count": total_columns,
            "metric_count": len(summary.catalog.metrics),
            "dimension_count": len(summary.catalog.dimensions),
            "auto_safe_dimension_count": sum(
                dimension.review_policy == DimensionReviewPolicy.AUTO_SAFE
                for dimension in summary.catalog.dimensions
            ),
            "release_required_dimension_count": sum(
                dimension.review_policy == DimensionReviewPolicy.RELEASE_REQUIRED
                for dimension in summary.catalog.dimensions
            ),
            "blocked_column_count": len(summary.catalog.blocked_columns),
            "declared_join_count": len(summary.catalog.joins),
            "catalog_json_chars": len(summary.catalog.to_json()),
            "source_count_missing": sorted(source_count_missing),
            "metric_role_risks": sorted(
                metric_role_risks, key=lambda item: item["column"]
            ),
            "string_time_not_typed": sorted(
                string_time_not_typed, key=lambda item: item["column"]
            ),
            "potential_pii_exposure": sorted(potential_pii_exposure),
            "composite_foreign_keys_blocked": composite_foreign_keys,
        }
    finally:
        if explorer._engine is not None:
            explorer._engine.dispose()


def _aggregate(results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    role_counts: dict[str, int] = {}
    for result in results:
        for risk in result["metric_role_risks"]:
            role = risk["role_evidence"]
            role_counts[role] = role_counts.get(role, 0) + 1
    return {
        "database_count": len(results),
        "table_count": sum(int(result["table_count"]) for result in results),
        "column_count": sum(int(result["column_count"]) for result in results),
        "source_count_missing_tables": sum(
            len(result["source_count_missing"]) for result in results
        ),
        "databases_with_source_count_gap": sum(
            bool(result["source_count_missing"]) for result in results
        ),
        "numeric_metric_role_risks": sum(
            len(result["metric_role_risks"]) for result in results
        ),
        "auto_safe_dimensions": sum(
            int(result.get("auto_safe_dimension_count", 0)) for result in results
        ),
        "release_required_dimensions": sum(
            int(result.get("release_required_dimension_count", 0)) for result in results
        ),
        "metric_role_risks_by_evidence": dict(sorted(role_counts.items())),
        "string_time_not_typed": sum(
            len(result["string_time_not_typed"]) for result in results
        ),
        "databases_with_string_time_gap": sum(
            bool(result["string_time_not_typed"]) for result in results
        ),
        "potential_pii_exposure": sum(
            len(result["potential_pii_exposure"]) for result in results
        ),
        "composite_foreign_keys_blocked": sum(
            len(result["composite_foreign_keys_blocked"]) for result in results
        ),
        "max_catalog_json_chars": max(
            (int(result["catalog_json_chars"]) for result in results), default=0
        ),
        "max_catalog_db_id": max(
            results, key=lambda result: int(result["catalog_json_chars"]), default={}
        ).get("db_id", ""),
        "elapsed_ms": round(sum(float(result["elapsed_ms"]) for result in results), 3),
    }


async def run_baseline(
    manifest: Mapping[str, Any], cache_root: Path, concurrency: int = 4
) -> dict[str, Any]:
    records = dataset_cache.declared_databases(manifest)
    paths = _materialized_paths(manifest, cache_root)
    hashes = _lock_hashes(cache_root)
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def bounded(record: Mapping[str, str]) -> dict[str, Any]:
        async with semaphore:
            result = await evaluate_database(record, paths[record["db_id"]])
            result["source_sha256"] = hashes[record["dataset_id"]]
            return result

    results = await asyncio.gather(*(bounded(record) for record in records))
    return {
        "baseline_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "evaluation_boundary": {
            "dialects": ["sqlite"],
            "raw_value_sampling": False,
            "semantic_review_auto_approval": False,
            "production_dataset_mappings": False,
        },
        "summary": _aggregate(results),
        "databases": results,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=dataset_cache.DEFAULT_MANIFEST)
    parser.add_argument(
        "--cache-root", type=Path, default=dataset_cache.DEFAULT_CACHE_ROOT
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("lang2sql-datasets/reports/public_onboarding_baseline.json"),
    )
    parser.add_argument("--concurrency", type=int, default=4)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    manifest = dataset_cache.load_manifest(args.manifest)
    report = asyncio.run(run_baseline(manifest, args.cache_root, args.concurrency))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(report["summary"], indent=2, sort_keys=True))
    print(f"report: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
