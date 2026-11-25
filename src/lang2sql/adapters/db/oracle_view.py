"""Verify an explicitly approved Oracle reporting view using the querying account."""

import hashlib
import json
import re
from datetime import date

from sqlalchemy import text

_IDENTIFIER = re.compile(r"[A-Z][A-Z0-9_]{0,127}")


def identifier(value):
    if (
        not isinstance(value, str)
        or not _IDENTIFIER.fullmatch(value.upper())
        or not value.isascii()
    ):
        raise ValueError("Use an unquoted Oracle identifier")
    return value.upper()


def view_contract(raw):
    required = {"owner", "view", "amount", "period", "aggregation", "grain", "filters"}
    if not isinstance(raw, dict) or set(raw) != required:
        raise ValueError(
            "Confirm owner, view, amount, period, aggregation, grain and filters"
        )
    if (
        raw["aggregation"] != "SUM"
        or not isinstance(raw["grain"], str)
        or not raw["grain"].strip()
    ):
        raise ValueError("Confirm SUM and the business meaning of one row")
    filters = raw["filters"]
    if not isinstance(filters, dict) or not all(
        isinstance(v, str) and v for v in filters.values()
    ):
        raise ValueError("Confirm string equality filters, or {} for none")
    normalized = {identifier(k): v for k, v in filters.items()}
    if len(normalized) != len(filters):
        raise ValueError("Duplicate filter column")
    return {
        **raw,
        **{k: identifier(raw[k]) for k in ("owner", "view", "amount", "period")},
        "filters": normalized,
    }


def metric_query(contract, start, end):
    contract = view_contract(contract)
    lo, hi = (date.fromisoformat(value) for value in (start, end))
    if lo >= hi:
        raise ValueError("Expected inclusive start before exclusive end")
    source = contract["owner"] + "." + contract["view"]
    period = contract["period"]
    conditions = [f"{period} >= :period_start", f"{period} < :period_end"]
    params = {"period_start": lo, "period_end": hi}
    for i, (column, value) in enumerate(sorted(contract["filters"].items())):
        name = f"filter_{i}"
        conditions.append(f"{column} = :{name}")
        params[name] = value
    return (
        f'SELECT SUM({contract["amount"]}) AS metric_value FROM {source} WHERE '
        + " AND ".join(conditions),
        params,
    )


def inspect_or_query(engine, contract, stamp, sql, params):
    contract = view_contract(contract)
    obj = {"owner": contract["owner"], "name": contract["view"]}
    with engine.connect() as conn:

        def snapshot():
            identity = tuple(
                conn.execute(
                    text(
                        "SELECT SYS_CONTEXT('USERENV','DB_UNIQUE_NAME'), SYS_CONTEXT('USERENV','CON_NAME'), "
                        "SYS_CONTEXT('USERENV','SERVICE_NAME') FROM dual"
                    )
                ).one()
            )
            row = conn.execute(
                text(
                    "SELECT object_id, TO_CHAR(last_ddl_time,'YYYYMMDDHH24MISS') FROM all_objects "
                    "WHERE owner=:owner AND object_name=:name AND object_type='VIEW' AND status='VALID'"
                ),
                obj,
            ).one_or_none()
            if not row:
                raise ValueError(
                    "Approved view is unavailable to this account or invalid"
                )
            definition = conn.execute(
                text(
                    "SELECT text FROM all_views WHERE owner=:owner AND view_name=:name"
                ),
                obj,
            ).scalar_one()
            columns = dict(
                conn.execute(
                    text(
                        "SELECT column_name, data_type FROM all_tab_columns WHERE owner=:owner AND table_name=:name"
                    ),
                    obj,
                ).all()
            )
            if (
                columns.get(contract["amount"]) != "NUMBER"
                or columns.get(contract["period"]) != "DATE"
            ):
                raise ValueError("View requires a NUMBER amount and DATE period")
            if any(
                columns.get(c) not in ("CHAR", "VARCHAR2", "NCHAR", "NVARCHAR2")
                for c in contract["filters"]
            ):
                raise ValueError("Required text filter column missing or changed")
            route = [
                engine.url.host,
                engine.url.port,
                engine.url.database,
                sorted(engine.url.query.items()),
            ]
            components = [
                route,
                identity,
                tuple(row),
                definition,
                sorted(columns.items()),
            ]
            dependencies = conn.execute(
                text(
                    "SELECT DISTINCT referenced_owner, referenced_name, referenced_type "
                    "FROM all_dependencies WHERE referenced_type IN ('FUNCTION','PACKAGE','PROCEDURE') "
                    "START WITH owner=:owner AND name=:name AND type='VIEW' "
                    "CONNECT BY NOCYCLE PRIOR referenced_owner=owner AND PRIOR referenced_name=name AND (PRIOR referenced_type=type OR (PRIOR referenced_type='PACKAGE' AND type='PACKAGE BODY'))"
                ),
                obj,
            ).all()
            procedural = []
            for dep_owner, dep_name, dep_type in sorted(dependencies):
                if dep_owner == "SYS":
                    continue
                objects = conn.execute(
                    text(
                        "SELECT object_type, status, TO_CHAR(last_ddl_time,'YYYYMMDDHH24MISS') "
                        "FROM all_objects WHERE owner=:owner AND object_name=:name "
                        "AND object_type IN ('FUNCTION','PROCEDURE','PACKAGE','PACKAGE BODY') ORDER BY object_type"
                    ),
                    {"owner": dep_owner, "name": dep_name},
                ).all()
                source = conn.execute(
                    text(
                        "SELECT type,line,text FROM all_source WHERE owner=:owner AND name=:name ORDER BY type,line"
                    ),
                    {"owner": dep_owner, "name": dep_name},
                ).all()
                if dep_type == "PACKAGE" and (
                    not any(item[0] == "PACKAGE BODY" for item in objects)
                    or not any(item[0] == "PACKAGE BODY" for item in source)
                ):
                    raise ValueError(
                        "Package body metadata is not visible; use an account with visibility before binding"
                    )
                if (
                    not objects
                    or any(item[1] != "VALID" for item in objects)
                    or not source
                ):
                    raise ValueError(
                        "View function/package dependency is unavailable or invalid"
                    )
                procedural.append(
                    [
                        dep_owner,
                        dep_name,
                        dep_type,
                        [tuple(item) for item in objects],
                        [tuple(item) for item in source],
                    ]
                )
            if procedural:
                components.append(procedural)
            fingerprint = json.dumps(components)
            return hashlib.sha256(fingerprint.encode()).hexdigest()

        current = snapshot()
        if stamp is None:
            # Dictionary visibility alone does not imply the account can SELECT.
            conn.execute(
                text(
                    f'SELECT {contract["amount"]} FROM {obj["owner"]}.{obj["name"]} WHERE 1=0'
                )
            )
            return current
        if stamp != current:
            raise ValueError(
                "Database identity or approved view changed; reconfirm the binding"
            )
        rows = [dict(r) for r in conn.execute(text(sql), params).mappings()]
        if snapshot() != current:
            raise ValueError("View changed during query; discard result and reconfirm")
        return rows
