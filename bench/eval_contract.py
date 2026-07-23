"""Immutable, benchmark-only semantic evaluation case contract."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re
from typing import Any, Mapping

_EXPECTED_STATES = {"ready", "review_then_ready", "clarification", "blocked"}
_SPLITS = {"dev", "holdout"}


class EvalContractError(ValueError):
    """A case is incomplete, mutable-looking, or internally inconsistent."""


@dataclass(frozen=True)
class EvalCase:
    case_id: str
    dataset_id: str
    dataset_version: str
    source_sha256: str
    license: str
    db_id: str
    domain: str
    topology_family: str
    dialect: str
    split: str
    question: str
    gold_operators: tuple[str, ...]
    expected_state: str
    gold_semantic_plan_json: str
    safety_tags: tuple[str, ...]
    oracle_sql: str = ""
    source_question_id: str = ""
    adversarial_semantic_call_json: str = ""

    @property
    def gold_semantic_plan(self) -> Mapping[str, Any]:
        value = json.loads(self.gold_semantic_plan_json)
        assert isinstance(value, dict)
        return value

    @property
    def adversarial_semantic_call(self) -> Mapping[str, Any]:
        if not self.adversarial_semantic_call_json:
            return {}
        value = json.loads(self.adversarial_semantic_call_json)
        assert isinstance(value, dict)
        return value

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "EvalCase":
        required_text = (
            "case_id",
            "dataset_id",
            "dataset_version",
            "source_sha256",
            "license",
            "db_id",
            "domain",
            "topology_family",
            "dialect",
            "split",
            "question",
            "expected_state",
        )
        values: dict[str, str] = {}
        for field in required_text:
            value = raw.get(field)
            if not isinstance(value, str) or not value.strip():
                raise EvalContractError(
                    f"{raw.get('case_id', '<unknown>')}.{field} is required"
                )
            values[field] = value
        if not re.fullmatch(r"[0-9a-f]{64}", values["source_sha256"]):
            raise EvalContractError(f"{values['case_id']}.source_sha256 is invalid")
        if values["dialect"] != "sqlite":
            raise EvalContractError(
                f"{values['case_id']} exceeds the SQLite evaluation boundary"
            )
        if values["split"] not in _SPLITS:
            raise EvalContractError(f"{values['case_id']}.split is invalid")
        if values["expected_state"] not in _EXPECTED_STATES:
            raise EvalContractError(f"{values['case_id']}.expected_state is invalid")
        operators = raw.get("gold_operators")
        if (
            not isinstance(operators, list)
            or not operators
            or not all(isinstance(item, str) and item for item in operators)
        ):
            raise EvalContractError(f"{values['case_id']}.gold_operators is required")
        plan = raw.get("gold_semantic_plan")
        if not isinstance(plan, dict) or not plan:
            raise EvalContractError(
                f"{values['case_id']}.gold_semantic_plan is required"
            )
        safety_tags = raw.get("safety_tags")
        if not isinstance(safety_tags, list) or not all(
            isinstance(item, str) and item for item in safety_tags
        ):
            raise EvalContractError(f"{values['case_id']}.safety_tags must be a list")
        adversarial_call = raw.get("adversarial_semantic_call", {})
        if not isinstance(adversarial_call, dict):
            raise EvalContractError(
                f"{values['case_id']}.adversarial_semantic_call must be an object"
            )
        if values["expected_state"] == "blocked":
            required_call_fields = {
                "metric_id",
                "metric_phrase",
                "aggregate",
                "dimensions",
                "unresolved_obligations",
            }
            if not required_call_fields.issubset(adversarial_call):
                raise EvalContractError(
                    f"{values['case_id']}.adversarial_semantic_call is required "
                    "for an executable blocked-case check"
                )
        return cls(
            **values,
            gold_operators=tuple(operators),
            gold_semantic_plan_json=json.dumps(
                plan, sort_keys=True, separators=(",", ":")
            ),
            safety_tags=tuple(safety_tags),
            oracle_sql=str(raw.get("oracle_sql", "")),
            source_question_id=str(raw.get("source_question_id", "")),
            adversarial_semantic_call_json=(
                json.dumps(adversarial_call, sort_keys=True, separators=(",", ":"))
                if adversarial_call
                else ""
            ),
        )


def load_cases(path: Path) -> tuple[EvalCase, ...]:
    cases: list[EvalCase] = []
    try:
        with path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise EvalContractError(f"{path}:{line_number}: {exc}") from exc
                if not isinstance(raw, dict):
                    raise EvalContractError(
                        f"{path}:{line_number}: case must be an object"
                    )
                cases.append(EvalCase.from_mapping(raw))
    except OSError as exc:
        raise EvalContractError(f"cannot load cases {path}: {exc}") from exc
    if not cases:
        raise EvalContractError(f"no cases in {path}")
    case_ids = [case.case_id for case in cases]
    if len(case_ids) != len(set(case_ids)):
        raise EvalContractError(f"duplicate case_id in {path}")
    return tuple(cases)
