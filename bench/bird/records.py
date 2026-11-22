"""Durable records for one run, keyed by question ID."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from bench.common.io import ProtocolError, read_json, write_json


@dataclass(frozen=True)
class CaseFiles:
    root: Path
    question_id: int
    condition: str
    profile: str
    settings_digest: str

    @property
    def record_path(self) -> Path:
        return self.root / "records" / f"{self.question_id}.json"

    @property
    def attempt_path(self) -> Path:
        return self.root / "attempts" / f"{self.question_id}.json"

    def identity(self) -> dict:
        return {
            "question_id": self.question_id,
            "condition": self.condition,
            "profile": self.profile,
            "settings_digest": self.settings_digest,
        }

    def _load(self, path: Path) -> dict:
        record = read_json(path)
        if any(record.get(key) != value for key, value in self.identity().items()):
            raise ProtocolError(f"Record belongs to another case or run: {path}")
        return record

    def read_result(self) -> dict:
        record = self._load(self.record_path)
        if record["status"] not in {"predicted", "failed"}:
            raise ProtocolError(f"Record is incomplete: {self.record_path}")
        return record

    def save_attempt(self, first_request: dict, calls: list[dict]) -> None:
        write_json(
            self.attempt_path,
            {**self.identity(), "first_request": first_request, "calls": calls},
        )

    def finish(self, record: dict) -> dict:
        result = {**record, **self.identity()}
        write_json(self.record_path, result)
        self.attempt_path.unlink(missing_ok=True)
        return result

    def resume(self) -> dict | None:
        if self.record_path.exists():
            return self.read_result()
        if self.attempt_path.exists():
            attempt = self._load(self.attempt_path)
            return self.finish(
                {
                    **attempt,
                    "status": "failed",
                    "failure": {"stage": "generation", "reason": "interrupted"},
                    "seconds": None,
                }
            )
        return None
