"""Freeze finalized Codex records for scoring without stopping a live run.

This creates a completed *subset*, never marks its parent run complete and never
invokes inference, SQL or a grader. Score the snapshot with the existing scorer.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import re
import shutil
import tempfile
from pathlib import Path

from bench.spider2.evaluate import digest, write_json

logger = logging.getLogger(__name__)


def snapshot_completed(source: Path, output: Path) -> dict:
    source, output = source.resolve(), output.resolve()
    if output.exists() or source in output.parents:
        raise ValueError("Use a fresh output directory outside the source run")
    manifest_bytes = (source / "manifest.json").read_bytes()
    settings_bytes = (source / "settings.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    settings = json.loads(settings_bytes)
    if [p["name"] for p in settings["profiles"]] != ["codex-mcp"]:
        raise ValueError("Snapshots support one codex-mcp profile only")
    cases = manifest["cases"]
    ids = [c["instance_id"] for c in cases]
    if len(set(ids)) != len(ids) or any(
        not isinstance(i, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", i) for i in ids
    ):
        raise ValueError("Case IDs must be unique safe filenames")
    directory = source / "codex-mcp"
    # The runner atomically publishes each record only in its finalizer. Freeze
    # this inventory once: records completed later belong to the next snapshot.
    present = {p.stem for p in (directory / "records").glob("*.json")}
    if present - set(ids):
        raise ValueError("Source contains records outside its manifest")
    selected = []
    hashes = {
        "manifest.json": hashlib.sha256(manifest_bytes).hexdigest(),
        "settings.json": hashlib.sha256(settings_bytes).hexdigest(),
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".codex-snapshot-", dir=output.parent
    ) as temp:
        stage = Path(temp) / "run"
        stage.mkdir()
        for case in cases:
            ident = case["instance_id"]
            if ident not in present:
                continue
            record_path = directory / "records" / (ident + ".json")
            record_hash = digest(record_path)
            record = json.loads(record_path.read_text())
            if record["instance_id"] != ident:
                raise ValueError(f"Record identity mismatch: {ident}")
            if record["status"] not in ("predicted", "failed"):
                logger.info(
                    "Skipping unfinished record id=%s status=%s",
                    ident,
                    record["status"],
                )
                continue
            paths = []
            for part in ("records", "events", "sql", "csv"):
                paths.extend((directory / part).glob(ident + ".*"))
            # Never publish a temporary writer file as benchmark evidence.
            paths = [p for p in paths if p.is_file() and p.suffix != ".tmp"]
            for path in paths:
                relative = path.relative_to(source)
                before = digest(path)
                target = stage / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path, target)
                if digest(target) != before or digest(path) != before:
                    raise ValueError(f"Artifact changed while copying: {relative}")
                hashes[str(relative)] = before
            copied = stage / "codex-mcp"
            if digest(copied / "records" / (ident + ".json")) != record_hash:
                raise ValueError(f"Record changed during snapshot: {ident}")
            for field, suffix in (
                ("events_sha256", "jsonl"),
                ("prompt_sha256", "prompt.txt"),
            ):
                if record.get(field):
                    path = copied / "events" / (ident + "." + suffix)
                    if not path.exists() or digest(path) != record[field]:
                        raise ValueError(f"Evidence hash mismatch: {ident} {field}")
            if record["status"] == "predicted":
                sql = copied / "sql" / (ident + ".sql")
                if not sql.exists() or sql.read_text().strip() != record["sql"].strip():
                    raise ValueError(f"Prediction SQL missing or inconsistent: {ident}")
                with (copied / "csv" / (ident + ".csv")).open(newline="") as stream:
                    rows = csv.reader(stream)
                    if (
                        next(rows, None) is None
                        or sum(1 for _ in rows) != record["rows"]
                    ):
                        raise ValueError(f"Prediction CSV row count mismatch: {ident}")
            selected.append(case)
        if not selected:
            raise ValueError("No finalized records to snapshot")
        # Metadata is frozen too; a concurrent mutation must not mix revisions.
        for filename in ("manifest.json", "settings.json"):
            if digest(source / filename) != hashes[filename]:
                raise ValueError(f"Source metadata changed: {filename}")
        info = {
            "source": str(source),
            "source_case_count": len(cases),
            "completed_case_count": len(selected),
            "selected_ids": [c["instance_id"] for c in selected],
            "source_sha256": hashes,
            "new_model_calls": 0,
            "scope": "Completed-record subset only; not full benchmark accuracy. Parent run unchanged.",
            "legacy_limit": "Prompt/event hashes verified where recorded; missing legacy hashes are not reconstructed.",
        }
        write_json(
            stage / "manifest.json",
            {
                **manifest,
                "cases": selected,
                "development_ids": info["selected_ids"],
                "holdout_ids": [],
            },
        )
        shutil.copyfile(source / "settings.json", stage / "settings.json")
        if digest(stage / "settings.json") != hashes["settings.json"]:
            raise ValueError("Source settings changed during snapshot")
        write_json(stage / "snapshot.json", info)
        write_json(
            stage / "codex-mcp/status.json",
            {
                "status": "completed",
                "kind": "completed_record_subset",
                "source_case_count": len(cases),
                "completed_case_count": len(selected),
            },
        )
        if output.exists():
            raise ValueError("Output appeared while snapshotting; refusing overwrite")
        stage.rename(output)
    logger.info(
        "Snapshot published source=%s output=%s cases=%d/%d",
        source,
        output,
        len(selected),
        len(cases),
    )
    return info


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    try:
        snapshot_completed(args.source, args.output)
    except (OSError, ValueError):
        logger.exception(
            "Snapshot failed source=%s output=%s", args.source, args.output
        )
        raise


if __name__ == "__main__":
    main()
