"""Checkpoint published terminal records without rerunning or altering inference."""

import json
from pathlib import Path

import pytest

from bench.accuracy.snapshot import snapshot_completed
from bench.spider2.evaluate import digest, write_json


def source_run(root):
    source = root / "source"
    write_json(
        source / "manifest.json",
        {
            "suite": "bird",
            "cases": [
                {"instance_id": i, "db": "db", "question": i} for i in ["a", "b", "c"]
            ],
            "db_sha256": {"db": "hash"},
            "development_ids": ["a", "b", "c"],
        },
    )
    write_json(
        source / "settings.json",
        {"profiles": [{"name": "codex-mcp"}], "revision": "frozen"},
    )
    for part in ["records", "events", "sql", "csv"]:
        (source / "codex-mcp" / part).mkdir(parents=True, exist_ok=True)
    d = source / "codex-mcp"
    (d / "events/a.prompt.txt").write_text("original prompt")
    (d / "events/a.jsonl").write_text('{"type":"turn.completed"}\n')
    (d / "sql/a.sql").write_text("SELECT 1\n")
    (d / "csv/a.csv").write_bytes(b"value\r\n1\r\n")
    write_json(
        d / "records/a.json",
        {
            "instance_id": "a",
            "status": "predicted",
            "sql": "SELECT 1",
            "rows": 1,
            "prompt_sha256": digest(d / "events/a.prompt.txt"),
            "events_sha256": digest(d / "events/a.jsonl"),
        },
    )
    write_json(
        d / "records/b.json",
        {"instance_id": "b", "status": "failed", "error_type": "TimeoutError"},
    )
    (d / "events/c.jsonl").write_text("in progress")
    return source


def test_snapshot_preserves_failures_bytes_and_parent(tmp_path):
    source = source_run(tmp_path)
    before = {
        str(p.relative_to(source)): p.read_bytes()
        for p in source.rglob("*")
        if p.is_file()
    }
    out = tmp_path / "checkpoint"
    info = snapshot_completed(source, out)
    assert info["selected_ids"] == ["a", "b"] and info["source_case_count"] == 3
    assert (
        json.loads((out / "codex-mcp/records/b.json").read_text())["status"] == "failed"
    )
    assert (out / "codex-mcp/csv/a.csv").read_bytes() == b"value\r\n1\r\n"
    assert not (out / "codex-mcp/events/c.jsonl").exists()
    assert not (source / "codex-mcp/status.json").exists()
    assert before == {
        str(p.relative_to(source)): p.read_bytes()
        for p in source.rglob("*")
        if p.is_file()
    }
    assert json.loads((out / "manifest.json").read_text())["db_sha256"] == {
        "db": "hash"
    }
    with pytest.raises(ValueError, match="fresh"):
        snapshot_completed(source, out)


@pytest.mark.parametrize("damage", ["hash", "sql", "csv", "id", "metadata"])
def test_corruption_never_publishes_subset(tmp_path, damage):
    source = source_run(tmp_path)
    d = source / "codex-mcp"
    if damage == "hash":
        (d / "events/a.prompt.txt").write_text("changed")
    if damage == "sql":
        (d / "sql/a.sql").write_text("SELECT 2")
    if damage == "csv":
        (d / "csv/a.csv").write_text("value\n")
    if damage == "id":
        p = d / "records/a.json"
        r = json.loads(p.read_text())
        r["instance_id"] = "other"
        write_json(p, r)
    if damage == "metadata":
        write_json(source / "settings.json", {"profiles": [{"name": "api"}]})
    out = tmp_path / "checkpoint"
    with pytest.raises(ValueError):
        snapshot_completed(source, out)
    assert not out.exists()


def test_unfinished_only_and_in_source_destination_rejected(tmp_path):
    source = source_run(tmp_path)
    for p in (source / "codex-mcp/records").glob("*.json"):
        p.unlink()
    write_json(
        source / "codex-mcp/records/c.json", {"instance_id": "c", "status": "started"}
    )
    with pytest.raises(ValueError, match="finalized"):
        snapshot_completed(source, tmp_path / "empty")
    assert not (tmp_path / "empty").exists()
    with pytest.raises(ValueError, match="outside"):
        snapshot_completed(source, source / "snapshot")


def test_concurrent_copy_mutation_rejected(tmp_path, monkeypatch):
    import bench.accuracy.snapshot as module

    source = source_run(tmp_path)
    copy = module.shutil.copyfile

    def changing_copy(src, dst):
        result = copy(src, dst)
        if Path(src).name == "a.sql":
            Path(src).write_text("SELECT 2")
        return result

    monkeypatch.setattr(module.shutil, "copyfile", changing_copy)
    with pytest.raises(ValueError, match="changed"):
        snapshot_completed(source, tmp_path / "checkpoint")
    assert not (tmp_path / "checkpoint").exists()


def test_metadata_change_after_read_rejected(tmp_path, monkeypatch):
    import bench.accuracy.snapshot as module

    source = source_run(tmp_path)
    loads = module.json.loads
    changed = False

    def mutate_after_parse(value):
        nonlocal changed
        result = loads(value)
        if not changed:
            changed = True
            altered = loads(value)
            altered["cases"][0]["question"] = "different input"
            write_json(source / "manifest.json", altered)
        return result

    monkeypatch.setattr(module.json, "loads", mutate_after_parse)
    with pytest.raises(ValueError, match="metadata changed"):
        snapshot_completed(source, tmp_path / "checkpoint")
    assert not (tmp_path / "checkpoint").exists()
