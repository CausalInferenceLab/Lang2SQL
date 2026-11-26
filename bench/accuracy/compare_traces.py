"""Compare completed, scored runs without invoking models, databases or graders."""
from __future__ import annotations

import argparse
import csv
import difflib
import html
import json
import logging
from pathlib import Path

from bench.accuracy.codex_mcp import _scored_cases
from bench.spider2.evaluate import digest, write_json

logger = logging.getLogger(__name__)
_PREVIEW_ROWS = 20


def _case_trace(root, ident, score):
    if Path(ident).name != ident:
        raise ValueError("Case IDs must be single path components")
    path = root / "codex-mcp/records" / (ident + ".json")
    record = json.loads(path.read_text())
    if record["instance_id"] != ident or (score == 1 and record["status"] != "predicted"):
        raise ValueError("Record identity/status disagrees with score")
    if record.get("events_sha256"):
        events = root / "codex-mcp/events" / (ident + ".jsonl")
        if digest(events) != record["events_sha256"]:
            raise ValueError("Recorded event hash mismatch")
    result = None
    if record["status"] == "predicted":
        csv_path = root / "codex-mcp/csv" / (ident + ".csv")
        with csv_path.open(newline="") as stream:
            reader = csv.reader(stream)
            columns = next(reader, [])
            preview, count = [], 0
            for row in reader:
                count += 1
                if count <= _PREVIEW_ROWS:
                    preview.append(row)
        if record.get("rows") is not None and record["rows"] != count:
            raise ValueError("CSV row count disagrees with prediction record")
        result = {"columns": columns, "row_count": count, "preview": preview,
                  "preview_truncated": count > _PREVIEW_ROWS, "csv_sha256": digest(csv_path)}
    return {"score": score, "record_sha256": digest(path), "record": record, "result": result}


def compare_runs(baseline, current):
    old = json.loads((baseline / "manifest.json").read_text())
    new = json.loads((current / "manifest.json").read_text())
    for key in ("suite", "evaluator_sha256", "question_file_sha256", "upstream_commit"):
        if old.get(key) != new.get(key):
            raise ValueError(f"Incompatible scoring inputs: {key}")
    _, old_scores = _scored_cases(old["cases"], baseline, old["db_sha256"])
    selected, new_scores = _scored_cases(old["cases"], current, old["db_sha256"])
    cases, transitions = [], {}
    for case in selected:
        ident = case["instance_id"]
        before = _case_trace(baseline, ident, old_scores[ident]["score"])
        after = _case_trace(current, ident, new_scores[ident]["score"])
        transition = f'{before["score"]} -> {after["score"]}'
        transitions[transition] = transitions.get(transition, 0) + 1
        diff = "\n".join(difflib.unified_diff(
            (before["record"].get("sql") or "").splitlines(),
            (after["record"].get("sql") or "").splitlines(),
            fromfile="before.sql", tofile="after.sql", lineterm="",
        ))
        cases.append({"input": case, "transition": transition,
                      "sql_diff": diff, "before": before, "after": after})
    logger.info("Compared completed traces baseline=%s current=%s cases=%d transitions=%s",
                baseline, current, len(cases), transitions)
    return {"scope": "Recorded score transitions on matched selected cases only; not full accuracy or proof of causality. CSV previews are text, not native SQL types. No semantic failure cause is inferred automatically.",
            "sources": [{"path": str(root), "manifest_sha256": digest(root / "manifest.json"),
                         "scores_sha256": digest(root / "codex-mcp/scores.json"),
                         "settings": json.loads((root / "settings.json").read_text())}
                        for root in (baseline, current)],
            "transitions": transitions, "cases": cases}


def render_html(report):
    def pre(value):
        text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, indent=2)
        return "<pre>" + html.escape(text) + "</pre>"

    cards = []
    for item in report["cases"]:
        ident = html.escape(item["input"]["instance_id"], quote=True)
        columns = []
        for side in ("before", "after"):
            trace = item[side]
            record = trace["record"]
            columns.append(f'<section><h3>{side}: score {trace["score"]}</h3>'
                           + pre({k: record.get(k) for k in ("status", "error_type", "error_message", "seconds")})
                           + "<h4>Final SQL / answer</h4>" + pre(record.get("sql") or record.get("answer", ""))
                           + f"<h4>Result preview (up to {_PREVIEW_ROWS} rows; full row count below)</h4>"
                           + pre(trace["result"])
                           + "<details><summary>Every MCP call: arguments and results</summary>"
                           + pre(record.get("mcp_calls", [])) + "</details></section>")
        cards.append(f'<details id="{ident}"><summary>{ident}: {html.escape(item["transition"])}</summary>'
                     + pre(item["input"]) + "<h3>SQL diff</h3>" + pre(item["sql_diff"] or "No SQL text change")
                     + '<div class="pair">' + "".join(columns) + "</div></details>")
    return ('<!doctype html><html lang="en"><meta charset="utf-8">'
            '<meta http-equiv="Content-Security-Policy" content="default-src \'none\'; style-src \'unsafe-inline\'">'
            '<meta name="viewport" content="width=device-width,initial-scale=1">'
            '<title>Lang2SQL trace comparison</title><style>'
            'body{font:16px system-ui;margin:24px;color:#172033;background:#f5f7fb}'
            '.pair{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,420px),1fr));gap:16px}'
            'section,details{padding:12px;background:white;border:1px solid #ccd4df;border-radius:6px;margin:8px 0}'
            'summary{cursor:pointer;font-weight:600}pre{white-space:pre-wrap;overflow-wrap:anywhere;font-size:13px}'
            '</style><body><h1>Lang2SQL trace comparison</h1>'
            + pre(report["scope"]) + pre(report["transitions"])
            + '<details><summary>Provenance and settings</summary>' + pre(report["sources"]) + '</details>'
            + "".join(cards) + '</body></html>')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for arg in ("baseline", "current", "output"):
        parser.add_argument("--" + arg, type=Path, required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    if args.output.exists():
        raise ValueError("Use a fresh report directory; never overwrite evidence")
    report = compare_runs(args.baseline, args.current)
    rendered = render_html(report)
    args.output.mkdir(parents=True)
    write_json(args.output / "comparison.json", report)
    (args.output / "index.html").write_text(rendered)
    logger.info("Trace comparison written output=%s", args.output)


if __name__ == "__main__":
    main()
