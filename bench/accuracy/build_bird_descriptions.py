"""Reproduce the separately pinned BIRD schema variant from official CSVs."""

import csv
import hashlib
import io
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
DESCRIPTION_MARKER = "\n\nOfficial column/value descriptions (2024 Dev release):\n"


def build_documents(directory, baseline):
    """Keep the baseline intact and append all verified column descriptions."""
    provenance = json.loads((directory / "provenance.json").read_text())
    tables = {db: {} for db in baseline}
    for name, expected in sorted(provenance["files"].items()):
        path = directory / name
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != expected:
            raise ValueError(f"Official description changed: {name}")
        # The official release mixes UTF-8 and Windows-1252 CSVs.
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            logger.warning("Using release CP1252 encoding file=%s error=%s: %s",
                           name, type(exc).__name__, exc)
            text = raw.decode("cp1252")
        rows = csv.DictReader(io.StringIO(text))
        tables[Path(name).parts[0]][path.stem] = [
            {key: row[key] for key in (
                "original_column_name", "column_name", "column_description",
                "data_format", "value_description",
            )}
            for row in rows if row["original_column_name"]
        ]
    return {
        db: schema + DESCRIPTION_MARKER
        + json.dumps(tables[db], ensure_ascii=False, indent=2)
        for db, schema in baseline.items()
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    root = Path(__file__).resolve().parent
    documents = build_documents(
        root / "bird-value-descriptions",
        json.loads((root / "bird-schema.json").read_text()),
    )
    (root / "bird-schema-with-values.json").write_text(
        json.dumps(documents, ensure_ascii=False, indent=2) + "\n"
    )
    logger.info("Built BIRD schema with official descriptions databases=%d", len(documents))
