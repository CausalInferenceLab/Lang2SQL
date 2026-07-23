from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


def test_semantic_runtime_quickstart_is_offline_and_cleans_temp_files(
    tmp_path: Path,
) -> None:
    repository = Path(__file__).parents[1]
    temporary_root = tmp_path / "temporary"
    temporary_root.mkdir()
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("LANG2SQL_")
        and key not in {"OPENAI_API_KEY", "DISCORD_BOT_TOKEN"}
    } | {
        "PYTHONPATH": str(repository / "src"),
        "TMPDIR": str(temporary_root),
    }

    completed = subprocess.run(
        [sys.executable, "examples/semantic_runtime_quickstart.py"],
        cwd=repository,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.stdout == "SUCCESS total_paid_amount=30\n"
    assert completed.stderr == ""
    assert list(temporary_root.iterdir()) == []
