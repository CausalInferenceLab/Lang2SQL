"""RunCode tool — execute Python code in a sandboxed subprocess."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from typing import Any

from ..harness.tool import ToolSpec
from ..harness.types import ToolResult


class RunCode:
    """Execute Python code in a sandboxed subprocess.

    Safety measures:
    - Runs in a subprocess with timeout
    - Captures stdout/stderr
    """

    def __init__(self, timeout: int = 30) -> None:
        self._timeout = timeout

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="run_code",
            description=(
                "Execute Python code safely. The code runs in a sandboxed "
                "subprocess with a timeout. Use after write_code to run "
                "generated analysis code."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "code": {
                        "type": "string",
                        "description": "Python code to execute",
                    },
                    "input_data_json": {
                        "type": "string",
                        "description": (
                            "JSON string of input data "
                            "(will be loaded as pandas DataFrame)"
                        ),
                    },
                },
                "required": ["code"],
            },
        )

    def execute(
        self, code: str, input_data_json: str = "", **_: Any
    ) -> ToolResult:
        # Build the full script
        script_parts = ["import sys, json"]
        if input_data_json:
            script_parts.append("import pandas as pd")
            # Use a raw string variable to avoid quoting issues
            script_parts.append(
                f"df = pd.read_json('''{input_data_json}''')"
            )

        script_parts.append("")
        script_parts.append(code)

        full_script = "\n".join(script_parts)

        # Write to temp file and execute
        fd, script_path = tempfile.mkstemp(suffix=".py", text=True)
        try:
            with os.fdopen(fd, "w") as f:
                f.write(full_script)

            result = subprocess.run(  # noqa: S603
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                timeout=self._timeout,
                cwd=tempfile.gettempdir(),
            )

            output = result.stdout
            if result.stderr:
                output += "\n--- stderr ---\n" + result.stderr

            if result.returncode != 0:
                return ToolResult(
                    tool_call_id="",
                    content=(
                        f"Code execution failed "
                        f"(exit code {result.returncode}):\n{output}"
                    ),
                    is_error=True,
                )

            return ToolResult(
                tool_call_id="",
                content=output if output else "(no output)",
                data={
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                    "returncode": result.returncode,
                },
            )
        except subprocess.TimeoutExpired:
            return ToolResult(
                tool_call_id="",
                content=(
                    f"Code execution timed out after {self._timeout}s"
                ),
                is_error=True,
            )
        finally:
            try:
                os.unlink(script_path)
            except OSError:
                pass
