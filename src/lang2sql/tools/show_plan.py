"""ShowPlan tool — harness-intercepted tool for plan approval."""

from __future__ import annotations

from ..harness.tool import ToolSpec
from ..harness.types import ToolResult


class ShowPlan:
    """Present an analysis plan for user approval before executing.

    The harness loop intercepts this tool and yields a ``PlanApprovalEvent``
    instead of executing it.
    """

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="show_plan",
            description=(
                "Present a step-by-step analysis plan to the user for "
                "approval. Use this for complex analyses that involve "
                "multiple queries, Python code, or multi-step "
                "investigations. The user can approve, modify, or reject "
                "the plan."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "plan": {
                        "type": "string",
                        "description": "Markdown-formatted analysis plan",
                    },
                    "steps": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of step descriptions",
                    },
                },
                "required": ["plan"],
            },
        )

    def execute(self, **kwargs: object) -> ToolResult:
        return ToolResult(
            tool_call_id="", content="(handled by harness loop)"
        )
