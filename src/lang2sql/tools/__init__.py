"""Harness tools — wrap data operations for the agent loop."""

from __future__ import annotations

from typing import Any

from .ask_user import AskUser
from .define_dimension import DefineDimension
from .define_metric import DefineMetric
from .explain_query import ExplainQuery
from .explore_schema import ExploreSchema
from .profile_table import ProfileTable
from .run_code import RunCode
from .run_sql import RunSQL
from .search_semantic import SearchSemantic
from .show_plan import ShowPlan
from .visualize import Visualize
from .write_code import WriteCode
from .write_sql import WriteSQLTool

__all__ = [
    "AskUser",
    "DefineDimension",
    "DefineMetric",
    "ExplainQuery",
    "ExploreSchema",
    "ProfileTable",
    "RunCode",
    "RunSQL",
    "SearchSemantic",
    "ShowPlan",
    "Visualize",
    "WriteCode",
    "WriteSQLTool",
    "build_default_tools",
]


def build_default_tools(
    *,
    explorer: Any = None,
    layer: Any = None,
    composer: Any = None,
    llm: Any = None,
) -> list[Any]:
    """Build the default set of tools from available components.

    Parameters
    ----------
    explorer:
        ``DBExplorerPort`` instance — enables ExploreSchema, RunSQL,
        ProfileTable.
    layer:
        ``SemanticLayer`` instance — enables SearchSemantic,
        DefineMetric, DefineDimension.
    composer:
        ``SQLComposer`` instance — enables WriteSQLTool.
    llm:
        ``LLMPort`` instance — enables ExplainQuery.
    """
    tools: list[Any] = [AskUser(), ShowPlan()]

    if explorer:
        tools.extend([
            ExploreSchema(explorer),
            RunSQL(explorer),
            ProfileTable(explorer),
        ])

    if layer:
        tools.extend([
            SearchSemantic(layer),
            DefineMetric(layer),
            DefineDimension(layer),
        ])

    if composer:
        tools.append(WriteSQLTool(composer))

    if llm:
        tools.append(ExplainQuery(llm))
        tools.append(WriteCode(llm))

    tools.append(RunCode())
    tools.append(Visualize())
    return tools
