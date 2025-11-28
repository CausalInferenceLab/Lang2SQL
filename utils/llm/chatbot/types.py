"""
ChatBot 관련 데이터 타입 및 구조 정의
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, TypedDict, Annotated
import operator

from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages

ToolFn = Callable[[Dict[str, Any]], Any]


@dataclass
class Guideline:
    id: str
    description: str
    example_phrases: List[str]
    tools: Optional[List[ToolFn]] = None
    priority: int = 0


class ChatBotState(TypedDict):
    """
    챗봇 상태
    """

    messages: Annotated[Sequence[BaseMessage], add_messages]
    context: Dict[str, Any]
    selected_ids: List[str]

    table_schema_outputs: Annotated[List[Optional[Dict[str, Any]]], operator.add]
    glossary_outputs: Annotated[List[Optional[Dict[str, Any]]], operator.add]
    query_example_outputs: Annotated[List[Optional[Dict[str, Any]]], operator.add]
