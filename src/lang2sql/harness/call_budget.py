"""One model-call allowance shared by the agent loop and its tools."""

from collections.abc import Sequence

from ..core.ports.llm import LLMPort, RequiredToolLLMPort
from ..core.types import Completion, Message, ToolSpec


class CallBudgetExhausted(RuntimeError):
    pass


class SchemaLookupUnavailable(RuntimeError):
    pass


class TurnModel:
    def __init__(self, model: LLMPort, allowance: int):
        self.model = model
        self.remaining = allowance

    def _charge(self) -> None:
        if self.remaining <= 0:
            raise CallBudgetExhausted
        self.remaining -= 1

    async def complete(
        self, messages: Sequence[Message], tools: Sequence[ToolSpec] = ()
    ) -> Completion:
        self._charge()
        return await self.model.complete(messages, tools)

    async def require_tool(
        self, messages: Sequence[Message], tools: Sequence[ToolSpec], name: str
    ) -> Completion:
        if not isinstance(self.model, RequiredToolLLMPort):
            raise SchemaLookupUnavailable(
                "the model adapter cannot require schema lookup"
            )
        self._charge()
        return await self.model.complete_with_required_tool(messages, tools, name)
