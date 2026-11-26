"""Opaque reasoning continuity without changing core message types or retry logic."""

from dataclasses import replace

import pytest

from bench.spider2.responses import ResponsesCodec
from lang2sql.core.types import Message, Role, ToolSpec


def test_response_items_survive_tool_roundtrip_but_not_prefix_edits():
    codec = ResponsesCodec(8)
    messages = [Message(Role.SYSTEM, "Read only"), Message(Role.USER, "Count rows")]
    tools = [ToolSpec("run_sql", "Query", {"type": "object"})]
    output = [
        {"type": "reasoning", "id": "r1", "summary": [], "encrypted_content": "opaque"},
        {
            "type": "function_call",
            "id": "fc1",
            "call_id": "call1",
            "name": "run_sql",
            "arguments": '{"sql":"SELECT COUNT(*) FROM t"}',
        },
    ]
    completion = codec.decode(
        {"output": output, "status": "completed"}, messages, tools
    )
    assert completion.content == "" and completion.tool_calls[0].id == "call1"
    assistant = Message(Role.ASSISTANT, completion.content, completion.tool_calls)
    history = [*messages, assistant, Message(Role.TOOL, "7", tool_call_id="call1")]
    encoded, spec = codec.encode(history, tools)
    assert encoded[2:4] == output
    assert encoded[4] == {
        "type": "function_call_output",
        "call_id": "call1",
        "output": "7",
    }
    assert spec[0]["name"] == "run_sql" and spec[0]["strict"] is False
    edited, _ = codec.encode(
        [replace(messages[0], content="Different scope"), *history[1:]], tools
    )
    assert not any(x.get("type") == "reasoning" for x in edited)
    changed_tools, _ = codec.encode(
        history, [replace(tools[0], description="Different tool")]
    )
    assert not any(x.get("type") == "reasoning" for x in changed_tools)


def test_auxiliary_completion_does_not_replace_main_history_and_cache_is_bounded():
    codec = ResponsesCodec(2)
    first = [Message(Role.USER, "Main")]
    raw = {
        "status": "completed",
        "output": [
            {
                "type": "message",
                "id": "m",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "answer"}],
            }
        ],
    }
    codec.decode(raw, first, [])
    codec.decode(raw, [Message(Role.USER, "Auxiliary")], [])
    encoded, _ = codec.encode([*first, Message(Role.ASSISTANT, "answer")], [])
    assert encoded[-1] == raw["output"][0]
    codec.decode(raw, [Message(Role.USER, "Third")], [])
    assert len(codec.outputs) == 2


@pytest.mark.asyncio
async def test_responses_path_uses_shared_measurement_budget(monkeypatch):
    litellm = pytest.importorskip("litellm")
    from bench.spider2.providers import MeasuredLLM

    requests = []

    async def api(**kwargs):
        requests.append(kwargs)

        class Response:
            def model_dump(self, **kwargs):
                assert kwargs == {"by_alias": True, "exclude_none": True}
                return {
                    "model": "test",
                    "status": "completed",
                    "usage": {"input_tokens": 10, "output_tokens": 2},
                    "output": [
                        {
                            "type": "message",
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": "done"}],
                        }
                    ],
                }

        return Response()

    monkeypatch.setattr(litellm, "aresponses", api)
    llm = MeasuredLLM(
        {
            "name": "test",
            "model": "openai/test",
            "api": "responses",
            "reasoning_effort": "max",
        },
        "not-a-key",
        1,
        1,
    )
    result = await llm.complete([Message(Role.USER, "hello")])
    assert result.content == "done" and len(llm.calls) == 1
    assert requests[0]["store"] is False
    assert requests[0]["include"] == ["reasoning.encrypted_content"]
    assert requests[0]["reasoning"] == {"effort": "max"}
    with pytest.raises(RuntimeError, match="budget"):
        await llm.complete([])
