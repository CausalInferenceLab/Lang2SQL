"""Tests for WriteCode and RunCode tools."""

from __future__ import annotations

from lang2sql.tools.run_code import RunCode
from lang2sql.tools.write_code import WriteCode


# ---------------------------------------------------------------------------
# FakeLLM
# ---------------------------------------------------------------------------

class FakeLLM:
    """Minimal LLMPort stub that returns scripted responses."""

    def __init__(self, response: str) -> None:
        self._response = response
        self.last_messages: list[dict] | None = None

    def invoke(self, messages: list[dict], **_) -> str:
        self.last_messages = messages
        return self._response


# ---------------------------------------------------------------------------
# WriteCode tests
# ---------------------------------------------------------------------------

class TestWriteCode:

    def test_generates_code_via_llm(self):
        llm = FakeLLM("result = df.describe()\nprint(result)")
        tool = WriteCode(llm)
        res = tool.execute(task="summarize data")

        assert not res.is_error
        assert "result = df.describe()" in res.content
        assert res.data["task"] == "summarize data"
        assert res.data["code"] == res.content

    def test_strips_markdown_fences(self):
        llm = FakeLLM("```python\nprint('hello')\n```")
        tool = WriteCode(llm)
        res = tool.execute(task="hello")

        assert not res.is_error
        assert res.content == "print('hello')"
        assert "```" not in res.content

    def test_strips_markdown_fences_without_closing(self):
        llm = FakeLLM("```python\nprint('hello')")
        tool = WriteCode(llm)
        res = tool.execute(task="hello")

        assert not res.is_error
        assert res.content == "print('hello')"

    def test_passes_data_description_and_libraries(self):
        llm = FakeLLM("result = 1")
        tool = WriteCode(llm)
        tool.execute(
            task="correlate",
            data_description="columns: x, y (float)",
            libraries=["scipy", "numpy"],
        )

        prompt = llm.last_messages[-1]["content"]
        assert "columns: x, y (float)" in prompt
        assert "scipy, numpy" in prompt

    def test_spec_metadata(self):
        llm = FakeLLM("")
        tool = WriteCode(llm)
        assert tool.spec.name == "write_code"
        assert "task" in tool.spec.input_schema["properties"]
        assert "task" in tool.spec.input_schema["required"]

    def test_llm_error_returns_error_result(self):
        class FailLLM:
            def invoke(self, messages, **_):
                raise RuntimeError("API down")

        tool = WriteCode(FailLLM())
        res = tool.execute(task="anything")

        assert res.is_error
        assert "RuntimeError" in res.content
        assert "API down" in res.content


# ---------------------------------------------------------------------------
# RunCode tests
# ---------------------------------------------------------------------------

class TestRunCode:

    def test_simple_print(self):
        tool = RunCode()
        res = tool.execute(code='print("hello world")')

        assert not res.is_error
        assert "hello world" in res.content
        assert res.data["returncode"] == 0

    def test_timeout(self):
        tool = RunCode(timeout=1)
        res = tool.execute(code="import time; time.sleep(100)")

        assert res.is_error
        assert "timed out" in res.content

    def test_syntax_error(self):
        tool = RunCode()
        res = tool.execute(code="def !!!invalid")

        assert res.is_error
        assert res.content.startswith("Code execution failed")

    def test_runtime_error(self):
        tool = RunCode()
        res = tool.execute(code='raise ValueError("boom")')

        assert res.is_error
        assert "boom" in res.content

    def test_with_input_data_json(self):
        tool = RunCode()
        data_json = '{"a": [1, 2, 3], "b": [4, 5, 6]}'
        code = "print(df.shape)\nprint(df.columns.tolist())"
        res = tool.execute(code=code, input_data_json=data_json)

        assert not res.is_error
        assert "(3, 2)" in res.content

    def test_no_output(self):
        tool = RunCode()
        res = tool.execute(code="x = 42")

        assert not res.is_error
        assert res.content == "(no output)"

    def test_spec_metadata(self):
        tool = RunCode()
        assert tool.spec.name == "run_code"
        assert "code" in tool.spec.input_schema["properties"]
        assert "code" in tool.spec.input_schema["required"]

    def test_stderr_included(self):
        tool = RunCode()
        res = tool.execute(
            code='import sys; print("warn", file=sys.stderr); print("ok")'
        )

        assert not res.is_error
        assert "ok" in res.content
        assert "warn" in res.content
