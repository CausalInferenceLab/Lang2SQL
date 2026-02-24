import pytest

from lang2sql.core.base import BaseComponent, BaseFlow
from lang2sql.core.hooks import MemoryHook
from lang2sql.core.exceptions import (
    ComponentError,
    ValidationError,
    IntegrationMissingError,
)


# -------------------------
# Fixtures: tiny components/flows
# -------------------------


class AddOne(BaseComponent):
    def _run(self, x: int) -> int:
        return x + 1


class BoomValueError(BaseComponent):
    def _run(self, x: int) -> int:
        raise ValueError("boom")


class BoomDomainError(BaseComponent):
    def _run(self, x: int) -> int:
        raise ValidationError("bad sql")


class BoomIntegrationMissing(BaseComponent):
    def _run(self, x: int) -> int:
        raise IntegrationMissingError("faiss", extra="faiss")


class FlowOk(BaseFlow):
    def _run(self, x: int) -> int:
        return x * 2


class FlowBoomDomain(BaseFlow):
    def _run(self, x: int) -> int:
        raise ValidationError("flow bad")


class FlowBoomUnknown(BaseFlow):
    def _run(self, x: int) -> int:
        raise RuntimeError("flow boom")


# -------------------------
# BaseComponent tests
# -------------------------


def test_base_component_emits_start_end_events():
    hook = MemoryHook()
    c = AddOne(hook=hook)

    out = c(1)
    assert out == 2

    assert len(hook.events) == 2
    assert hook.events[0].name == "component.run"
    assert hook.events[0].phase == "start"
    assert hook.events[1].name == "component.run"
    assert hook.events[1].phase == "end"
    assert hook.events[1].duration_ms is not None
    assert hook.events[1].duration_ms >= 0.0


def test_base_component_wraps_non_domain_exception_as_component_error():
    hook = MemoryHook()
    c = BoomValueError(hook=hook)

    with pytest.raises(ComponentError) as ei:
        c(1)

    # error chain preserved
    assert isinstance(ei.value.cause, ValueError)
    assert "ValueError" in str(ei.value) or "boom" in str(ei.value)

    # events: start + error
    assert len(hook.events) == 2
    assert hook.events[0].phase == "start"
    assert hook.events[1].phase == "error"
    assert "ValueError" in (hook.events[1].error or "")
    assert "boom" in (hook.events[1].error or "")


def test_base_component_preserves_domain_error_validationerror():
    hook = MemoryHook()
    c = BoomDomainError(hook=hook)

    with pytest.raises(ValidationError) as ei:
        c(1)

    assert "bad sql" in str(ei.value)

    # events: start + error
    assert len(hook.events) == 2
    assert hook.events[0].phase == "start"
    assert hook.events[1].phase == "error"
    assert "ValidationError" in (hook.events[1].error or "")
    assert "bad sql" in (hook.events[1].error or "")


def test_base_component_preserves_domain_error_integration_missing():
    hook = MemoryHook()
    c = BoomIntegrationMissing(hook=hook)

    with pytest.raises(IntegrationMissingError) as ei:
        c(1)

    msg = str(ei.value)
    assert "Missing optional integration: faiss" in msg
    assert "lang2sql[faiss]" in msg

    # events: start + error
    assert len(hook.events) == 2
    assert hook.events[0].phase == "start"
    assert hook.events[1].phase == "error"
    assert "IntegrationMissingError" in (hook.events[1].error or "")
    assert "faiss" in (hook.events[1].error or "")


# -------------------------
# BaseFlow tests
# -------------------------


def test_base_flow_emits_start_end_events():
    hook = MemoryHook()
    f = FlowOk(hook=hook)

    out = f(3)
    assert out == 6

    assert len(hook.events) == 2
    assert hook.events[0].name == "flow.run"
    assert hook.events[0].phase == "start"
    assert hook.events[1].name == "flow.run"
    assert hook.events[1].phase == "end"
    assert hook.events[1].duration_ms is not None
    assert hook.events[1].duration_ms >= 0.0


def test_base_flow_preserves_domain_error():
    hook = MemoryHook()
    f = FlowBoomDomain(hook=hook)

    with pytest.raises(ValidationError) as ei:
        f(1)

    assert "flow bad" in str(ei.value)

    assert len(hook.events) == 2
    assert hook.events[0].phase == "start"
    assert hook.events[1].phase == "error"
    assert "ValidationError" in (hook.events[1].error or "")
    assert "flow bad" in (hook.events[1].error or "")


def test_base_flow_raises_unknown_error_and_emits_error_event():
    hook = MemoryHook()
    f = FlowBoomUnknown(hook=hook)

    with pytest.raises(RuntimeError) as ei:
        f(1)

    assert "flow boom" in str(ei.value)

    assert len(hook.events) == 2
    assert hook.events[0].phase == "start"
    assert hook.events[1].phase == "error"
    assert "RuntimeError" in (hook.events[1].error or "")
    assert "flow boom" in (hook.events[1].error or "")
