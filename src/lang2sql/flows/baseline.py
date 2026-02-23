from __future__ import annotations

from typing import Any, Callable, Sequence

from ..core.base import BaseFlow


class SequentialFlow(BaseFlow):
    """
    A minimal sequential pipeline runner (define-by-run style).

    This flow runs `steps` in order, passing the output of each step
    as the input to the next. Each step is a plain callable — no shared
    state bag is required.

    Args:
        steps: Ordered sequence of callables.
        name: Optional name override for tracing/logging.
        hook: Optional TraceHook. If not provided, a NullHook is used by BaseFlow.

    Returns:
        The final value after running all steps.
    """

    def __init__(
        self,
        *,
        steps: Sequence[Callable[..., Any]],
        name: str | None = None,
        hook=None,
    ) -> None:
        """
        Initialize the flow with an ordered list of steps.

        Args:
            steps: Ordered sequence of callables. Must be non-empty.
            name: Optional name override.
            hook: Optional TraceHook used by BaseFlow for flow-level events.

        Raises:
            ValueError: If `steps` is empty.
        """
        super().__init__(name=name or "SequentialFlow", hook=hook)
        if not steps:
            raise ValueError("SequentialFlow requires at least one step.")
        self.steps: list[Callable[..., Any]] = list(steps)

    def run(self, value: Any) -> Any:
        """
        Execute the flow by passing `value` through each step in order.

        Args:
            value: The initial input value.

        Returns:
            The final value after running all configured steps.
        """
        for step in self.steps:
            value = step(value)
        return value


def BaselineFlow(*args, **kwargs):
    """
    Deprecated alias for SequentialFlow.

    .. deprecated::
        Use :class:`SequentialFlow` instead.
    """
    import warnings

    warnings.warn(
        "BaselineFlow is deprecated. Use SequentialFlow instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return SequentialFlow(*args, **kwargs)
