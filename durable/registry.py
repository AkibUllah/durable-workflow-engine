"""Registering workflows and activities by name."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .errors import WorkflowNotFound
from .retry import RetryPolicy


@dataclass(frozen=True)
class ActivityDef:
    name: str
    fn: Callable[..., Any]
    retry: RetryPolicy | None


@dataclass(frozen=True)
class WorkflowDef:
    name: str
    fn: Callable[..., Any]
    description: str


class Registry:
    def __init__(self) -> None:
        self._workflows: dict[str, WorkflowDef] = {}
        self._activities: dict[str, ActivityDef] = {}

    def workflow(self, fn: Callable | None = None, *, name: str | None = None):
        """Register ``async def wf(ctx, input)`` as a workflow."""

        def register(func: Callable) -> Callable:
            if not inspect.iscoroutinefunction(func):
                raise TypeError(f"workflow {func.__name__!r} must be an async function")
            wf_name = name or func.__name__
            if wf_name in self._workflows:
                raise ValueError(f"workflow {wf_name!r} is already registered")
            doc = inspect.getdoc(func) or ""
            self._workflows[wf_name] = WorkflowDef(wf_name, func, doc.split("\n\n")[0])
            func.__durable_workflow__ = wf_name  # type: ignore[attr-defined]
            return func

        return register(fn) if fn is not None else register

    def activity(
        self,
        fn: Callable | None = None,
        *,
        name: str | None = None,
        retry: RetryPolicy | None = None,
    ):
        """Register a plain function as an activity. It stays directly callable."""

        def register(func: Callable) -> Callable:
            act_name = name or func.__name__
            if act_name in self._activities:
                raise ValueError(f"activity {act_name!r} is already registered")
            definition = ActivityDef(act_name, func, retry)
            self._activities[act_name] = definition
            func.__durable_activity__ = definition  # type: ignore[attr-defined]
            return func

        return register(fn) if fn is not None else register

    def get_workflow(self, name: str) -> WorkflowDef:
        try:
            return self._workflows[name]
        except KeyError:
            raise WorkflowNotFound(f"workflow {name!r} is not registered") from None

    def get_activity(self, name: str) -> ActivityDef | None:
        return self._activities.get(name)

    def workflows(self) -> list[WorkflowDef]:
        return sorted(self._workflows.values(), key=lambda w: w.name)

    def activities(self) -> list[ActivityDef]:
        return sorted(self._activities.values(), key=lambda a: a.name)
