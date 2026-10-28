"""The engine: starts runs, delivers signals and cancels, and processes the three task kinds.

* **workflow** task: replay the run's history through the workflow function, persist the new
  commands it issued, and finish the run if it returned or raised.
* **activity** task: call the activity function (outside any transaction), then record the
  result, schedule a retry, or record the final failure.
* **timer** task: record that a durable timer (``ctx.sleep`` or a signal timeout) fired.

Activities run at least once: if a worker dies mid-activity its lease expires and another
worker runs it again. ``current_activity().idempotency_key`` is stable across those attempts.
"""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any

from .clock import Clock, SystemClock
from .context import RunInfo, WorkflowContext, drive
from .errors import (
    NonDeterminismError,
    NonRetryableError,
    RunConflict,
    RunFinished,
    RunNotFound,
)
from .events import Event, NewEvent
from .events import EventType as E
from .registry import Registry
from .retry import RetryPolicy
from .store import Run, Store, Task

log = logging.getLogger("durable")


@dataclass(frozen=True)
class ActivityInfo:
    run_id: str
    workflow: str
    activity: str
    cmd: int
    attempt: int

    @property
    def idempotency_key(self) -> str:
        """Same value for every attempt of this activity call; pass it to downstream APIs."""
        return f"{self.run_id}:{self.cmd}"


_current: contextvars.ContextVar[ActivityInfo] = contextvars.ContextVar("durable_activity")


def current_activity() -> ActivityInfo:
    """Inside an activity: which run, call and attempt this is."""
    try:
        return _current.get()
    except LookupError:
        raise RuntimeError("current_activity() called outside an activity") from None


class _VersionConflict(Exception):
    pass


class Engine:
    def __init__(self, store: Store, registry: Registry, clock: Clock | None = None) -> None:
        self.store = store
        self.registry = registry
        self.clock = clock or SystemClock()

    # ------------------------------------------------------------------ client API

    def start(self, workflow: str, input: Any = None, run_id: str | None = None) -> Run:
        """Start a run. Starting again with the same ``run_id`` returns the existing run."""
        self.registry.get_workflow(workflow)
        _ensure_json(input, "workflow input")
        run_id = run_id or uuid.uuid4().hex
        now = self.clock.now()
        with self.store.transaction() as conn:
            existing = self.store.get_run(conn, run_id)
            if existing is not None:
                if existing.workflow != workflow:
                    raise RunConflict(f"run {run_id!r} belongs to workflow {existing.workflow!r}")
                return existing
            self.store.insert_run(conn, run_id, workflow, input, now)
            self.store.append(
                conn, run_id, [NewEvent(E.WORKFLOW_STARTED, None, {"input": input})], now
            )
            self.store.enqueue_workflow_task(conn, run_id, now)
            return self.store.get_run(conn, run_id)  # type: ignore[return-value]

    def signal(self, run_id: str, name: str, payload: Any = None) -> None:
        _ensure_json(payload, "signal payload")
        self._append_external(
            run_id, NewEvent(E.SIGNAL_RECEIVED, None, {"name": name, "payload": payload})
        )

    def cancel(self, run_id: str) -> None:
        """Ask the run to stop; it sees ``WorkflowCancelled`` at its current await."""
        self._append_external(run_id, NewEvent(E.CANCEL_REQUESTED, None, {}), once=True)

    def get(self, run_id: str) -> Run:
        with self.store.transaction() as conn:
            run = self.store.get_run(conn, run_id)
        if run is None:
            raise RunNotFound(run_id)
        return run

    def history(self, run_id: str) -> list[Event]:
        with self.store.transaction() as conn:
            if self.store.get_run(conn, run_id) is None:
                raise RunNotFound(run_id)
            return self.store.history(conn, run_id)

    def list(self, status: str | None = None, workflow: str | None = None, limit: int = 50):
        with self.store.transaction() as conn:
            return self.store.list_runs(conn, status=status, workflow=workflow, limit=limit)

    def stats(self) -> dict[str, Any]:
        with self.store.transaction() as conn:
            return {
                "runs": self.store.status_counts(conn),
                "tasks": self.store.task_counts(conn),
            }

    def _append_external(self, run_id: str, event: NewEvent, once: bool = False) -> None:
        now = self.clock.now()
        with self.store.transaction() as conn:
            run = self.store.get_run(conn, run_id)
            if run is None:
                raise RunNotFound(run_id)
            if run.finished:
                raise RunFinished(f"run {run_id} is {run.status}")
            if once and any(e.type is event.type for e in self.store.history(conn, run_id)):
                return
            self.store.append(conn, run_id, [event], now)
            self.store.enqueue_workflow_task(conn, run_id, now)

    # ------------------------------------------------------------------ task processing

    def process(self, task: Task, owner: str) -> None:
        handler = {
            "workflow": self._workflow_task,
            "activity": self._activity_task,
            "timer": self._timer_task,
        }[task.kind]
        try:
            handler(task, owner)
        except _VersionConflict:
            # Someone appended to the run while we replayed it; replay again with that event.
            self.store.release(task, owner, due_at=self.clock.now())
        except Exception:
            log.exception(
                "task %s (%s) for run %s crashed; releasing", task.id, task.kind, task.run_id
            )
            self.store.release(task, owner, due_at=self.clock.now() + 1.0)

    def _workflow_task(self, task: Task, owner: str) -> None:
        now = self.clock.now()
        with self.store.transaction() as conn:
            run = self.store.get_run(conn, task.run_id)
            if run is None or run.finished:
                self.store.finish_task(conn, task, owner)
                return
            history = self.store.history(conn, run.id)
            definition = self.registry.get_workflow(run.workflow)
            ctx = WorkflowContext(RunInfo(run.id, run.workflow), history, self.registry, now)
            try:
                outcome = drive(definition.fn, ctx)
            except NonDeterminismError as exc:
                log.error("run %s: %s", run.id, exc)
                ctx.new_events.clear()
                outcome_status, error, error_type, result = (
                    "failed",
                    str(exc),
                    "NonDeterminismError",
                    None,
                )
            else:
                outcome_status, error, error_type, result = (
                    outcome.status,
                    outcome.error,
                    outcome.error_type,
                    outcome.result,
                )
                if outcome_status == "completed":
                    try:
                        _ensure_json(result, "workflow result")
                    except TypeError as exc:
                        outcome_status, error, error_type, result = (
                            "failed",
                            str(exc),
                            "TypeError",
                            None,
                        )

            new = list(ctx.new_events)
            terminal = {
                "completed": NewEvent(E.WORKFLOW_COMPLETED, None, {"result": result}),
                "failed": NewEvent(
                    E.WORKFLOW_FAILED, None, {"error": error, "error_type": error_type}
                ),
                "cancelled": NewEvent(E.WORKFLOW_CANCELLED, None, {"error": error}),
            }.get(outcome_status)
            if terminal is not None:
                new.append(terminal)

            if not self.store.finish_task(conn, task, owner):
                return  # lease lost; whoever holds it now will do this work
            if not self.store.append(conn, run.id, new, now, expected_version=run.version):
                raise _VersionConflict

            if terminal is not None:
                self.store.drop_tasks(conn, run.id)
                self.store.finish_run(conn, run.id, outcome_status, now, result, error, error_type)
                return
            for ev in new:
                if ev.type is E.ACTIVITY_SCHEDULED:
                    self.store.enqueue(conn, "activity", run.id, now, cmd=ev.cmd)
                elif ev.type is E.TIMER_STARTED or (
                    ev.type is E.SIGNAL_WAIT_STARTED and ev.data.get("fire_at") is not None
                ):
                    self.store.enqueue(conn, "timer", run.id, ev.data["fire_at"], cmd=ev.cmd)

    def _activity_task(self, task: Task, owner: str) -> None:
        with self.store.transaction() as conn:
            run = self.store.get_run(conn, task.run_id)
            scheduled = None
            if run is not None and not run.finished:
                scheduled = next(
                    (
                        e
                        for e in self.store.history(conn, run.id)
                        if e.cmd == task.cmd and e.type is E.ACTIVITY_SCHEDULED
                    ),
                    None,
                )
            if scheduled is None:
                self.store.finish_task(conn, task, owner)
                return

        name = scheduled.data["name"]
        retry = RetryPolicy.from_dict(scheduled.data["retry"])
        info = ActivityInfo(run.id, run.workflow, name, task.cmd, task.attempt)  # type: ignore[union-attr,arg-type]
        ok, result, error, error_type = self._call_activity(name, scheduled.data["args"], info)

        now = self.clock.now()
        with self.store.transaction() as conn:
            if not self.store.owns(conn, task, owner):
                log.warning(
                    "run %s: lease on %s attempt %s lost; result dropped",
                    run.id,
                    name,
                    task.attempt,
                )  # type: ignore[union-attr]
                return
            if ok:
                event = NewEvent(
                    E.ACTIVITY_COMPLETED, task.cmd, {"result": result, "attempt": task.attempt}
                )
                self.store.finish_task(conn, task, owner)
                self.store.append(conn, task.run_id, [event], now)
                self.store.enqueue_workflow_task(conn, task.run_id, now)
            elif error_type != NonRetryableError.__name__ and retry.should_retry(
                task.attempt, error_type
            ):
                delay = retry.delay(task.attempt)
                event = NewEvent(
                    E.ACTIVITY_ATTEMPT_FAILED,
                    task.cmd,
                    {
                        "error": error,
                        "error_type": error_type,
                        "attempt": task.attempt,
                        "retry_in": delay,
                    },
                )
                self.store.reschedule(conn, task, owner, now + delay, task.attempt + 1)
                self.store.append(conn, task.run_id, [event], now)
            else:
                event = NewEvent(
                    E.ACTIVITY_FAILED,
                    task.cmd,
                    {"error": error, "error_type": error_type, "attempts": task.attempt},
                )
                self.store.finish_task(conn, task, owner)
                self.store.append(conn, task.run_id, [event], now)
                self.store.enqueue_workflow_task(conn, task.run_id, now)

    def _call_activity(self, name: str, args: list, info: ActivityInfo):
        definition = self.registry.get_activity(name)
        if definition is None:
            return False, None, f"activity {name!r} is not registered", NonRetryableError.__name__
        token = _current.set(info)
        try:
            value = definition.fn(*args)
            if inspect.isawaitable(value):
                value = asyncio.run(_await(value))
        except Exception as exc:
            return False, None, str(exc) or type(exc).__name__, type(exc).__name__
        finally:
            _current.reset(token)
        try:
            _ensure_json(value, f"result of {name}")
        except TypeError as exc:  # retrying would produce the same unserialisable value
            return False, None, str(exc), NonRetryableError.__name__
        return True, value, None, None

    def _timer_task(self, task: Task, owner: str) -> None:
        now = self.clock.now()
        with self.store.transaction() as conn:
            run = self.store.get_run(conn, task.run_id)
            if not self.store.finish_task(conn, task, owner):
                return
            if run is None or run.finished:
                return
            self.store.append(conn, run.id, [NewEvent(E.TIMER_FIRED, task.cmd, {})], now)
            self.store.enqueue_workflow_task(conn, run.id, now)


async def _await(awaitable):
    return await awaitable


def _ensure_json(value: Any, what: str) -> None:
    try:
        json.dumps(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"{what} is not JSON-serialisable: {exc}") from None
