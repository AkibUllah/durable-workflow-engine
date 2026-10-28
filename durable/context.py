"""Workflow context and the replay driver.

A workflow task runs the workflow function from the top every time. Each ``ctx`` call is a
*command* numbered in call order. If the history already holds the outcome of command ``n``, the
call returns it immediately; if not, the command is recorded as new and the first ``await`` that
cannot be satisfied suspends the workflow. The driver then throws the coroutine away: the next
task replays the history again, one event further along.

This is why workflow code must be deterministic: the same history has to produce the same
sequence of commands. Anything non-deterministic (time, randomness, I/O) goes through an
activity or ``ctx.side_effect``, whose result is recorded once and replayed afterwards.
"""

from __future__ import annotations

import math
import uuid
from collections.abc import Callable, Generator, Iterable
from dataclasses import dataclass
from typing import Any

from .errors import (
    ActivityError,
    NonDeterminismError,
    SignalTimeout,
    WorkflowCancelled,
)
from .events import Event, NewEvent
from .events import EventType as E
from .registry import Registry
from .retry import RetryPolicy

_BLOCKED = object()  # what a pending command yields to the driver

# Events that open a command, and the payload field that names it.
_OPENING = {
    E.ACTIVITY_SCHEDULED,
    E.TIMER_STARTED,
    E.SIGNAL_WAIT_STARTED,
    E.SIDE_EFFECT,
}


class _History:
    """Index of one run's events for quick lookup during replay."""

    def __init__(self, events: Iterable[Event]) -> None:
        self.by_cmd: dict[int, list[Event]] = {}
        self.signals: dict[str, list[Event]] = {}
        self.cancel_requested = False
        self.cancel_delivered: dict[int, float] = {}  # cmd -> seq of the delivery event
        self.input: Any = None
        for event in events:
            if event.type is E.WORKFLOW_STARTED:
                self.input = event.data.get("input")
            elif event.type is E.SIGNAL_RECEIVED:
                self.signals.setdefault(event.data["name"], []).append(event)
            elif event.type is E.CANCEL_REQUESTED:
                self.cancel_requested = True
            elif event.type is E.CANCEL_DELIVERED:
                self.cancel_delivered[event.cmd] = event.seq  # type: ignore[index]
            if event.cmd is not None:
                self.by_cmd.setdefault(event.cmd, []).append(event)

    def find(self, cmd: int, kind: E, before: float = math.inf) -> Event | None:
        for event in self.by_cmd.get(cmd, ()):
            if event.type is kind and event.seq < before:
                return event
        return None


class _Command:
    """Awaitable handle for one command.

    ``poll(before)`` returns ``(done, value)`` or raises, looking only at events with
    ``seq < before``. The cutoff matters after a cancel: history recorded later than the cancel
    delivery must stay invisible to that ``await`` so replay reaches the same decision.
    """

    def __init__(
        self, ctx: WorkflowContext, cmd: int, poll: Callable[[float], tuple[bool, Any]]
    ) -> None:
        self._ctx = ctx
        self.cmd = cmd
        self._poll = poll
        self._settled = False
        self._value: Any = None

    def result(self, before: float = math.inf) -> tuple[bool, Any]:
        if self._settled:
            return True, self._value
        done, value = self._poll(before)
        if done:
            self._settled, self._value = True, value
        return done, value

    def __await__(self) -> Generator[Any, None, Any]:
        while True:
            if self.cmd in self._ctx._history.cancel_delivered:
                raise WorkflowCancelled(f"run {self._ctx.run_id} was cancelled")
            done, value = self.result()
            if done:
                return value
            self._ctx._blocked_on(self.cmd)
            yield _BLOCKED


class _Gather:
    def __init__(self, ctx: WorkflowContext, commands: list[_Command]) -> None:
        self._ctx = ctx
        self._commands = commands

    def __await__(self) -> Generator[Any, None, list[Any]]:
        delivered = self._ctx._history.cancel_delivered
        while True:
            cutoff = min(
                (delivered[c.cmd] for c in self._commands if c.cmd in delivered), default=math.inf
            )
            pending = None
            values = []
            for command in self._commands:
                done, value = command.result(cutoff)  # a failed command raises here, in order
                if not done and pending is None:
                    pending = command
                values.append(value)
            if cutoff != math.inf:
                raise WorkflowCancelled(f"run {self._ctx.run_id} was cancelled")
            if pending is None:
                return values
            self._ctx._blocked_on(pending.cmd)
            yield _BLOCKED


@dataclass(frozen=True)
class RunInfo:
    run_id: str
    workflow: str


class WorkflowContext:
    """The only door from workflow code to the outside world."""

    def __init__(
        self,
        run: RunInfo,
        history: Iterable[Event],
        registry: Registry,
        now: float,
    ) -> None:
        self.run_id = run.run_id
        self.workflow = run.workflow
        self._registry = registry
        self._history = _History(history)
        self._task_time = now
        self._next_cmd = 0
        self._signals_taken: dict[str, int] = {}
        self._closing = False
        self.new_events: list[NewEvent] = []

    # ------------------------------------------------------------------ plumbing

    @property
    def input(self) -> Any:
        return self._history.input

    def _open(self, kind: E, name: str, data: dict[str, Any]) -> tuple[int, Event | None]:
        if self._closing:
            raise RuntimeError("workflow commands cannot be issued while the task is closing")
        cmd = self._next_cmd
        self._next_cmd += 1
        recorded = next(
            (e for e in self._history.by_cmd.get(cmd, ()) if e.type in _OPENING),
            None,
        )
        if recorded is not None:
            if recorded.type is not kind or recorded.data.get("name") != name:
                raise NonDeterminismError(
                    f"command {cmd}: history has {recorded.type.value} "
                    f"{recorded.data.get('name')!r}, workflow code now issues {kind.value} {name!r}"
                )
            return cmd, recorded
        self.new_events.append(NewEvent(kind, cmd, {"name": name, **data}))
        return cmd, None

    def _blocked_on(self, cmd: int) -> None:
        """Called when an await cannot complete. Delivers a pending cancel request there."""
        history = self._history
        if history.cancel_requested and not history.cancel_delivered:
            history.cancel_delivered[cmd] = math.inf  # not persisted yet: sees everything
            self.new_events.append(NewEvent(E.CANCEL_DELIVERED, cmd, {}))
            raise WorkflowCancelled(f"run {self.run_id} was cancelled")

    # ------------------------------------------------------------------ commands

    def activity(
        self, activity: Callable | str, *args: Any, retry: RetryPolicy | None = None
    ) -> _Command:
        """Schedule an activity. Await the handle for its result (or an ``ActivityError``)."""
        if isinstance(activity, str):
            name = activity
        else:
            definition = getattr(activity, "__durable_activity__", None)
            if definition is None:
                raise TypeError(f"{activity!r} is not a registered activity")
            name = definition.name
        if retry is None:
            registered = self._registry.get_activity(name)
            retry = (registered.retry if registered else None) or RetryPolicy()
        cmd, _ = self._open(
            E.ACTIVITY_SCHEDULED, name, {"args": list(args), "retry": retry.to_dict()}
        )

        def poll(before: float) -> tuple[bool, Any]:
            completed = self._history.find(cmd, E.ACTIVITY_COMPLETED, before)
            if completed is not None:
                return True, completed.data.get("result")
            failed = self._history.find(cmd, E.ACTIVITY_FAILED, before)
            if failed is not None:
                raise ActivityError(
                    name, failed.data["error"], failed.data["error_type"], failed.data["attempts"]
                )
            return False, None

        return _Command(self, cmd, poll)

    def gather(self, *commands: _Command) -> _Gather:
        """Await several commands started together; results come back in argument order."""
        return _Gather(self, list(commands))

    def sleep(self, seconds: float) -> _Command:
        """A durable timer: survives worker restarts, fires once ``seconds`` have passed."""
        if seconds < 0:
            raise ValueError("cannot sleep a negative amount of time")
        cmd, _ = self._open(
            E.TIMER_STARTED, "sleep", {"seconds": seconds, "fire_at": self._task_time + seconds}
        )

        def poll(before: float) -> tuple[bool, Any]:
            return self._history.find(cmd, E.TIMER_FIRED, before) is not None, None

        return _Command(self, cmd, poll)

    def wait_signal(self, name: str, timeout: float | None = None) -> _Command:
        """Wait for the next ``name`` signal and return its payload.

        Signals sent before the wait are buffered. With ``timeout``, raises ``SignalTimeout`` if
        the timer fires first; "first" is decided by the order of events in history, so replay
        always reaches the same answer.
        """
        data: dict[str, Any] = {"timeout": timeout}
        if timeout is not None:
            data["fire_at"] = self._task_time + timeout
        cmd, _ = self._open(E.SIGNAL_WAIT_STARTED, name, data)

        def poll(before: float) -> tuple[bool, Any]:
            taken = self._signals_taken.get(name, 0)
            received = [e for e in self._history.signals.get(name, []) if e.seq < before]
            fired = self._history.find(cmd, E.TIMER_FIRED, before)
            if taken < len(received) and (fired is None or received[taken].seq < fired.seq):
                self._signals_taken[name] = taken + 1
                return True, received[taken].data.get("payload")
            if fired is not None:
                raise SignalTimeout(f"no {name!r} signal within {timeout}s")
            return False, None

        return _Command(self, cmd, poll)

    def side_effect(self, fn: Callable[[], Any], name: str = "side_effect") -> Any:
        """Run ``fn`` once, record its (JSON) result, and return the recorded value on replay."""
        cmd = self._next_cmd
        recorded = self._history.find(cmd, E.SIDE_EFFECT)
        if recorded is not None:
            self._open(E.SIDE_EFFECT, name, {})
            return recorded.data.get("value")
        value = fn()
        self._open(E.SIDE_EFFECT, name, {"value": value})
        return value

    def now(self) -> float:
        """Deterministic wall-clock time (recorded the first time this point is reached)."""
        return self.side_effect(lambda: self._task_time, name="now")

    def uuid4(self) -> str:
        return self.side_effect(lambda: str(uuid.uuid4()), name="uuid4")


# ---------------------------------------------------------------------------- driver


@dataclass(frozen=True)
class Outcome:
    status: str  # "blocked" | "completed" | "failed" | "cancelled"
    result: Any = None
    error: str | None = None
    error_type: str | None = None


def drive(fn: Callable[..., Any], ctx: WorkflowContext) -> Outcome:
    """Run the workflow until it finishes or blocks. Raises ``NonDeterminismError``."""
    coro = fn(ctx, ctx.input)
    try:
        yielded = coro.send(None)
    except StopIteration as stop:
        return Outcome("completed", result=stop.value)
    except NonDeterminismError:
        raise
    except WorkflowCancelled as exc:
        return Outcome("cancelled", error=str(exc), error_type="WorkflowCancelled")
    except Exception as exc:
        return Outcome("failed", error=str(exc), error_type=type(exc).__name__)

    produced = list(ctx.new_events)
    ctx._closing = True
    try:
        coro.close()
    except Exception:  # a ``finally`` in workflow code misbehaved while we unwound it
        pass
    ctx.new_events[:] = produced
    if yielded is not _BLOCKED:
        return Outcome(
            "failed",
            error="workflows may only await ctx commands (got a foreign awaitable)",
            error_type="WorkflowError",
        )
    return Outcome("blocked")
