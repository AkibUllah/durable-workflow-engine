"""History events. A run's history is append-only; the workflow's state is whatever replaying it
produces."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class EventType(str, Enum):
    WORKFLOW_STARTED = "workflow_started"
    WORKFLOW_COMPLETED = "workflow_completed"
    WORKFLOW_FAILED = "workflow_failed"
    WORKFLOW_CANCELLED = "workflow_cancelled"

    ACTIVITY_SCHEDULED = "activity_scheduled"
    ACTIVITY_ATTEMPT_FAILED = "activity_attempt_failed"  # informational, retried
    ACTIVITY_COMPLETED = "activity_completed"
    ACTIVITY_FAILED = "activity_failed"  # final

    TIMER_STARTED = "timer_started"
    TIMER_FIRED = "timer_fired"

    SIGNAL_WAIT_STARTED = "signal_wait_started"
    SIGNAL_RECEIVED = "signal_received"

    SIDE_EFFECT = "side_effect"

    CANCEL_REQUESTED = "cancel_requested"
    CANCEL_DELIVERED = "cancel_delivered"


TERMINAL = {
    EventType.WORKFLOW_COMPLETED,
    EventType.WORKFLOW_FAILED,
    EventType.WORKFLOW_CANCELLED,
}


@dataclass(frozen=True)
class Event:
    seq: int
    type: EventType
    cmd: int | None = None  # the workflow command this event belongs to, if any
    data: dict[str, Any] = field(default_factory=dict)
    at: float = 0.0


@dataclass(frozen=True)
class NewEvent:
    """An event produced during a workflow task, not yet persisted."""

    type: EventType
    cmd: int | None = None
    data: dict[str, Any] = field(default_factory=dict)
