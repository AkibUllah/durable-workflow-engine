"""Exceptions raised to workflow code and to callers of the engine."""

from __future__ import annotations


class DurableError(Exception):
    """Base class for everything this package raises on purpose."""


class ActivityError(DurableError):
    """An activity failed for good (retries exhausted or a non-retryable error).

    Raised inside the workflow at the ``await``, so the workflow can catch it and compensate.
    """

    def __init__(self, activity: str, message: str, error_type: str, attempts: int) -> None:
        self.activity = activity
        self.message = message
        self.error_type = error_type
        self.attempts = attempts
        super().__init__(f"activity {activity!r} failed after {attempts} attempt(s): {message}")


class NonRetryableError(DurableError):
    """Raise from an activity to fail it immediately, without further attempts."""


class NonDeterminismError(DurableError):
    """Replaying the workflow produced a different command than the one recorded in history."""


class WorkflowCancelled(DurableError):
    """Raised at the workflow's current ``await`` after a cancel request.

    Let it propagate to end the run as ``cancelled``, or catch it to run compensation first.
    """


class SignalTimeout(DurableError, TimeoutError):
    """``ctx.wait_signal`` reached its timeout before the signal arrived."""


class WorkflowNotFound(DurableError):
    pass


class RunNotFound(DurableError):
    pass


class RunFinished(DurableError):
    """The run has already completed, failed or been cancelled."""


class RunConflict(DurableError):
    """A run with this id already exists for a different workflow."""
