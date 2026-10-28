"""Durable workflows: plain async Python functions that survive crashes, restarts and deploys.

from durable import Registry, RetryPolicy

registry = Registry()

@registry.activity(retry=RetryPolicy(max_attempts=5))
def charge(order_id: str, amount: int) -> str: ...

@registry.workflow
async def checkout(ctx, order):
    payment = await ctx.activity(charge, order["id"], order["amount"])
    await ctx.sleep(3600)
    ...
"""

from .clock import ManualClock, SystemClock
from .context import WorkflowContext
from .engine import ActivityInfo, Engine, current_activity
from .errors import (
    ActivityError,
    NonDeterminismError,
    NonRetryableError,
    RunConflict,
    RunFinished,
    RunNotFound,
    SignalTimeout,
    WorkflowCancelled,
    WorkflowNotFound,
)
from .events import Event, EventType
from .registry import Registry
from .retry import RetryPolicy
from .store import Run, Store
from .worker import Worker, WorkerPool

__version__ = "0.1.0"

__all__ = [
    "ActivityError",
    "ActivityInfo",
    "Engine",
    "Event",
    "EventType",
    "ManualClock",
    "NonDeterminismError",
    "NonRetryableError",
    "Registry",
    "RetryPolicy",
    "Run",
    "RunConflict",
    "RunFinished",
    "RunNotFound",
    "SignalTimeout",
    "Store",
    "SystemClock",
    "Worker",
    "WorkerPool",
    "WorkflowCancelled",
    "WorkflowContext",
    "WorkflowNotFound",
    "current_activity",
    "__version__",
]
