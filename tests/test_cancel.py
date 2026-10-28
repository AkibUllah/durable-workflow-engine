"""Cancellation: delivered at the current await, replayed deterministically, compensable."""

from __future__ import annotations

import pytest

from durable import EventType, RetryPolicy, RunFinished, WorkflowCancelled


def test_cancel_ends_a_waiting_run(engine, registry, worker):
    @registry.workflow
    async def waiter(ctx, _):
        await ctx.wait_signal("never")

    run = engine.start("waiter")
    worker.run_until_idle()
    engine.cancel(run.id)
    engine.cancel(run.id)  # idempotent
    worker.run_until_idle()

    cancelled = engine.get(run.id)
    assert cancelled.status == "cancelled"
    kinds = [e.type for e in engine.history(run.id)]
    assert kinds.count(EventType.CANCEL_REQUESTED) == 1
    assert kinds[-2:] == [EventType.CANCEL_DELIVERED, EventType.WORKFLOW_CANCELLED]
    with pytest.raises(RunFinished):
        engine.cancel(run.id)


def test_workflow_can_catch_cancellation_and_compensate(engine, registry, worker):
    undo = []

    @registry.activity
    def reserve():
        return "R-1"

    @registry.activity
    def release(reservation):
        undo.append(reservation)
        return True

    @registry.workflow
    async def booking(ctx, _):
        reservation = await ctx.activity(reserve)
        try:
            await ctx.wait_signal("confirm")
        except WorkflowCancelled:
            await ctx.activity(release, reservation)
            return {"status": "released", "reservation": reservation}
        return {"status": "confirmed"}

    run = engine.start("booking")
    worker.run_until_idle()
    engine.cancel(run.id)
    worker.run_until_idle()
    assert engine.get(run.id).result == {"status": "released", "reservation": "R-1"}
    assert undo == ["R-1"]


def test_cancel_inside_gather_replays_deterministically(engine, registry, worker, clock):
    """Results that land after the cancel was delivered must stay invisible to that await."""

    @registry.activity(retry=RetryPolicy(max_attempts=1))
    def quick():
        return "quick"

    @registry.activity
    def compensate():
        return "compensated"

    @registry.workflow
    async def fan(ctx, _):
        try:
            await ctx.gather(ctx.activity(quick), ctx.wait_signal("slow"))
        except WorkflowCancelled:
            await ctx.activity(compensate)
            return "cancelled cleanly"
        return "finished"

    run = engine.start("fan")
    worker.run_once()  # schedules quick + the wait
    engine.cancel(run.id)
    worker.run_once()  # quick runs, or the cancel is delivered first: either order is fine
    engine.signal(run.id, "slow", 1)  # arrives after the cancel request
    worker.run_until_idle()
    assert engine.get(run.id).result == "cancelled cleanly"
