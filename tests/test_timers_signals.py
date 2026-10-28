"""Durable timers and signals."""

from __future__ import annotations

import pytest

from durable import EventType, RunFinished, RunNotFound, SignalTimeout


def test_sleep_waits_for_the_clock(engine, registry, worker, clock):
    @registry.workflow
    async def nap(ctx, seconds):
        await ctx.sleep(seconds)
        return "awake"

    run = engine.start("nap", 3600)
    worker.run_until_idle()
    clock.advance(3599)
    worker.run_until_idle()
    assert engine.get(run.id).status == "running"
    clock.advance(1)
    worker.run_until_idle()
    assert engine.get(run.id).result == "awake"
    fired = [e for e in engine.history(run.id) if e.type is EventType.TIMER_FIRED]
    assert len(fired) == 1 and fired[0].at == 1_800_003_600.0


def test_negative_sleep_is_rejected(engine, registry, worker):
    @registry.workflow
    async def bad(ctx, _):
        await ctx.sleep(-1)

    run = engine.start("bad")
    worker.run_until_idle()
    assert engine.get(run.id).error_type == "ValueError"


def test_signal_resumes_a_waiting_run(engine, registry, worker):
    @registry.workflow
    async def approval(ctx, _):
        decision = await ctx.wait_signal("decision")
        return decision["approved"]

    run = engine.start("approval")
    worker.run_until_idle()
    assert engine.get(run.id).status == "running"
    engine.signal(run.id, "decision", {"approved": True})
    worker.run_until_idle()
    assert engine.get(run.id).result is True


def test_signals_sent_before_the_wait_are_buffered_in_order(engine, registry, worker):
    @registry.activity
    def slow_step():
        return None

    @registry.workflow
    async def collector(ctx, _):
        await ctx.activity(slow_step)
        return [await ctx.wait_signal("item") for _ in range(3)]

    run = engine.start("collector")
    for item in ("a", "b", "c"):
        engine.signal(run.id, "item", item)  # all arrive before the workflow waits
    worker.run_until_idle()
    assert engine.get(run.id).result == ["a", "b", "c"]


def test_signal_timeout_raises_in_the_workflow(engine, registry, worker, clock):
    @registry.workflow
    async def impatient(ctx, _):
        try:
            return await ctx.wait_signal("reply", timeout=60)
        except SignalTimeout:
            return "timed out"

    run = engine.start("impatient")
    worker.run_until_idle()
    clock.advance(60)
    worker.run_until_idle()
    assert engine.get(run.id).result == "timed out"


def test_a_signal_that_beats_the_timer_wins_and_the_late_timer_is_harmless(
    engine, registry, worker, clock
):
    @registry.workflow
    async def race(ctx, _):
        first = await ctx.wait_signal("reply", timeout=60)
        second = await ctx.wait_signal("reply")  # proves the timer did not steal anything
        return [first, second]

    run = engine.start("race")
    worker.run_until_idle()
    engine.signal(run.id, "reply", 1)
    clock.advance(61)  # timer fires too, after the signal
    worker.run_until_idle()
    assert engine.get(run.id).status == "running"
    engine.signal(run.id, "reply", 2)
    worker.run_until_idle()
    assert engine.get(run.id).result == [1, 2]


def test_a_signal_after_the_timeout_goes_to_the_next_wait(engine, registry, worker, clock):
    @registry.workflow
    async def two_waits(ctx, _):
        try:
            await ctx.wait_signal("reply", timeout=10)
        except SignalTimeout:
            pass
        return await ctx.wait_signal("reply")

    run = engine.start("two_waits")
    worker.run_until_idle()
    clock.advance(10)
    worker.run_once()  # timer task: records the firing and wakes the workflow
    engine.signal(run.id, "reply", "late")  # lands in history after timer_fired
    worker.run_until_idle()
    assert engine.get(run.id).result == "late"


def test_signals_to_missing_or_finished_runs_are_rejected(engine, registry, worker):
    @registry.workflow
    async def instant(ctx, _):
        return 1

    run = engine.start("instant")
    worker.run_until_idle()
    with pytest.raises(RunFinished):
        engine.signal(run.id, "x")
    with pytest.raises(RunNotFound):
        engine.signal("nope", "x")
    with pytest.raises(TypeError):
        engine.start("instant", {"bad": object()})


def test_finished_runs_drop_their_pending_timers(engine, registry, worker, clock, store):
    @registry.workflow
    async def early_exit(ctx, _):
        try:
            await ctx.wait_signal("stop", timeout=3600)
        except Exception:  # pragma: no cover
            pass
        return "stopped"

    run = engine.start("early_exit")
    worker.run_until_idle()
    assert engine.stats()["tasks"] == {"timer": 1}
    engine.signal(run.id, "stop")
    worker.run_until_idle()
    assert engine.get(run.id).status == "completed"
    assert engine.stats()["tasks"] == {}
