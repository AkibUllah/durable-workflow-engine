"""Deterministic replay: outcomes are recorded once and replayed, divergence is detected."""

from __future__ import annotations

import asyncio
import random

from durable import EventType


def types(engine, run_id):
    return [e.type for e in engine.history(run_id)]


def test_completed_run_records_every_step(engine, registry, worker):
    @registry.activity
    def double(x):
        return x * 2

    @registry.workflow
    async def pipeline(ctx, n):
        a = await ctx.activity(double, n)
        b = await ctx.activity(double, a)
        return {"a": a, "b": b}

    run = engine.start("pipeline", 5)
    worker.run_until_idle()

    done = engine.get(run.id)
    assert done.status == "completed" and done.result == {"a": 10, "b": 20}
    assert types(engine, run.id) == [
        EventType.WORKFLOW_STARTED,
        EventType.ACTIVITY_SCHEDULED,
        EventType.ACTIVITY_COMPLETED,
        EventType.ACTIVITY_SCHEDULED,
        EventType.ACTIVITY_COMPLETED,
        EventType.WORKFLOW_COMPLETED,
    ]
    seqs = [e.seq for e in engine.history(run.id)]
    assert seqs == list(range(1, len(seqs) + 1))


def test_activities_run_once_even_though_the_workflow_replays(engine, registry, worker):
    calls = []
    replays = []

    @registry.activity
    def step(i):
        calls.append(i)
        return i

    @registry.workflow
    async def many(ctx, _):
        replays.append(1)
        total = 0
        for i in range(5):
            total += await ctx.activity(step, i)
        return total

    run = engine.start("many")
    worker.run_until_idle()
    assert engine.get(run.id).result == 10
    assert calls == [0, 1, 2, 3, 4]
    assert len(replays) == 6  # one workflow task per completed activity, plus the first


def test_side_effects_are_recorded_and_replayed(engine, registry, worker, clock):
    seen = []

    @registry.activity
    def noop():
        return None

    @registry.workflow
    async def lucky(ctx, _):
        number = ctx.side_effect(lambda: random.randint(1, 10**9))
        seen.append(number)
        stamp = ctx.now()
        await ctx.activity(noop)
        clock.advance(100)  # time passes between tasks; ctx.now() must not notice on replay
        return {"number": number, "stamp": stamp, "id": ctx.uuid4()}

    run = engine.start("lucky")
    worker.run_until_idle()
    result = engine.get(run.id).result
    assert len(set(seen)) == 1 and result["number"] == seen[0]
    assert result["stamp"] == 1_800_000_000.0


def test_changed_workflow_code_is_detected_as_non_determinism(engine, registry, worker):
    @registry.activity
    def first():
        return 1

    @registry.activity
    def second():
        return 2

    version = {"current": 1}

    @registry.workflow
    async def evolving(ctx, _):
        if version["current"] == 1:
            await ctx.activity(first)
        else:
            await ctx.activity(second)  # a deploy changed the command at position 0
        await ctx.wait_signal("go")
        return "done"

    run = engine.start("evolving")
    worker.run_until_idle()
    version["current"] = 2
    engine.signal(run.id, "go")
    worker.run_until_idle()

    failed = engine.get(run.id)
    assert failed.status == "failed" and failed.error_type == "NonDeterminismError"
    assert "history has activity_scheduled 'first'" in failed.error


def test_unhandled_exception_fails_the_run(engine, registry, worker):
    @registry.workflow
    async def broken(ctx, data):
        return 1 / data["zero"]

    run = engine.start("broken", {"zero": 0})
    worker.run_until_idle()
    failed = engine.get(run.id)
    assert failed.status == "failed" and failed.error_type == "ZeroDivisionError"
    assert types(engine, run.id)[-1] is EventType.WORKFLOW_FAILED


def test_awaiting_something_other_than_ctx_fails_the_run(engine, registry, worker):
    @registry.workflow
    async def sneaky(ctx, _):
        await asyncio.sleep(0)

    run = engine.start("sneaky")
    worker.run_until_idle()
    failed = engine.get(run.id)
    assert failed.status == "failed" and "only await ctx commands" in failed.error


def test_non_json_workflow_result_fails_the_run(engine, registry, worker):
    @registry.workflow
    async def returns_object(ctx, _):
        return object()

    run = engine.start("returns_object")
    worker.run_until_idle()
    assert engine.get(run.id).error_type == "TypeError"


def test_finally_blocks_in_workflow_code_cannot_issue_commands_while_suspended(
    engine, registry, worker
):
    @registry.activity
    def cleanup():
        return "cleaned"

    @registry.workflow
    async def guarded(ctx, _):
        try:
            return await ctx.wait_signal("go")
        finally:
            await ctx.activity(cleanup)  # runs when the coroutine is discarded, too

    run = engine.start("guarded")
    worker.run_until_idle()
    names = [e.data.get("name") for e in engine.history(run.id)]
    assert "cleanup" not in names  # unwinding the suspended coroutine recorded nothing
    engine.signal(run.id, "go", 7)
    worker.run_until_idle()
    assert engine.get(run.id).result == 7
