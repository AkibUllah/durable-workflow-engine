"""Leases, crash recovery, optimistic concurrency and idempotent starts."""

from __future__ import annotations

import pytest

from durable import EventType, RunConflict, Worker, WorkflowNotFound, current_activity
from durable.events import NewEvent
from durable.store import normalise_url


def test_postgres_urls_are_normalised_for_psycopg():
    assert normalise_url("postgres://u:p@h/db") == "postgresql+psycopg://u:p@h/db"
    assert normalise_url("postgresql://u:p@h/db") == "postgresql+psycopg://u:p@h/db"
    assert normalise_url("sqlite:///x.db") == "sqlite:///x.db"


def test_starting_twice_with_the_same_run_id_is_idempotent(engine, registry):
    @registry.workflow
    async def once(ctx, _):
        return 1

    @registry.workflow
    async def other(ctx, _):
        return 2

    first = engine.start("once", {"a": 1}, run_id="invoice-7")
    again = engine.start("once", {"a": 2}, run_id="invoice-7")
    assert first.id == again.id == "invoice-7" and again.input == {"a": 1}
    assert len(engine.history("invoice-7")) == 1
    with pytest.raises(RunConflict):
        engine.start("other", run_id="invoice-7")
    with pytest.raises(WorkflowNotFound):
        engine.start("missing")


def test_a_leased_task_is_invisible_to_other_workers(engine, store, registry, clock):
    @registry.workflow
    async def wf(ctx, _):
        return 1

    engine.start("wf")
    first = store.claim("a", clock.now(), lease_seconds=30, limit=10)
    second = store.claim("b", clock.now(), lease_seconds=30, limit=10)
    assert len(first) == 1 and second == []
    clock.advance(31)
    assert len(store.claim("b", clock.now(), lease_seconds=30, limit=10)) == 1


def test_a_crashed_worker_activity_is_retried_by_another_worker(engine, registry, clock, store):
    runs_seen = []

    @registry.activity
    def ship(order):
        runs_seen.append((current_activity().attempt, current_activity().idempotency_key))
        return f"shipped {order}"

    @registry.workflow
    async def fulfil(ctx, order):
        return await ctx.activity(ship, order)

    run = engine.start("fulfil", "A-1")
    crashed, healthy = Worker(engine, name="crashed", lease_seconds=30), Worker(engine, name="ok")
    crashed.run_once()  # the workflow task schedules the activity
    leased = store.claim("crashed", clock.now(), lease_seconds=30, limit=1)
    assert leased[0].kind == "activity"  # ...and then the worker dies holding it

    assert healthy.run_until_idle() == 0  # lease still valid: nobody else touches it
    clock.advance(30.5)
    healthy.run_until_idle()
    assert engine.get(run.id).result == "shipped A-1"
    assert runs_seen == [(2, f"{run.id}:0")]  # counted as the second attempt, same key


def test_a_late_result_from_a_worker_that_lost_its_lease_is_dropped(engine, registry, clock):
    stolen = []

    @registry.activity
    def slow():
        clock.advance(60)  # takes longer than the 30 s lease...
        stolen.extend(engine.store.claim("thief", clock.now(), 30, 1))  # ...so it is taken over
        return "late"

    @registry.workflow
    async def wf(ctx, _):
        return await ctx.activity(slow)

    run = engine.start("wf")
    Worker(engine, name="slow", lease_seconds=30).run_until_idle()

    assert [(t.kind, t.attempt, t.lease_owner) for t in stolen] == [("activity", 2, "thief")]
    kinds = [e.type for e in engine.history(run.id)]
    assert EventType.ACTIVITY_COMPLETED not in kinds  # the stale owner could not record it
    assert engine.get(run.id).status == "running"


def test_workflow_task_aborts_when_history_moved_underneath_it(engine, registry, clock, store):
    @registry.workflow
    async def waiter(ctx, _):
        return await ctx.wait_signal("go")

    run = engine.start("waiter")
    task = store.claim("w", clock.now(), 30, 1)[0]
    with store.transaction() as conn:  # an append that bumps the version mid-task
        assert store.append(
            conn,
            run.id,
            [NewEvent(EventType.SIGNAL_RECEIVED, None, {"name": "go", "payload": 5})],
            clock.now(),
        )
    with store.transaction() as conn:
        stale = store.get_run(conn, run.id).version - 1
        assert not store.append(conn, run.id, [], clock.now(), expected_version=stale)
    engine.process(task, "w")
    Worker(engine).run_until_idle()
    assert engine.get(run.id).result == 5


def test_only_one_workflow_task_is_queued_per_run(engine, registry, store, clock):
    @registry.workflow
    async def waiter(ctx, _):
        return [await ctx.wait_signal("s") for _ in range(3)]

    run = engine.start("waiter")
    for i in range(3):
        engine.signal(run.id, "s", i)
    assert engine.stats()["tasks"] == {"workflow": 1}  # three signals, one wake-up
    Worker(engine).run_until_idle()
    assert engine.get(run.id).result == [0, 1, 2]


def test_two_workflow_tasks_for_one_run_are_never_held_at_once(engine, registry, store, clock):
    @registry.workflow
    async def waiter(ctx, _):
        return await ctx.wait_signal("s")

    run = engine.start("waiter")
    held = store.claim("a", clock.now(), 30, 10)
    engine.signal(run.id, "s", 1)  # queues a second workflow task while the first is leased
    assert store.claim("b", clock.now(), 30, 10) == []
    assert [t.kind for t in held] == ["workflow"]


def test_list_and_stats(engine, registry, worker):
    @registry.workflow
    async def quick(ctx, n):
        return n

    @registry.workflow
    async def stuck(ctx, _):
        await ctx.wait_signal("never")

    for i in range(3):
        engine.start("quick", i)
    engine.start("stuck")
    worker.run_until_idle()
    assert engine.stats() == {"runs": {"completed": 3, "running": 1}, "tasks": {}}
    assert len(engine.list(status="completed")) == 3
    assert [r.workflow for r in engine.list(workflow="stuck")] == ["stuck"]
    assert len(engine.list(limit=2)) == 2
