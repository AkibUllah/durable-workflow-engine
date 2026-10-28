"""Activity retries, backoff, failures and the information an activity can see."""

from __future__ import annotations

import pytest

from durable import (
    ActivityError,
    EventType,
    NonRetryableError,
    RetryPolicy,
    current_activity,
)


def test_retry_policy_backoff_is_exponential_and_capped():
    policy = RetryPolicy(max_attempts=6, initial_interval=1, backoff=3, max_interval=20)
    assert [policy.delay(a) for a in range(1, 6)] == [1, 3, 9, 20, 20]
    assert policy.should_retry(5, "ValueError") and not policy.should_retry(6, "ValueError")
    assert RetryPolicy.from_dict(policy.to_dict()) == policy
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)
    with pytest.raises(ValueError):
        RetryPolicy(backoff=0.5)


def test_failed_attempts_are_retried_on_the_backoff_schedule(engine, registry, worker, clock):
    attempts = []

    @registry.activity(retry=RetryPolicy(max_attempts=4, initial_interval=10, backoff=2))
    def flaky():
        attempts.append((current_activity().attempt, clock.now()))
        if len(attempts) < 3:
            raise ConnectionError("try again")
        return "ok"

    @registry.workflow
    async def wf(ctx, _):
        return await ctx.activity(flaky)

    run = engine.start("wf")
    worker.run_until_idle()
    clock.advance(9.9)
    worker.run_until_idle()
    assert len(attempts) == 1  # not due yet
    clock.advance(0.1)
    worker.run_until_idle()
    clock.advance(20)
    worker.run_until_idle()

    assert engine.get(run.id).result == "ok"
    start = 1_800_000_000.0
    assert attempts == [(1, start), (2, start + 10), (3, start + 30)]
    failed = [e for e in engine.history(run.id) if e.type is EventType.ACTIVITY_ATTEMPT_FAILED]
    assert [(e.data["attempt"], e.data["retry_in"]) for e in failed] == [(1, 10), (2, 20)]


def test_exhausted_retries_raise_activity_error_in_the_workflow(engine, registry, worker, clock):
    @registry.activity(retry=RetryPolicy(max_attempts=2, initial_interval=1))
    def always_down():
        raise ConnectionError("down")

    @registry.workflow
    async def wf(ctx, _):
        try:
            await ctx.activity(always_down)
        except ActivityError as exc:
            return {"caught": exc.error_type, "attempts": exc.attempts, "activity": exc.activity}

    run = engine.start("wf")
    worker.run_until_idle()
    clock.advance(1)
    worker.run_until_idle()
    assert engine.get(run.id).result == {
        "caught": "ConnectionError",
        "attempts": 2,
        "activity": "always_down",
    }


def test_non_retryable_errors_fail_on_the_first_attempt(engine, registry, worker):
    calls = []

    @registry.activity(retry=RetryPolicy(max_attempts=5, non_retryable=("KeyError",)))
    def lookup(key):
        calls.append(key)
        return {}[key]

    @registry.activity(retry=RetryPolicy(max_attempts=5))
    def validate(value):
        calls.append(value)
        raise NonRetryableError("invalid amount")

    @registry.workflow
    async def wf(ctx, _):
        errors = []
        for call in (ctx.activity(lookup, "missing"), ctx.activity(validate, -1)):
            try:
                await call
            except ActivityError as exc:
                errors.append([exc.error_type, exc.attempts])
        return errors

    run = engine.start("wf")
    worker.run_until_idle()
    assert engine.get(run.id).result == [["KeyError", 1], ["NonRetryableError", 1]]
    assert calls == ["missing", -1]


def test_retry_policy_at_the_call_site_overrides_the_registered_one(engine, registry, worker):
    @registry.activity(retry=RetryPolicy(max_attempts=5))
    def down():
        raise RuntimeError("nope")

    @registry.workflow
    async def wf(ctx, _):
        try:
            await ctx.activity(down, retry=RetryPolicy(max_attempts=1))
        except ActivityError as exc:
            return exc.attempts

    run = engine.start("wf")
    worker.run_until_idle()
    assert engine.get(run.id).result == 1


def test_activity_info_and_a_stable_idempotency_key(engine, registry, worker, clock):
    seen = []

    @registry.activity(retry=RetryPolicy(max_attempts=3, initial_interval=1))
    def charge():
        info = current_activity()
        seen.append((info.attempt, info.idempotency_key, info.workflow, info.activity))
        if info.attempt == 1:
            raise TimeoutError("gateway timeout")
        return info.idempotency_key

    @registry.workflow
    async def pay(ctx, _):
        return await ctx.activity(charge)

    run = engine.start("pay", run_id="order-42")
    worker.run_until_idle()
    clock.advance(1)
    worker.run_until_idle()
    assert seen == [(1, "order-42:0", "pay", "charge"), (2, "order-42:0", "pay", "charge")]
    assert engine.get(run.id).result == "order-42:0"
    with pytest.raises(RuntimeError):
        current_activity()


def test_async_activities_are_supported(engine, registry, worker):
    @registry.activity
    async def fetch(x):
        return {"x": x}

    @registry.workflow
    async def wf(ctx, _):
        return await ctx.activity(fetch, 3)

    run = engine.start("wf")
    worker.run_until_idle()
    assert engine.get(run.id).result == {"x": 3}


def test_unknown_activity_and_unserialisable_results_fail_without_retrying(
    engine, registry, worker
):
    @registry.activity(retry=RetryPolicy(max_attempts=5))
    def returns_set():
        return {1, 2}

    @registry.workflow
    async def wf(ctx, _):
        out = []
        for call in (ctx.activity("not_registered"), ctx.activity(returns_set)):
            try:
                await call
            except ActivityError as exc:
                out.append(
                    [
                        exc.attempts,
                        exc.error_type,
                        "JSON" in exc.message or "registered" in exc.message,
                    ]
                )
        return out

    run = engine.start("wf")
    worker.run_until_idle()
    assert engine.get(run.id).result == [
        [1, "NonRetryableError", True],
        [1, "NonRetryableError", True],
    ]


def test_scheduling_an_unregistered_function_is_a_type_error(engine, registry, worker):
    def plain():
        return 1

    @registry.workflow
    async def wf(ctx, _):
        return await ctx.activity(plain)

    run = engine.start("wf")
    worker.run_until_idle()
    assert engine.get(run.id).error_type == "TypeError"


def test_gather_runs_activities_in_parallel_and_keeps_argument_order(engine, registry, worker):
    @registry.activity
    def square(x):
        return x * x

    @registry.workflow
    async def fan_out(ctx, n):
        return await ctx.gather(*(ctx.activity(square, i) for i in range(n)))

    run = engine.start("fan_out", 5)
    claimed_first_round = worker.run_once()  # the workflow task schedules all five at once
    assert claimed_first_round == 1
    assert len([e for e in engine.history(run.id) if e.type is EventType.ACTIVITY_SCHEDULED]) == 5
    worker.run_until_idle()
    assert engine.get(run.id).result == [0, 1, 4, 9, 16]


def test_gather_raises_the_first_failure(engine, registry, worker):
    @registry.activity(retry=RetryPolicy(max_attempts=1))
    def maybe(x):
        if x == 2:
            raise ValueError("bad input 2")
        return x

    @registry.workflow
    async def fan_out(ctx, _):
        try:
            await ctx.gather(*(ctx.activity(maybe, i) for i in range(4)))
        except ActivityError as exc:
            return exc.message

    run = engine.start("fan_out")
    worker.run_until_idle()
    assert engine.get(run.id).result == "bad input 2"
