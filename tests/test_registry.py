from __future__ import annotations

import pytest

from durable import ManualClock, Registry, RetryPolicy, WorkflowNotFound


def test_registration_rules():
    registry = Registry()

    @registry.workflow(name="renamed")
    async def wf(ctx, _):
        """Short description.

        Longer text that is not part of the catalogue entry."""

    @registry.activity(retry=RetryPolicy(max_attempts=7))
    def act():
        return 1

    assert [w.name for w in registry.workflows()] == ["renamed"]
    assert registry.get_workflow("renamed").description == "Short description."
    assert registry.get_activity("act").retry.max_attempts == 7
    assert act() == 1  # still a plain function
    with pytest.raises(WorkflowNotFound):
        registry.get_workflow("wf")
    with pytest.raises(ValueError):
        registry.activity(act, name="act")
    with pytest.raises(ValueError):

        @registry.workflow(name="renamed")
        async def duplicate(ctx, _):
            pass


def test_workflows_must_be_async():
    with pytest.raises(TypeError):

        @Registry().workflow
        def sync_workflow(ctx, _):
            pass


def test_manual_clock_only_moves_forward():
    clock = ManualClock(start=10)
    clock.advance(5)
    assert clock.now() == 15
    with pytest.raises(ValueError):
        clock.advance(-1)
