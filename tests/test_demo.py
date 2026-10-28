"""The bundled demo workflows, end to end."""

from __future__ import annotations

import pytest

from durable import Engine, EventType, Worker, demo


@pytest.fixture
def demo_engine(store, clock):
    demo.ledger.seed({"ACC-1001": 50_000, "ACC-2002": 10_000, "ACC-3003": 0})
    demo.outbox.clear()
    return Engine(store, demo.registry, clock)


def drain(engine, clock, seconds=0.0, steps=1):
    worker = Worker(engine)
    worker.run_until_idle()
    for _ in range(steps):
        clock.advance(seconds)
        worker.run_until_idle()


def test_transfer_survives_a_flaky_debit_without_double_charging(demo_engine, clock):
    run = demo_engine.start(
        "money_transfer",
        {"from": "ACC-1001", "to": "ACC-2002", "amount": 2500, "debit_flaky_attempts": 2},
    )
    drain(demo_engine, clock, seconds=10, steps=3)
    result = demo_engine.get(run.id).result
    assert result["status"] == "completed" and len(result["transfer_id"]) == 36
    assert demo.ledger.balances["ACC-1001"] == 47_500
    assert demo.ledger.balances["ACC-2002"] == 12_500
    retries = [
        e for e in demo_engine.history(run.id) if e.type is EventType.ACTIVITY_ATTEMPT_FAILED
    ]
    assert len(retries) == 2


def test_transfer_compensates_when_the_credit_fails_for_good(demo_engine, clock):
    run = demo_engine.start(
        "money_transfer",
        {"from": "ACC-1001", "to": "ACC-3003", "amount": 1000, "fail_credit": True},
    )
    drain(demo_engine, clock, seconds=10, steps=3)
    result = demo_engine.get(run.id).result
    assert result["status"] == "refunded" and "unreachable" in result["reason"]
    assert demo.ledger.balances["ACC-1001"] == 50_000  # debit undone
    assert demo.ledger.balances["ACC-3003"] == 0


def test_transfer_with_insufficient_funds_fails_without_retrying(demo_engine, clock):
    run = demo_engine.start("money_transfer", {"from": "ACC-3003", "to": "ACC-1001", "amount": 1})
    drain(demo_engine, clock)
    failed = demo_engine.get(run.id)
    assert failed.status == "failed" and failed.error_type == "ActivityError"
    assert "insufficient funds" in failed.error


def test_document_approval_with_a_decision(demo_engine, clock):
    run = demo_engine.start(
        "document_approval",
        {"document": "Q3 budget", "reviewer": "lead@example.com", "remind_after_seconds": 60},
    )
    drain(demo_engine, clock)
    demo_engine.signal(run.id, "decision", {"approved": False, "note": "cut travel"})
    drain(demo_engine, clock)
    assert demo_engine.get(run.id).result == {
        "document": "Q3 budget",
        "status": "rejected",
        "note": "cut travel",
    }
    assert [m["template"] for m in demo.outbox] == ["review_requested", "rejected"]


def test_document_approval_reminds_then_escalates(demo_engine, clock):
    run = demo_engine.start(
        "document_approval",
        {
            "document": "Policy",
            "reviewer": "lead@example.com",
            "remind_after_seconds": 60,
            "reminders": 2,
        },
    )
    drain(demo_engine, clock, seconds=60, steps=3)
    assert demo_engine.get(run.id).result == {"document": "Policy", "status": "escalated"}
    assert [m["template"] for m in demo.outbox] == [
        "review_requested",
        "reminder",
        "reminder",
        "escalated",
    ]


def test_onboarding_waits_then_branches(demo_engine, clock):
    activated = demo_engine.start(
        "user_onboarding", {"user": "nadia@example.com", "delay_seconds": 3600}
    )
    nudged = demo_engine.start("user_onboarding", {"user": "rafi@mail.test", "delay_seconds": 3600})
    drain(demo_engine, clock, seconds=3600)
    assert demo_engine.get(activated.id).result == {
        "user": "nadia@example.com",
        "outcome": "activated",
        "elapsed": 3600.0,
    }
    assert demo_engine.get(nudged.id).result["outcome"] == "nudged"
    assert sorted(m["template"] for m in demo.outbox) == ["nudge", "tips", "welcome", "welcome"]


def test_regional_report_fans_out_and_aggregates(demo_engine, clock):
    run = demo_engine.start(
        "regional_report", {"regions": ["dhaka", "sylhet"], "flaky_attempts": 1}
    )
    drain(demo_engine, clock, seconds=1, steps=2)
    rows = [
        demo.fetch_region_metrics(r) for r in ("dhaka", "sylhet")
    ]  # activities are plain functions
    assert demo_engine.get(run.id).result == {
        "regions": 2,
        "orders": sum(r["orders"] for r in rows),
        "revenue": sum(r["revenue"] for r in rows),
        "top": max(rows, key=lambda r: r["revenue"])["region"],
    }
    kinds = [e.type for e in demo_engine.history(run.id)]
    assert kinds.count(EventType.ACTIVITY_ATTEMPT_FAILED) == 2  # one flaky attempt per region


def test_every_demo_workflow_has_a_sample_input(demo_engine):
    names = {w.name for w in demo.registry.workflows()}
    assert names == set(demo.SAMPLE_INPUTS)
