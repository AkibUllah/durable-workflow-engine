"""Demo workflows registered by the service. The "outside world" (a ledger, a mailer) is an
in-process fake, so the demo needs nothing but the engine itself; failure knobs in the inputs
show off retries and compensation."""

from __future__ import annotations

import threading

from . import NonRetryableError, Registry, RetryPolicy, current_activity
from .errors import ActivityError, SignalTimeout, WorkflowCancelled

registry = Registry()


class Ledger:
    """A toy account ledger. Postings are keyed by idempotency key, so a retried activity never
    moves money twice: the same trick a real payment API's Idempotency-Key header gives you."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.balances: dict[str, int] = {}
        self._postings: dict[str, int] = {}

    def seed(self, balances: dict[str, int]) -> None:
        with self._lock:
            self.balances.update(balances)

    def post(self, key: str, account: str, amount: int) -> int:
        with self._lock:
            if key not in self._postings:
                balance = self.balances.get(account, 0)
                if balance + amount < 0:
                    raise NonRetryableError(f"insufficient funds in {account}")
                self.balances[account] = balance + amount
                self._postings[key] = amount
            return self.balances[account]


ledger = Ledger()
ledger.seed({"ACC-1001": 50_000, "ACC-2002": 10_000, "ACC-3003": 0})
outbox: list[dict] = []  # "sent" emails


def _maybe_fail(flaky_attempts: int, what: str) -> None:
    if flaky_attempts and current_activity().attempt <= flaky_attempts:
        raise ConnectionError(f"{what}: upstream timed out (attempt {current_activity().attempt})")


# ---------------------------------------------------------------------- activities


@registry.activity(retry=RetryPolicy(max_attempts=4, initial_interval=2, backoff=2))
def debit(account: str, amount: int, flaky_attempts: int = 0) -> int:
    _maybe_fail(flaky_attempts, "debit")
    return ledger.post(current_activity().idempotency_key, account, -amount)


@registry.activity(retry=RetryPolicy(max_attempts=3, initial_interval=2, backoff=2))
def credit(account: str, amount: int, fail: bool = False) -> int:
    if fail:
        raise ConnectionError(f"bank for {account} is unreachable")
    return ledger.post(current_activity().idempotency_key, account, amount)


@registry.activity(retry=RetryPolicy(max_attempts=10, initial_interval=1, max_interval=30))
def refund(account: str, amount: int) -> int:
    return ledger.post(current_activity().idempotency_key, account, amount)


@registry.activity
def send_email(to: str, template: str, data: dict | None = None) -> str:
    message_id = f"msg-{current_activity().idempotency_key}"
    if not any(m["id"] == message_id for m in outbox):
        outbox.append({"id": message_id, "to": to, "template": template, "data": data or {}})
    return message_id


@registry.activity
def account_activated(user: str) -> bool:
    return user.endswith("@example.com")


@registry.activity(retry=RetryPolicy(max_attempts=5, initial_interval=1))
def fetch_region_metrics(region: str, flaky_attempts: int = 0) -> dict:
    _maybe_fail(flaky_attempts, f"metrics {region}")
    base = sum(ord(c) for c in region)
    return {"region": region, "orders": base * 3 % 997, "revenue": base * 41 % 99_991}


# ---------------------------------------------------------------------- workflows


@registry.workflow
async def money_transfer(ctx, data):
    """Saga: debit, then credit; if the credit fails for good (or the run is cancelled), refund.

    Input: {"from": "ACC-1001", "to": "ACC-2002", "amount": 2500,
            "debit_flaky_attempts": 0, "fail_credit": false}
    """
    source, target, amount = data["from"], data["to"], int(data["amount"])
    await ctx.activity(debit, source, amount, int(data.get("debit_flaky_attempts", 0)))
    try:
        await ctx.activity(credit, target, amount, bool(data.get("fail_credit", False)))
    except (ActivityError, WorkflowCancelled) as exc:
        await ctx.activity(refund, source, amount)
        return {"status": "refunded", "reason": str(exc), "transfer_id": ctx.uuid4()}
    return {"status": "completed", "amount": amount, "transfer_id": ctx.uuid4()}


@registry.workflow
async def document_approval(ctx, data):
    """Ask a reviewer, remind them while they are silent, escalate after the last reminder.

    Input: {"document": "Q3 budget", "reviewer": "lead@example.com",
            "remind_after_seconds": 86400, "reminders": 2}
    Signal "decision" with {"approved": true, "note": "..."}.
    """
    doc, reviewer = data["document"], data["reviewer"]
    wait = float(data.get("remind_after_seconds", 86_400))
    await ctx.activity(send_email, reviewer, "review_requested", {"document": doc})
    for reminder in range(int(data.get("reminders", 2)) + 1):
        try:
            decision = await ctx.wait_signal("decision", timeout=wait)
        except SignalTimeout:
            if reminder == int(data.get("reminders", 2)):
                await ctx.activity(
                    send_email, "manager@example.com", "escalated", {"document": doc}
                )
                return {"document": doc, "status": "escalated"}
            await ctx.activity(
                send_email, reviewer, "reminder", {"document": doc, "n": reminder + 1}
            )
            continue
        status = "approved" if decision.get("approved") else "rejected"
        await ctx.activity(
            send_email, data.get("author", "author@example.com"), status, {"document": doc}
        )
        return {"document": doc, "status": status, "note": decision.get("note")}
    return {"document": doc, "status": "unreachable"}  # pragma: no cover


@registry.workflow
async def user_onboarding(ctx, data):
    """Drip campaign: welcome now, check activation after a delay, then tips or a nudge.

    Input: {"user": "nadia@example.com", "delay_seconds": 259200}
    """
    user = data["user"]
    started = ctx.now()
    await ctx.activity(send_email, user, "welcome")
    await ctx.sleep(float(data.get("delay_seconds", 3 * 86_400)))
    if await ctx.activity(account_activated, user):
        await ctx.activity(send_email, user, "tips")
        outcome = "activated"
    else:
        await ctx.activity(send_email, user, "nudge")
        outcome = "nudged"
    return {"user": user, "outcome": outcome, "elapsed": ctx.now() - started}


@registry.workflow
async def regional_report(ctx, data):
    """Fan out one activity per region in parallel, then aggregate.

    Input: {"regions": ["dhaka", "chattogram", "sylhet"], "flaky_attempts": 1}
    """
    regions = data.get("regions") or ["dhaka", "chattogram", "sylhet", "khulna"]
    flaky = int(data.get("flaky_attempts", 0))
    rows = await ctx.gather(*(ctx.activity(fetch_region_metrics, r, flaky) for r in regions))
    return {
        "regions": len(rows),
        "orders": sum(r["orders"] for r in rows),
        "revenue": sum(r["revenue"] for r in rows),
        "top": max(rows, key=lambda r: r["revenue"])["region"],
    }


SAMPLE_INPUTS = {
    "money_transfer": {
        "from": "ACC-1001",
        "to": "ACC-2002",
        "amount": 2500,
        "debit_flaky_attempts": 1,
    },
    "document_approval": {
        "document": "Q3 budget",
        "reviewer": "lead@example.com",
        "remind_after_seconds": 60,
        "reminders": 2,
    },
    "user_onboarding": {"user": "nadia@example.com", "delay_seconds": 30},
    "regional_report": {
        "regions": ["dhaka", "chattogram", "sylhet", "khulna"],
        "flaky_attempts": 1,
    },
}
