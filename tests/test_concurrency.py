"""Several workers on one database: every run completes, every activity runs exactly once (no
crashes here), and histories stay gap-free. Runs on SQLite by default and on PostgreSQL in CI
(TEST_DATABASE_URL)."""

from __future__ import annotations

import threading
from collections import Counter

from durable import Engine, SystemClock, Worker


def test_many_workers_many_runs(store, registry):
    executed: Counter = Counter()
    lock = threading.Lock()

    @registry.activity
    def step(run_key, i):
        with lock:
            executed[(run_key, i)] += 1
        return i

    @registry.workflow
    async def chain(ctx, key):
        first = await ctx.activity(step, key, 0)
        rest = await ctx.gather(ctx.activity(step, key, 1), ctx.activity(step, key, 2))
        return first + sum(rest)

    engine = Engine(store, registry, SystemClock())
    run_ids = [engine.start("chain", f"k{i}").id for i in range(30)]

    stop = threading.Event()
    workers = [Worker(engine, name=f"w{i}", batch_size=4) for i in range(4)]
    threads = [threading.Thread(target=w.run_forever, args=(stop, 0.01)) for w in workers]
    for t in threads:
        t.start()
    try:
        deadline = threading.Event()
        for _ in range(600):
            if all(r.status != "running" for r in engine.list(limit=100)):
                break
            deadline.wait(0.05)
    finally:
        stop.set()
        for t in threads:
            t.join(5)

    runs = {r.id: r for r in engine.list(limit=100)}
    assert {runs[i].status for i in run_ids} == {"completed"}
    assert {runs[i].result for i in run_ids} == {3}
    assert len(executed) == 90 and set(executed.values()) == {1}
    for run_id in run_ids:
        seqs = [e.seq for e in engine.history(run_id)]
        assert seqs == list(range(1, len(seqs) + 1))
        assert len({(e.type, e.cmd) for e in engine.history(run_id) if e.cmd is not None}) == len(
            [e for e in engine.history(run_id) if e.cmd is not None]
        )
