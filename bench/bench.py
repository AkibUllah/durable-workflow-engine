"""Throughput benchmark: N runs of a 3-activity workflow (one sequential step, then two in
parallel) processed by W worker threads. Reports runs/s, tasks/s and history size.

    python bench/bench.py --runs 300 --workers 4                      # SQLite file
    python bench/bench.py --url postgresql://user:pass@localhost/db   # PostgreSQL
"""

from __future__ import annotations

import argparse
import platform
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from durable import Engine, Registry, Store, SystemClock, Worker  # noqa: E402
from durable.store import metadata, normalise_url  # noqa: E402


def build_registry() -> Registry:
    registry = Registry()

    @registry.activity
    def step(i):
        return i

    @registry.workflow
    async def chain(ctx, key):
        first = await ctx.activity(step, 0)
        rest = await ctx.gather(ctx.activity(step, 1), ctx.activity(step, 2))
        return first + sum(rest)

    return registry


def run(url: str, runs: int, workers: int) -> dict:
    store = Store(url)
    engine = Engine(store, build_registry(), SystemClock())
    for i in range(runs):
        engine.start("chain", i)

    stop = threading.Event()
    pool = [Worker(engine, name=f"bench-{i}", batch_size=8) for i in range(workers)]
    threads = [threading.Thread(target=w.run_forever, args=(stop, 0.005)) for w in pool]
    started = time.perf_counter()
    for t in threads:
        t.start()
    while engine.stats()["runs"].get("running", 0):
        time.sleep(0.02)
    elapsed = time.perf_counter() - started
    stop.set()
    for t in threads:
        t.join()

    events = sum(len(engine.history(r.id)) for r in engine.list(limit=runs))
    # 3 activity tasks + 3 workflow tasks per run (start, after step, after gather)
    tasks = runs * 6
    return {
        "database": store.engine.dialect.name,
        "workers": workers,
        "runs": runs,
        "seconds": elapsed,
        "runs_per_s": runs / elapsed,
        "tasks_per_s": tasks / elapsed,
        "events_per_run": events / runs,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", help="database URL (default: a fresh SQLite file)")
    parser.add_argument("--runs", type=int, default=300)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 4])
    args = parser.parse_args()

    print(f"Python {platform.python_version()}, {platform.machine()} {platform.system()}")
    print("| database | workers | runs | seconds | runs/s | tasks/s | events/run |")
    print("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for workers in args.workers:
        with tempfile.TemporaryDirectory() as tmp:
            url = args.url or f"sqlite:///{Path(tmp) / 'bench.db'}"
            if args.url:
                from sqlalchemy import create_engine

                eng = create_engine(normalise_url(args.url))
                metadata.drop_all(eng)
                eng.dispose()
            r = run(url, args.runs, workers)
        print(
            f"| {r['database']} | {r['workers']} | {r['runs']} | {r['seconds']:.2f} | "
            f"{r['runs_per_s']:.0f} | {r['tasks_per_s']:.0f} | {r['events_per_run']:.0f} |"
        )


if __name__ == "__main__":
    main()
