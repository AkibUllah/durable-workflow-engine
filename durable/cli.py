"""Command line: run workers, start runs, inspect history.

durable worker --concurrency 4
durable start money_transfer '{"from": "ACC-1001", "to": "ACC-2002", "amount": 500}'
durable runs --status running
durable history <run-id>
durable signal <run-id> decision '{"approved": true}'
durable cancel <run-id>
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import threading
from datetime import datetime, timezone

from .config import get_settings
from .engine import Engine
from .errors import DurableError
from .store import Store
from .worker import WorkerPool


def _engine() -> Engine:
    from . import demo

    return Engine(Store(get_settings().database_url), demo.registry)


def _json(text: str | None):
    return None if text is None else json.loads(text)


def _ts(t: float) -> str:
    return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def cmd_worker(args: argparse.Namespace) -> int:
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    pool = WorkerPool(_engine(), concurrency=args.concurrency, lease_seconds=settings.lease_seconds)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    pool.start(poll_interval=settings.poll_interval)
    stop.wait()
    pool.stop()
    return 0


def cmd_start(args: argparse.Namespace) -> int:
    run = _engine().start(args.workflow, _json(args.input), run_id=args.run_id)
    print(run.id)
    return 0


def cmd_runs(args: argparse.Namespace) -> int:
    for run in _engine().list(status=args.status, workflow=args.workflow, limit=args.limit):
        print(f"{run.id}  {run.status:<10} {run.workflow:<20} {_ts(run.created_at)}")
    return 0


def cmd_history(args: argparse.Namespace) -> int:
    engine = _engine()
    run = engine.get(args.run_id)
    print(f"{run.workflow} {run.id}: {run.status}")
    for event in engine.history(args.run_id):
        cmd = f"#{event.cmd}" if event.cmd is not None else ""
        data = json.dumps(event.data)
        print(f"{event.seq:>4} {_ts(event.at)} {event.type.value:<24} {cmd:<4} {data}")
    return 0


def cmd_signal(args: argparse.Namespace) -> int:
    _engine().signal(args.run_id, args.name, _json(args.payload))
    return 0


def cmd_cancel(args: argparse.Namespace) -> int:
    _engine().cancel(args.run_id)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="durable", description="Durable workflow engine CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("worker", help="run worker threads until interrupted")
    p.add_argument("--concurrency", type=int, default=2)
    p.set_defaults(fn=cmd_worker)

    p = sub.add_parser("start", help="start a workflow run and print its id")
    p.add_argument("workflow")
    p.add_argument("input", nargs="?", help="JSON input")
    p.add_argument("--run-id")
    p.set_defaults(fn=cmd_start)

    p = sub.add_parser("runs", help="list recent runs")
    p.add_argument("--status")
    p.add_argument("--workflow")
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(fn=cmd_runs)

    p = sub.add_parser("history", help="print a run's event history")
    p.add_argument("run_id")
    p.set_defaults(fn=cmd_history)

    p = sub.add_parser("signal", help="send a signal to a run")
    p.add_argument("run_id")
    p.add_argument("name")
    p.add_argument("payload", nargs="?", help="JSON payload")
    p.set_defaults(fn=cmd_signal)

    p = sub.add_parser("cancel", help="request cancellation of a run")
    p.add_argument("run_id")
    p.set_defaults(fn=cmd_cancel)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.fn(args)
    except (DurableError, json.JSONDecodeError, TypeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
