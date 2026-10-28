"""Persistence: runs, their event histories, and the task queue that drives them.

Three rules keep concurrent workers honest:

* Appending to a history bumps ``runs.next_seq`` and ``runs.version`` in the same statement.
  On PostgreSQL that UPDATE row-locks the run, so appenders to one run are serialised; SQLite
  serialises all writers anyway. Sequence numbers are therefore gap-free and ordered.
* A workflow task commits only if the run's version is still the one it replayed
  (optimistic concurrency); otherwise it is rolled back and retried with the newer history.
* Tasks are claimed with a lease through a conditional UPDATE, so two workers can never hold
  the same task. PostgreSQL additionally uses ``FOR UPDATE SKIP LOCKED`` when picking
  candidates, so workers don't queue behind each other's row locks.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from sqlalchemy import (
    Column,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    and_,
    case,
    create_engine,
    delete,
    event,
    func,
    insert,
    or_,
    select,
    update,
)
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.pool import StaticPool

from .events import Event, EventType, NewEvent

metadata = MetaData()

runs = Table(
    "durable_runs",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("workflow", String(128), nullable=False, index=True),
    Column("status", String(16), nullable=False, index=True),
    Column("input", Text),
    Column("result", Text),
    Column("error", Text),
    Column("error_type", String(128)),
    Column("version", Integer, nullable=False, default=0),
    Column("next_seq", Integer, nullable=False, default=1),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
)

events = Table(
    "durable_events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(64), ForeignKey("durable_runs.id"), nullable=False),
    Column("seq", Integer, nullable=False),
    Column("type", String(32), nullable=False),
    Column("cmd", Integer),
    Column("data", Text, nullable=False),
    Column("at", Float, nullable=False),
    UniqueConstraint("run_id", "seq", name="uq_durable_events_run_seq"),
)

tasks = Table(
    "durable_tasks",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("kind", String(16), nullable=False),  # workflow | activity | timer
    Column("run_id", String(64), ForeignKey("durable_runs.id"), nullable=False),
    Column("cmd", Integer),
    Column("attempt", Integer, nullable=False, default=1),
    Column("due_at", Float, nullable=False),
    Column("lease_owner", String(64)),
    Column("lease_until", Float),
    Index("ix_durable_tasks_due", "due_at"),
    Index("ix_durable_tasks_run", "run_id", "kind"),
)

ACTIVE = "running"
FINISHED = ("completed", "failed", "cancelled")


@dataclass(frozen=True)
class Run:
    id: str
    workflow: str
    status: str
    input: Any
    result: Any
    error: str | None
    error_type: str | None
    version: int
    created_at: float
    updated_at: float

    @property
    def finished(self) -> bool:
        return self.status in FINISHED


@dataclass(frozen=True)
class Task:
    id: int
    kind: str
    run_id: str
    cmd: int | None
    attempt: int
    due_at: float
    lease_owner: str | None


def normalise_url(url: str) -> str:
    """Accept the ``postgres://`` URLs hosting providers hand out."""
    for prefix in ("postgres://", "postgresql://"):
        if url.startswith(prefix):
            return "postgresql+psycopg://" + url[len(prefix) :]
    return url


def _loads(text: str | None) -> Any:
    return None if text is None else json.loads(text)


def _dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _run(row: Any) -> Run:
    return Run(
        id=row.id,
        workflow=row.workflow,
        status=row.status,
        input=_loads(row.input),
        result=_loads(row.result),
        error=row.error,
        error_type=row.error_type,
        version=row.version,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _task(row: Any) -> Task:
    return Task(row.id, row.kind, row.run_id, row.cmd, row.attempt, row.due_at, row.lease_owner)


class Store:
    def __init__(self, url: str = "sqlite://", *, engine: Engine | None = None) -> None:
        url = normalise_url(url)
        if engine is None:
            if url.startswith("sqlite"):
                kwargs: dict[str, Any] = {
                    "connect_args": {"check_same_thread": False, "timeout": 30}
                }
                if url in ("sqlite://", "sqlite:///:memory:"):
                    kwargs["poolclass"] = StaticPool
                engine = create_engine(url, **kwargs)
                event.listen(engine, "connect", _sqlite_pragmas)
                event.listen(engine, "begin", _sqlite_begin_immediate)
            else:
                engine = create_engine(url, pool_pre_ping=True)
        self.engine = engine
        self.is_postgres = engine.dialect.name == "postgresql"
        # SQLite allows one writer at a time and its busy handler is not fair: under load a
        # thread can be starved past any timeout. Queue this process's transactions on a lock
        # instead; other processes still fall back to the 30 s busy timeout.
        self._serial = threading.RLock() if engine.dialect.name == "sqlite" else None
        metadata.create_all(engine)

    @contextmanager
    def transaction(self) -> Iterator[Connection]:
        if self._serial is None:
            with self.engine.begin() as conn:
                yield conn
            return
        with self._serial, self.engine.begin() as conn:
            yield conn

    # ------------------------------------------------------------------ runs

    def insert_run(self, conn: Connection, run_id: str, workflow: str, input: Any, now: float):
        conn.execute(
            insert(runs).values(
                id=run_id,
                workflow=workflow,
                status=ACTIVE,
                input=_dumps(input),
                version=0,
                next_seq=1,
                created_at=now,
                updated_at=now,
            )
        )

    def get_run(self, conn: Connection, run_id: str) -> Run | None:
        row = conn.execute(select(runs).where(runs.c.id == run_id)).first()
        return _run(row) if row else None

    def list_runs(
        self,
        conn: Connection,
        status: str | None = None,
        workflow: str | None = None,
        limit: int = 50,
    ) -> list[Run]:
        query = select(runs).order_by(runs.c.created_at.desc(), runs.c.id).limit(limit)
        if status:
            query = query.where(runs.c.status == status)
        if workflow:
            query = query.where(runs.c.workflow == workflow)
        return [_run(r) for r in conn.execute(query)]

    def finish_run(
        self,
        conn: Connection,
        run_id: str,
        status: str,
        now: float,
        result: Any = None,
        error: str | None = None,
        error_type: str | None = None,
    ) -> None:
        conn.execute(
            update(runs)
            .where(runs.c.id == run_id)
            .values(
                status=status,
                result=_dumps(result),
                error=error,
                error_type=error_type,
                updated_at=now,
            )
        )

    def status_counts(self, conn: Connection) -> dict[str, int]:
        rows = conn.execute(select(runs.c.status, func.count()).group_by(runs.c.status))
        return {status: count for status, count in rows}

    # ------------------------------------------------------------------ history

    def history(self, conn: Connection, run_id: str) -> list[Event]:
        rows = conn.execute(select(events).where(events.c.run_id == run_id).order_by(events.c.seq))
        return [Event(r.seq, EventType(r.type), r.cmd, json.loads(r.data), r.at) for r in rows]

    def append(
        self,
        conn: Connection,
        run_id: str,
        new: Iterable[NewEvent],
        now: float,
        expected_version: int | None = None,
    ) -> bool:
        """Append events atomically. Returns False if ``expected_version`` no longer matches."""
        new = list(new)
        condition = runs.c.id == run_id
        if expected_version is not None:
            condition = and_(condition, runs.c.version == expected_version)
        bumped = conn.execute(
            update(runs)
            .where(condition)
            .values(
                next_seq=runs.c.next_seq + len(new),
                version=runs.c.version + 1,
                updated_at=now,
            )
        )
        if bumped.rowcount != 1:
            return False
        end = conn.execute(select(runs.c.next_seq).where(runs.c.id == run_id)).scalar_one()
        first = end - len(new)
        if new:
            conn.execute(
                insert(events),
                [
                    {
                        "run_id": run_id,
                        "seq": first + i,
                        "type": ev.type.value,
                        "cmd": ev.cmd,
                        "data": _dumps(ev.data),
                        "at": now,
                    }
                    for i, ev in enumerate(new)
                ],
            )
        return True

    # ------------------------------------------------------------------ tasks

    def enqueue(
        self,
        conn: Connection,
        kind: str,
        run_id: str,
        due_at: float,
        cmd: int | None = None,
        attempt: int = 1,
    ) -> None:
        conn.execute(
            insert(tasks).values(kind=kind, run_id=run_id, cmd=cmd, attempt=attempt, due_at=due_at)
        )

    def enqueue_workflow_task(self, conn: Connection, run_id: str, now: float) -> None:
        """Wake the run, unless a wake-up that nobody has picked up yet is already queued."""
        waiting = conn.execute(
            select(tasks.c.id).where(
                tasks.c.run_id == run_id,
                tasks.c.kind == "workflow",
                tasks.c.lease_owner.is_(None),
            )
        ).first()
        if waiting is None:
            self.enqueue(conn, "workflow", run_id, now)

    def claim(self, owner: str, now: float, lease_seconds: float, limit: int) -> list[Task]:
        """Lease up to ``limit`` due tasks for ``owner``."""
        busy = tasks.alias("busy")
        run_is_busy = (
            select(busy.c.id)
            .where(
                busy.c.run_id == tasks.c.run_id,
                busy.c.kind == "workflow",
                busy.c.id != tasks.c.id,
                busy.c.lease_until >= now,
            )
            .exists()
        )
        free = or_(tasks.c.lease_until.is_(None), tasks.c.lease_until < now)
        candidates = (
            select(tasks.c.id)
            .where(
                tasks.c.due_at <= now,
                free,
                or_(tasks.c.kind != "workflow", ~run_is_busy),
            )
            .order_by(tasks.c.due_at, tasks.c.id)
            .limit(limit * 2)
        )
        if self.is_postgres:
            candidates = candidates.with_for_update(skip_locked=True, of=tasks)
        claimed: list[Task] = []
        with self.transaction() as conn:
            ids = [r.id for r in conn.execute(candidates)]
            seen_runs: set[str] = set()
            for task_id in ids:
                if len(claimed) >= limit:
                    break
                # An activity whose previous lease expired was cut off mid-run (the worker died
                # or stalled), so this claim is a new attempt.
                attempt = case(
                    (
                        and_(tasks.c.kind == "activity", tasks.c.lease_owner.is_not(None)),
                        tasks.c.attempt + 1,
                    ),
                    else_=tasks.c.attempt,
                )
                took = conn.execute(
                    update(tasks)
                    .where(tasks.c.id == task_id, free)
                    .values(lease_owner=owner, lease_until=now + lease_seconds, attempt=attempt)
                )
                if took.rowcount != 1:
                    continue
                row = conn.execute(select(tasks).where(tasks.c.id == task_id)).one()
                if row.kind == "workflow":
                    if row.run_id in seen_runs:  # one workflow task per run per batch
                        conn.execute(
                            update(tasks)
                            .where(tasks.c.id == task_id)
                            .values(lease_owner=None, lease_until=None)
                        )
                        continue
                    seen_runs.add(row.run_id)
                claimed.append(_task(row))
        return claimed

    def owns(self, conn: Connection, task: Task, owner: str) -> bool:
        row = conn.execute(select(tasks.c.lease_owner).where(tasks.c.id == task.id)).first()
        return row is not None and row.lease_owner == owner

    def finish_task(self, conn: Connection, task: Task, owner: str) -> bool:
        done = conn.execute(
            delete(tasks).where(tasks.c.id == task.id, tasks.c.lease_owner == owner)
        )
        return done.rowcount == 1

    def reschedule(
        self, conn: Connection, task: Task, owner: str, due_at: float, attempt: int
    ) -> bool:
        moved = conn.execute(
            update(tasks)
            .where(tasks.c.id == task.id, tasks.c.lease_owner == owner)
            .values(due_at=due_at, attempt=attempt, lease_owner=None, lease_until=None)
        )
        return moved.rowcount == 1

    def release(self, task: Task, owner: str, due_at: float) -> None:
        """Give a task back after an unexpected error or a lost race."""
        with self.transaction() as conn:
            conn.execute(
                update(tasks)
                .where(tasks.c.id == task.id, tasks.c.lease_owner == owner)
                .values(lease_owner=None, lease_until=None, due_at=due_at)
            )

    def drop_tasks(self, conn: Connection, run_id: str) -> None:
        conn.execute(delete(tasks).where(tasks.c.run_id == run_id))

    def task_counts(self, conn: Connection) -> dict[str, int]:
        rows = conn.execute(select(tasks.c.kind, func.count()).group_by(tasks.c.kind))
        return {kind: count for kind, count in rows}

    def next_due(self, conn: Connection) -> float | None:
        return conn.execute(select(func.min(tasks.c.due_at))).scalar()


def _sqlite_pragmas(dbapi_conn, _record) -> None:
    # Let SQLAlchemy's "begin" event issue BEGIN itself (see below) instead of pysqlite.
    dbapi_conn.isolation_level = None
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    # WAL + NORMAL: a crashed process loses nothing; only a power cut can drop the last commits.
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute("PRAGMA busy_timeout=30000")
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


def _sqlite_begin_immediate(conn) -> None:
    # Take the write lock up front. A deferred transaction that reads and then writes fails
    # with SQLITE_BUSY when another thread committed in between; IMMEDIATE waits instead.
    conn.exec_driver_sql("BEGIN IMMEDIATE")
