<!-- topics: durable-execution,workflow-engine,event-sourcing,saga-pattern,python,fastapi,postgresql,sqlalchemy,distributed-systems -->
# durable-workflow-engine

Durable execution for Python: workflows are plain `async` functions whose every step is recorded in an event-sourced history, so a crashed or redeployed worker resumes exactly where it stopped by deterministic replay. Retries with backoff, durable timers, signals, sagas with compensation and cancellation, on SQLite or PostgreSQL.

[![CI](https://github.com/AkibUllah/durable-workflow-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/AkibUllah/durable-workflow-engine/actions/workflows/ci.yml)
[![CodeQL](https://github.com/AkibUllah/durable-workflow-engine/actions/workflows/codeql.yml/badge.svg)](https://github.com/AkibUllah/durable-workflow-engine/actions/workflows/codeql.yml)
[![Release](https://img.shields.io/github/v/release/AkibUllah/durable-workflow-engine)](https://github.com/AkibUllah/durable-workflow-engine/releases)
![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791)
![License](https://img.shields.io/badge/license-MIT-green)

## Try it live

| | |
|---|---|
| API reference | https://akibullah.github.io/durable-workflow-engine/: Swagger UI built from the exported OpenAPI document (static, with no backend behind it) |
| Run in your browser | [![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/AkibUllah/durable-workflow-engine?quickstart=1). It starts the API and two worker containers on PostgreSQL with `docker compose`, then starts one run of every demo workflow. Open forwarded port 8000 for the dashboard and `/docs` for the API. To see crash recovery, run `docker compose kill worker` and then `docker compose up -d worker`. |
| One-click deploy | [![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/AkibUllah/durable-workflow-engine). `render.yaml` provisions the web service (with an embedded worker pool) and a free PostgreSQL. `API_KEY` is generated: enter it in the dashboard's key field, or send it as `X-API-Key`. |
| Local | `pip install -e . && uvicorn durable.asgi:app` (SQLite, workers in-process), or `docker compose up` |

## Screenshots

![Dashboard showing a money transfer saga's event history](docs/images/dashboard.png)

A real run of the `money_transfer` saga against a bank that is down. The debit succeeds, the credit fails twice with backoff (2 s, then 4 s) and then fails for good, and the workflow catches the `ActivityError` and refunds. Every step is an event in the history; the worker that resumes a run reads it back by replaying that list.

![Swagger UI API reference](docs/images/api-reference.png)

The live API reference on GitHub Pages: start runs, list them, read history, send signals, cancel.

## Why this exists

Every payment or onboarding flow I have built eventually needed the same things: "charge, then provision, and if provisioning fails, refund"; "wait three days, then email them"; "pause until a human approves, and remind them if they don't". The usual first version is a job queue plus a `status` column and a pile of cron jobs. It works until a deploy kills a worker between the charge and the provisioning, and nobody can say which step a given order reached.

Durable execution engines such as Temporal, Restate and DBOS solve this by letting you write the flow as ordinary code and making that code survive crashes. This repo builds the core idea from scratch so every moving part is visible and tested:

- the event-sourced history
- replay with non-determinism detection
- leased task queues with crash recovery
- the concurrency control that keeps two workers from advancing the same run

## Features

- **Workflows are `async def wf(ctx, input)`** with ordinary `if`, `for`, `try` and `except`. The `ctx` commands:
  - `ctx.activity(fn, *args)`, awaited for its result
  - `ctx.gather(...)` to run several commands in parallel
  - `ctx.sleep(seconds)`, a durable timer
  - `ctx.wait_signal(name, timeout=...)`
  - `ctx.side_effect(fn)`, `ctx.now()` and `ctx.uuid4()`
- **Event-sourced history** per run, append-only and gap-free. The workflow's state is whatever replaying the history produces.
- **Deterministic replay with non-determinism detection.** If a deploy changes which command a workflow issues at a recorded position, the run fails loudly with `NonDeterminismError` instead of silently diverging.
- **Activities with retry policies:** exponential backoff with a cap, non-retryable error types, and `NonRetryableError` to fail fast. A final failure surfaces in the workflow as a catchable `ActivityError`.
- **At-least-once activities with a stable `idempotency_key`** (`run_id:command`), visible through `current_activity()`, plus the attempt number and the workflow name.
- **Durable timers:** sleeps and signal timeouts survive restarts and fire from the task queue.
- **Signals** are buffered when they arrive early and consumed in order. When a signal and a timeout race, the order of events in history decides, so replay always reaches the same answer.
- **Cancellation** is delivered at the workflow's current `await` as `WorkflowCancelled`. Catch it to compensate (a saga), or let it end the run as `cancelled`.
- **Leased task queue.** If a worker dies holding a task, the lease expires and another worker takes over. A result from a worker that lost its lease is dropped.
- **Optimistic concurrency on runs.** A workflow task commits only if no event was appended while it replayed. Otherwise it retries with the newer history.
- **SQLite or PostgreSQL** through SQLAlchemy Core. PostgreSQL picks tasks with `FOR UPDATE SKIP LOCKED`. SQLite runs in WAL mode with immediate transactions and an in-process writer queue.
- **Service:** a FastAPI API (start, list, get, history, signal, cancel, stats, with an optional API key), a live dashboard with history timelines and `#run=<id>` deep links, a CLI, and either embedded worker threads or separate `durable worker` processes.
- **Four demo workflows:**
  - a money transfer saga with refunds
  - document approval with reminders and escalation
  - an onboarding drip campaign
  - a parallel regional report

## Architecture

```mermaid
flowchart LR
    subgraph Clients
        UI[Dashboard /]
        API[FastAPI /v1]
        CLI[durable CLI]
    end
    UI --> API
    API --> ENG[Engine\nstart · signal · cancel]
    CLI --> ENG
    ENG --> DB[(SQLite / PostgreSQL\nruns · events · tasks)]
    subgraph Workers["Workers (threads or processes)"]
        W1[Worker] --> P[Engine.process]
        W2[Worker] --> P
    end
    DB -- "claim due tasks\n(lease, SKIP LOCKED)" --> W1
    DB -- claim --> W2
    P -- "workflow task:\nreplay history -> new commands" --> DB
    P -- "activity task:\ncall fn, record result / retry" --> DB
    P -- "timer task:\nrecord timer_fired" --> DB
```

### How a run moves

```mermaid
sequenceDiagram
    participant C as Client
    participant E as Engine / DB
    participant W as Worker
    C->>E: start("money_transfer", input)
    E->>E: append workflow_started, queue workflow task
    W->>E: claim workflow task (lease)
    W->>W: replay: ctx.activity(debit) has no result yet
    W->>E: append activity_scheduled #0, queue activity task (if run.version unchanged)
    W->>E: claim activity task
    W->>W: debit("ACC-1001", 2500), outside any transaction
    W->>E: append activity_completed #0, queue workflow task
    W->>E: claim workflow task
    W->>W: replay: #0 returns recorded result, ctx.activity(credit) is new
    Note over W,E: ...and so on until workflow_completed
```

Key decisions:

- **Replay from the top every time.** A workflow task runs the function from the beginning. Each `ctx` call is numbered in call order. A command whose outcome is in history returns it at once, and the first `await` that cannot complete suspends the coroutine, which is then thrown away. No coroutine state is ever persisted, only events, so workers are stateless and any worker can continue any run.
- **The history decides races.** "Did the approval signal beat the 24-hour timeout?" is answered by comparing event sequence numbers, not wall-clock time. That is what makes replay deterministic.
- **Cancellation is recorded where it was delivered.** The `cancel_delivered` event carries the command it interrupted. On replay, results that landed after the delivery stay invisible to that `await` (including inside `gather`), so the workflow takes the same branch again.
- **Three consistency rules in the store.**
  - Appends bump `runs.next_seq` and `runs.version` in one UPDATE. That row-locks the run on PostgreSQL, so sequence numbers stay gap-free.
  - A workflow task commits only if `version` matches what it replayed.
  - Tasks are claimed with a conditional UPDATE (`WHERE lease_until < now`), so one task never has two owners.
- **Activities run outside transactions.** A slow HTTP call never holds a database lock. Its result is recorded only if the worker still owns the lease, and a reclaimed lease counts as a new attempt with the same idempotency key.
- **One wake-up per run.** Ten signals arriving together queue one workflow task, not ten, and a run never has two workflow tasks leased at once.

## Writing a workflow

```python
from durable import ActivityError, Registry, RetryPolicy, WorkflowCancelled, current_activity

registry = Registry()

@registry.activity(retry=RetryPolicy(max_attempts=4, initial_interval=2, backoff=2))
def debit(account: str, amount: int) -> int:
    # pass the stable key to the payment API so a retried attempt never charges twice
    return bank.post(current_activity().idempotency_key, account, -amount)

@registry.workflow
async def money_transfer(ctx, data):
    await ctx.activity(debit, data["from"], data["amount"])
    try:
        await ctx.activity(credit, data["to"], data["amount"])
    except (ActivityError, WorkflowCancelled):
        await ctx.activity(refund, data["from"], data["amount"])   # compensate
        return {"status": "refunded"}
    return {"status": "completed", "transfer_id": ctx.uuid4()}
```

More patterns from [`durable/demo.py`](durable/demo.py):

```python
decision = await ctx.wait_signal("decision", timeout=86_400)    # raises SignalTimeout
await ctx.sleep(3 * 86_400)                                     # survives restarts and deploys
rows = await ctx.gather(*(ctx.activity(fetch, r) for r in regions))
```

The rule that comes with replay: workflow code must be deterministic. Put I/O, randomness and time in activities or in `ctx.side_effect` / `ctx.now()` / `ctx.uuid4()`. Awaiting anything other than a `ctx` command fails the run with a clear error.

## Benchmark

`bench/bench.py` runs N copies of a 3-activity workflow (one step, then two in parallel). That is 6 tasks and 8 history events per run. Real numbers from this machine (Python 3.10.12, x86_64 Linux, 4 cores, laptop SSD; PostgreSQL 16 in Docker with default `fsync`/`synchronous_commit`):

| database | workers | runs | seconds | runs/s | tasks/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| sqlite | 1 | 300 | 15.92 | 19 | 113 |
| sqlite | 4 | 300 | 16.48 | 18 | 109 |
| postgresql | 1 | 300 | 94.51 | 3 | 19 |
| postgresql | 4 | 300 | 38.47 | 8 | 47 |

How to read it:

- Every task ends in a durable commit, so throughput is bounded by commit latency, not by Python.
- SQLite has a single writer, so extra threads don't help it. Switching its WAL mode to `synchronous=NORMAL` took it from 21 to 113 tasks/s on this disk.
- PostgreSQL pays a full fsync per commit inside a Docker volume on a laptop, but it scales with workers because claiming uses `SKIP LOCKED`.
- On a server-grade disk, or with `synchronous_commit=off` for workloads that tolerate losing the last few milliseconds, the PostgreSQL numbers move up by an order of magnitude.

```bash
make bench                                    # SQLite
python bench/bench.py --url postgresql://durable:durable@localhost:5432/durable
```

## Tech Stack

| Layer | Choice |
| --- | --- |
| Engine | Python 3.10+, `asyncio` coroutines driven by hand (no event loop in workflow code), dataclasses |
| Storage | SQLAlchemy 2 Core; SQLite (WAL) or PostgreSQL 16 via psycopg 3 |
| Service | FastAPI, Pydantic v2, pydantic-settings, uvicorn; vanilla JS dashboard |
| Tooling | pytest (+ coverage), ruff, GitHub Actions (SQLite matrix, PostgreSQL service job, Docker build), CodeQL, Dependabot |
| Runtime | Docker (python:3.12-slim, non-root), docker compose: API + 2 worker replicas + PostgreSQL |

## Quick Start

### Local, SQLite, one process

```bash
git clone git@github.com:AkibUllah/durable-workflow-engine.git
cd durable-workflow-engine
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
uvicorn durable.asgi:app --port 8080       # dashboard: http://localhost:8080/
```

Click a workflow name in the dashboard to start a run with sample input.

### docker compose, PostgreSQL, separate workers

```bash
docker compose up --build
# API + dashboard on http://localhost:8000, two `durable worker` containers, PostgreSQL
docker compose kill worker && docker compose up -d worker   # crash recovery in action
```

### CLI

```bash
durable start money_transfer '{"from": "ACC-1001", "to": "ACC-2002", "amount": 500}'
durable runs --status running
durable history <run-id>
durable signal <run-id> decision '{"approved": true}'
durable cancel <run-id>
durable worker --concurrency 4
```

## API

| Method | Path | Description |
| --- | --- | --- |
| GET | `/` | Dashboard (runs, history timeline, start / signal / cancel) |
| GET | `/health` | Status, version, database dialect, embedded worker flag |
| GET | `/v1/workflows` | Registered workflows with descriptions and sample inputs |
| POST | `/v1/workflows/{name}/runs` | `{input, run_id?}` → run; the same `run_id` again returns the existing run |
| GET | `/v1/runs?status=&workflow=&limit=` | Recent runs |
| GET | `/v1/runs/{id}` | Status, input, result or error |
| GET | `/v1/runs/{id}/history` | Every event: `seq`, `type`, `cmd`, `data`, `at` |
| POST | `/v1/runs/{id}/signals/{name}` | `{payload}`; `409` when the run has finished |
| POST | `/v1/runs/{id}/cancel` | Request cancellation (idempotent) |
| GET | `/v1/stats` | Runs by status, queued tasks by kind |

When `API_KEY` is set, `/v1/*` requires `X-API-Key`. Configuration (`.env.example`):

- `DATABASE_URL`: `postgres://` URLs from hosting providers are accepted
- `EMBEDDED_WORKER` and `WORKER_CONCURRENCY`
- `LEASE_SECONDS`
- `POLL_INTERVAL`

## Project Structure

```
durable/
├── context.py      # WorkflowContext: commands, replay index, cancellation cut-off, driver
├── engine.py       # start / signal / cancel; workflow, activity and timer task processing
├── store.py        # SQLAlchemy tables, appends with version check, leased claiming
├── worker.py       # Worker (run_once / run_until_idle / run_forever), WorkerPool
├── registry.py     # @registry.workflow / @registry.activity
├── retry.py        # RetryPolicy
├── events.py       # EventType, Event
├── errors.py       # ActivityError, WorkflowCancelled, SignalTimeout, NonDeterminismError...
├── api.py, asgi.py # FastAPI app factory + entry point
├── cli.py          # `durable` command
├── demo.py         # demo workflows and their fake ledger / mailer
└── static/index.html
bench/bench.py      # throughput benchmark (table above)
tests/              # 65 tests; the whole suite also runs on PostgreSQL in CI
```

## Testing

```bash
make test        # SQLite
TEST_DATABASE_URL=postgresql://durable:durable@localhost:5432/durable pytest   # PostgreSQL
make lint
```

65 tests, 96% line coverage, all run against both SQLite and PostgreSQL.

**Replay:**
- activities run once although the workflow replays
- side effects and `ctx.now()` are replayed exactly
- changed code is caught as `NonDeterminismError`
- a foreign `await` and non-JSON results fail the run
- `finally` blocks cannot issue commands while the coroutine is discarded

**Activities:**
- the backoff schedule, checked against the manual clock
- exhausted retries arrive as `ActivityError`
- non-retryable types and `NonRetryableError`
- call-site retry overrides
- a stable idempotency key across attempts
- async activities and unknown activities
- `gather` ordering and failure

**Timers and signals:**
- buffered signals
- timeouts
- a signal racing its timer
- a late signal going to the next wait
- finished runs dropping their timers

**Cancellation:**
- plain cancel
- cancel with compensation
- cancel inside `gather` replaying deterministically

**Store:**
- idempotent start
- lease exclusivity
- crash recovery by another worker
- a late result from a worker that lost its lease being dropped
- version-conflict retry
- one wake-up per run
- no concurrent workflow tasks for one run

**End to end:**
- the four demo workflows
- the API, including the API key and the embedded worker pool
- the CLI
- 4 worker threads × 30 runs, with every activity executed exactly once and gap-free histories

## Roadmap

- [ ] Child workflows and `continue_as_new` to keep very long histories bounded
- [ ] Workflow versioning helper (`ctx.version("change-id", 1, 2)`) for safe deploys of changed code
- [ ] Activity heartbeats with per-activity start-to-close timeouts (today the lease is the bound)
- [ ] Queries: read a value computed by replay without advancing the run
- [ ] OpenTelemetry spans per workflow and activity task, and Prometheus metrics for queue depth and task latency

## License

MIT © 2026 Akib Jafor
