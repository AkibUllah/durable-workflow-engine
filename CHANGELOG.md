# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-10-28

First public release.

### Added

- Workflow context with activities, `gather`, durable timers, signals with timeouts, side effects, `now()` and `uuid4()`.
- Event-sourced run history with deterministic replay and non-determinism detection.
- Activity retry policies (exponential backoff, cap, non-retryable types, `NonRetryableError`), `ActivityError` in the workflow, `current_activity()` with a stable idempotency key.
- Cancellation delivered at the current await, with compensation support and a replay cut-off for `gather`.
- SQLAlchemy store for SQLite (WAL, immediate transactions) and PostgreSQL (`FOR UPDATE SKIP LOCKED`), leased task claiming, optimistic run versions, one wake-up per run.
- Workers (`run_once`, `run_until_idle`, `run_forever`), worker pools and the `durable` CLI.
- FastAPI service with a dashboard, optional API key and embedded workers; four demo workflows.
- Benchmark script, Docker image, docker compose (API + 2 workers + PostgreSQL), CI on Python 3.10-3.12 plus a PostgreSQL job, CodeQL, Dependabot, Pages API reference, Codespaces and Render kits.

[Unreleased]: https://github.com/AkibUllah/durable-workflow-engine/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/AkibUllah/durable-workflow-engine/releases/tag/v0.1.0
