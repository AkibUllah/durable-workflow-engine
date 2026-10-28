"""Workers lease due tasks and hand them to the engine. Run as many as you like, in as many
processes as you like; the store makes sure a task has one owner at a time."""

from __future__ import annotations

import logging
import threading
import uuid

from .engine import Engine

log = logging.getLogger("durable.worker")


class Worker:
    def __init__(
        self,
        engine: Engine,
        *,
        name: str | None = None,
        lease_seconds: float = 30.0,
        batch_size: int = 10,
    ) -> None:
        self.engine = engine
        self.name = name or f"worker-{uuid.uuid4().hex[:8]}"
        self.lease_seconds = lease_seconds
        self.batch_size = batch_size

    def run_once(self) -> int:
        """Claim one batch of due tasks and process it. Returns how many tasks were processed."""
        claimed = self.engine.store.claim(
            self.name, self.engine.clock.now(), self.lease_seconds, self.batch_size
        )
        for task in claimed:
            self.engine.process(task, self.name)
        return len(claimed)

    def run_until_idle(self, max_rounds: int = 10_000) -> int:
        """Keep going until nothing is due right now (timers in the future stay queued)."""
        total = 0
        for _ in range(max_rounds):
            processed = self.run_once()
            if processed == 0:
                return total
            total += processed
        raise RuntimeError(f"still busy after {max_rounds} rounds; is a workflow looping?")

    def run_forever(self, stop: threading.Event, poll_interval: float = 0.5) -> None:
        log.info("%s started", self.name)
        while not stop.is_set():
            try:
                processed = self.run_once()
            except Exception:  # database hiccup: back off and try again
                log.exception("%s: claim failed", self.name)
                processed = 0
            if processed == 0:
                stop.wait(poll_interval)
        log.info("%s stopped", self.name)


class WorkerPool:
    """N worker threads in this process (the API uses one when EMBEDDED_WORKER is on)."""

    def __init__(self, engine: Engine, concurrency: int = 2, **worker_kwargs) -> None:
        self._stop = threading.Event()
        self._workers = [Worker(engine, **worker_kwargs) for _ in range(concurrency)]
        self._threads: list[threading.Thread] = []

    def start(self, poll_interval: float = 0.5) -> None:
        for worker in self._workers:
            thread = threading.Thread(
                target=worker.run_forever,
                args=(self._stop, poll_interval),
                name=worker.name,
                daemon=True,
            )
            thread.start()
            self._threads.append(thread)

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout)
        self._threads.clear()
