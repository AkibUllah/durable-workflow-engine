"""Clocks. The engine reads time only through one of these, so tests can move it by hand."""

from __future__ import annotations

import threading
import time
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float: ...


class SystemClock:
    def now(self) -> float:
        return time.time()


class ManualClock:
    """A clock that only moves when told to; durable timers fire on ``advance``."""

    def __init__(self, start: float = 1_800_000_000.0) -> None:
        self._now = start
        self._lock = threading.Lock()

    def now(self) -> float:
        with self._lock:
            return self._now

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("time only moves forward")
        with self._lock:
            self._now += seconds
