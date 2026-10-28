"""Retry policies for activities."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff: ``initial_interval * backoff ** (attempt - 1)``, capped at
    ``max_interval``. ``non_retryable`` lists exception class names that fail at once."""

    max_attempts: int = 3
    initial_interval: float = 1.0
    backoff: float = 2.0
    max_interval: float = 60.0
    non_retryable: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.initial_interval < 0 or self.max_interval < 0:
            raise ValueError("intervals must not be negative")
        if self.backoff < 1:
            raise ValueError("backoff must be >= 1")
        object.__setattr__(self, "non_retryable", tuple(self.non_retryable))

    def delay(self, attempt: int) -> float:
        """Seconds to wait after failed attempt number ``attempt`` (1-based)."""
        return min(self.initial_interval * self.backoff ** (attempt - 1), self.max_interval)

    def should_retry(self, attempt: int, error_type: str) -> bool:
        return attempt < self.max_attempts and error_type not in self.non_retryable

    def to_dict(self) -> dict:
        return {
            "max_attempts": self.max_attempts,
            "initial_interval": self.initial_interval,
            "backoff": self.backoff,
            "max_interval": self.max_interval,
            "non_retryable": list(self.non_retryable),
        }

    @classmethod
    def from_dict(cls, data: dict) -> RetryPolicy:
        return cls(**{**data, "non_retryable": tuple(data.get("non_retryable", ()))})
