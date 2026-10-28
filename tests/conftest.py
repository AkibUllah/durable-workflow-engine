from __future__ import annotations

import os

import pytest

from durable import Engine, ManualClock, Registry, Store, Worker

PG_URL = os.environ.get("TEST_DATABASE_URL")


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def store(tmp_path) -> Store:
    if PG_URL:
        from sqlalchemy import create_engine

        from durable.store import metadata

        engine = create_engine(PG_URL.replace("postgresql://", "postgresql+psycopg://", 1))
        metadata.drop_all(engine)
        engine.dispose()
        return Store(PG_URL)
    return Store(f"sqlite:///{tmp_path / 'durable.db'}")


@pytest.fixture
def registry() -> Registry:
    return Registry()


@pytest.fixture
def engine(store, registry, clock) -> Engine:
    return Engine(store, registry, clock)


@pytest.fixture
def worker(engine) -> Worker:
    return Worker(engine, name="w1")
