"""HTTP API and dashboard for the engine."""

from __future__ import annotations

import logging
import secrets
from contextlib import asynccontextmanager
from importlib import resources
from typing import Any

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request, Security, status
from fastapi.responses import HTMLResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

from . import __version__
from .clock import Clock
from .config import Settings, get_settings
from .engine import Engine
from .errors import RunConflict, RunFinished, RunNotFound, WorkflowNotFound
from .events import Event
from .registry import Registry
from .store import Run, Store
from .worker import WorkerPool

log = logging.getLogger("durable.api")

_api_key = APIKeyHeader(name="X-API-Key", auto_error=False)
RUN_ID = r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$"


class StartIn(BaseModel):
    input: Any = None
    run_id: str | None = Field(
        default=None, pattern=RUN_ID, description="Business id; starting twice returns the same run"
    )


class SignalIn(BaseModel):
    payload: Any = None


class RunOut(BaseModel):
    id: str
    workflow: str
    status: str
    input: Any
    result: Any
    error: str | None
    error_type: str | None
    created_at: float
    updated_at: float

    @classmethod
    def of(cls, run: Run) -> RunOut:
        return cls(
            id=run.id,
            workflow=run.workflow,
            status=run.status,
            input=run.input,
            result=run.result,
            error=run.error,
            error_type=run.error_type,
            created_at=run.created_at,
            updated_at=run.updated_at,
        )


class EventOut(BaseModel):
    seq: int
    type: str
    cmd: int | None
    data: dict[str, Any]
    at: float

    @classmethod
    def of(cls, event: Event) -> EventOut:
        return cls(
            seq=event.seq, type=event.type.value, cmd=event.cmd, data=event.data, at=event.at
        )


class WorkflowOut(BaseModel):
    name: str
    description: str
    sample_input: Any = None


class StatsOut(BaseModel):
    runs: dict[str, int]
    tasks: dict[str, int]


class HealthOut(BaseModel):
    status: str
    version: str
    database: str
    embedded_worker: bool


def require_api_key(request: Request, key: str | None = Security(_api_key)) -> None:
    expected = request.app.state.settings.api_key
    if expected and not (key and secrets.compare_digest(key, expected)):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or missing API key")


def create_app(
    settings: Settings | None = None,
    *,
    registry: Registry | None = None,
    store: Store | None = None,
    clock: Clock | None = None,
    sample_inputs: dict[str, Any] | None = None,
) -> FastAPI:
    settings = settings or get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if registry is None:
        from . import demo

        registry = demo.registry
        sample_inputs = demo.SAMPLE_INPUTS if sample_inputs is None else sample_inputs
    sample_inputs = sample_inputs or {}
    engine = Engine(store or Store(settings.database_url), registry, clock)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool = None
        if settings.embedded_worker:
            pool = WorkerPool(
                engine,
                concurrency=settings.worker_concurrency,
                lease_seconds=settings.lease_seconds,
            )
            pool.start(poll_interval=settings.poll_interval)
            log.info("embedded worker pool started (%s threads)", settings.worker_concurrency)
        yield
        if pool is not None:
            pool.stop()

    app = FastAPI(
        title="Durable Workflow Engine",
        version=__version__,
        description="Start workflow runs, signal or cancel them, and read their event history.",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.engine = engine

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def dashboard() -> str:
        return resources.files("durable").joinpath("static/index.html").read_text("utf-8")

    @app.get("/health", response_model=HealthOut, tags=["meta"])
    def health() -> HealthOut:
        return HealthOut(
            status="ok",
            version=__version__,
            database=engine.store.engine.dialect.name,
            embedded_worker=settings.embedded_worker,
        )

    v1 = APIRouter(prefix="/v1", dependencies=[Depends(require_api_key)])

    @v1.get("/workflows", response_model=list[WorkflowOut], tags=["workflows"])
    def workflows() -> list[WorkflowOut]:
        return [
            WorkflowOut(
                name=w.name, description=w.description, sample_input=sample_inputs.get(w.name)
            )
            for w in registry.workflows()
        ]

    @v1.post(
        "/workflows/{name}/runs",
        response_model=RunOut,
        status_code=status.HTTP_201_CREATED,
        tags=["runs"],
    )
    def start(name: str, body: StartIn) -> RunOut:
        try:
            return RunOut.of(engine.start(name, body.input, run_id=body.run_id))
        except WorkflowNotFound as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, str(exc)) from None
        except RunConflict as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from None
        except TypeError as exc:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc)) from None

    @v1.get("/runs", response_model=list[RunOut], tags=["runs"])
    def list_runs(
        status_: str | None = Query(default=None, alias="status"),
        workflow: str | None = None,
        limit: int = Query(default=50, ge=1, le=500),
    ) -> list[RunOut]:
        return [RunOut.of(r) for r in engine.list(status=status_, workflow=workflow, limit=limit)]

    @v1.get("/runs/{run_id}", response_model=RunOut, tags=["runs"])
    def get_run(run_id: str) -> RunOut:
        try:
            return RunOut.of(engine.get(run_id))
        except RunNotFound:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"run {run_id!r} not found") from None

    @v1.get("/runs/{run_id}/history", response_model=list[EventOut], tags=["runs"])
    def history(run_id: str) -> list[EventOut]:
        try:
            return [EventOut.of(e) for e in engine.history(run_id)]
        except RunNotFound:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"run {run_id!r} not found") from None

    @v1.post(
        "/runs/{run_id}/signals/{name}",
        status_code=status.HTTP_202_ACCEPTED,
        response_model=RunOut,
        tags=["runs"],
    )
    def signal(run_id: str, name: str, body: SignalIn) -> RunOut:
        try:
            engine.signal(run_id, name, body.payload)
            return RunOut.of(engine.get(run_id))
        except RunNotFound:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"run {run_id!r} not found") from None
        except RunFinished as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from None

    @v1.post(
        "/runs/{run_id}/cancel",
        status_code=status.HTTP_202_ACCEPTED,
        response_model=RunOut,
        tags=["runs"],
    )
    def cancel(run_id: str) -> RunOut:
        try:
            engine.cancel(run_id)
            return RunOut.of(engine.get(run_id))
        except RunNotFound:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"run {run_id!r} not found") from None
        except RunFinished as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from None

    @v1.get("/stats", response_model=StatsOut, tags=["meta"])
    def stats() -> StatsOut:
        return StatsOut(**engine.stats())

    app.include_router(v1)
    return app
