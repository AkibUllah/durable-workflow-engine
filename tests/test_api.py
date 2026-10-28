"""HTTP API, driven with the worker stepped by hand."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from durable import Worker, demo
from durable.api import create_app
from durable.config import Settings


def make_client(store, clock, **settings):
    base = dict(_env_file=None, log_level="WARNING", embedded_worker=False)
    base.update(settings)
    app = create_app(Settings(**base), store=store, clock=clock)
    return app, TestClient(app)


@pytest.fixture
def api(store, clock):
    demo.ledger.seed({"ACC-1001": 50_000, "ACC-2002": 10_000, "ACC-3003": 0})
    app, client = make_client(store, clock)
    with client:
        yield client, Worker(app.state.engine)


def test_health_dashboard_and_workflow_catalogue(api):
    client, _ = api
    health = client.get("/health").json()
    assert health["status"] == "ok" and health["database"] in {"sqlite", "postgresql"}
    assert "Durable Workflow Engine" in client.get("/").text
    catalogue = {w["name"]: w for w in client.get("/v1/workflows").json()}
    assert set(catalogue) == {
        "money_transfer",
        "document_approval",
        "user_onboarding",
        "regional_report",
    }
    assert catalogue["money_transfer"]["description"].startswith("Saga")
    assert catalogue["money_transfer"]["sample_input"]["from"] == "ACC-1001"


def test_start_run_and_read_history(api, clock):
    client, worker = api
    r = client.post(
        "/v1/workflows/money_transfer/runs",
        json={"input": {"from": "ACC-1001", "to": "ACC-2002", "amount": 100}, "run_id": "tx-1"},
    )
    assert r.status_code == 201 and r.json()["status"] == "running" and r.json()["id"] == "tx-1"
    worker.run_until_idle()

    run = client.get("/v1/runs/tx-1").json()
    assert run["status"] == "completed" and run["result"]["status"] == "completed"
    history = client.get("/v1/runs/tx-1/history").json()
    assert [e["type"] for e in history][:2] == ["workflow_started", "activity_scheduled"]
    assert history[1]["data"]["name"] == "debit" and history[1]["cmd"] == 0
    assert [r["id"] for r in client.get("/v1/runs", params={"status": "completed"}).json()] == [
        "tx-1"
    ]
    assert client.get("/v1/stats").json() == {"runs": {"completed": 1}, "tasks": {}}


def test_signal_and_cancel_endpoints(api):
    client, worker = api
    sample = {"document": "Plan", "reviewer": "lead@example.com", "remind_after_seconds": 600}
    approve = client.post("/v1/workflows/document_approval/runs", json={"input": sample}).json()[
        "id"
    ]
    cancel = client.post("/v1/workflows/document_approval/runs", json={"input": sample}).json()[
        "id"
    ]
    worker.run_until_idle()

    r = client.post(f"/v1/runs/{approve}/signals/decision", json={"payload": {"approved": True}})
    assert r.status_code == 202
    assert client.post(f"/v1/runs/{cancel}/cancel").status_code == 202
    worker.run_until_idle()

    assert client.get(f"/v1/runs/{approve}").json()["result"]["status"] == "approved"
    assert client.get(f"/v1/runs/{cancel}").json()["status"] == "cancelled"
    assert client.post(f"/v1/runs/{approve}/signals/decision", json={}).status_code == 409
    assert client.post(f"/v1/runs/{cancel}/cancel").status_code == 409


@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("get", "/v1/runs/missing", None),
        ("get", "/v1/runs/missing/history", None),
        ("post", "/v1/runs/missing/signals/x", {}),
        ("post", "/v1/runs/missing/cancel", None),
        ("post", "/v1/workflows/missing/runs", {}),
    ],
)
def test_not_found(api, method, path, body):
    client, _ = api
    assert (
        getattr(client, method)(path, **({"json": body} if body is not None else {})).status_code
        == 404
    )


def test_validation_and_conflicts(api):
    client, _ = api
    assert (
        client.post("/v1/workflows/user_onboarding/runs", json={"run_id": "bad id!"}).status_code
        == 422
    )
    client.post(
        "/v1/workflows/user_onboarding/runs",
        json={"input": {"user": "a@example.com"}, "run_id": "r1"},
    )
    assert (
        client.post("/v1/workflows/regional_report/runs", json={"run_id": "r1"}).status_code == 409
    )
    again = client.post("/v1/workflows/user_onboarding/runs", json={"run_id": "r1"})
    assert again.status_code == 201 and again.json()["input"] == {"user": "a@example.com"}
    assert client.get("/v1/runs", params={"limit": 0}).status_code == 422


def test_api_key_protects_v1_but_not_health_or_dashboard(store, clock):
    _, client = make_client(store, clock, api_key="s3cret")
    with client:
        assert client.get("/health").status_code == 200
        assert client.get("/").status_code == 200
        assert client.get("/v1/runs").status_code == 401
        assert client.get("/v1/runs", headers={"X-API-Key": "wrong"}).status_code == 401
        assert client.get("/v1/runs", headers={"X-API-Key": "s3cret"}).status_code == 200


def test_embedded_worker_runs_workflows_in_the_background(tmp_path):
    import time

    from durable import Store

    settings = Settings(
        _env_file=None,
        log_level="WARNING",
        embedded_worker=True,
        worker_concurrency=2,
        poll_interval=0.02,
    )
    app = create_app(settings, store=Store(f"sqlite:///{tmp_path / 'bg.db'}"))
    with TestClient(app) as client:
        run_id = client.post(
            "/v1/workflows/regional_report/runs", json={"input": {"regions": ["dhaka", "khulna"]}}
        ).json()["id"]
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if client.get(f"/v1/runs/{run_id}").json()["status"] == "completed":
                break
            time.sleep(0.05)
        assert client.get(f"/v1/runs/{run_id}").json()["status"] == "completed"
