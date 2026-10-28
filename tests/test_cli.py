from __future__ import annotations

import json

import pytest

from durable import cli
from durable.config import get_settings


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path / 'cli.db'}")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def run(capsys, *argv):
    code = cli.main(list(argv))
    out = capsys.readouterr()
    return code, out.out, out.err


def test_start_list_signal_cancel_and_history(db, capsys):
    code, out, _ = run(
        capsys,
        "start",
        "document_approval",
        json.dumps({"document": "Plan", "reviewer": "a@example.com"}),
        "--run-id",
        "doc-1",
    )
    assert code == 0 and out.strip() == "doc-1"
    code, out, _ = run(capsys, "runs")
    assert "doc-1" in out and "running" in out and "document_approval" in out
    assert run(capsys, "signal", "doc-1", "decision", '{"approved": true}')[0] == 0
    code, out, _ = run(capsys, "history", "doc-1")
    assert "workflow_started" in out and "signal_received" in out
    assert run(capsys, "cancel", "doc-1")[0] == 0


def test_errors_are_reported_without_a_traceback(db, capsys):
    code, _, err = run(capsys, "start", "no_such_workflow")
    assert code == 1 and "not registered" in err
    code, _, err = run(capsys, "start", "user_onboarding", "{not json")
    assert code == 1 and err.startswith("error:")
    code, _, err = run(capsys, "history", "missing-run")
    assert code == 1
