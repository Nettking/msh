"""A failed reviewer command must stay red and retain safe diagnostic evidence."""
from __future__ import annotations

import argparse
import io
import json
from types import SimpleNamespace

import pytest

from demo.icse.network import run


@pytest.mark.parametrize(
    ("worker_error", "worker_code", "public_error", "public_code"),
    [
        ("QuorumUnavailable", "federation-quorum-leader-required",
         "QuorumUnavailable", "federation-quorum-leader-required"),
        ("TimeoutError", None, "TimeoutError", None),
        ("secret-enrollment-marker", "secret-invitation-marker",
         "unclassified", "unclassified"),
    ],
)
def test_failed_campaign_retains_safe_command_cause_and_stops_every_child(
    tmp_path, monkeypatch, capsys, worker_error, worker_code, public_error, public_code,
) -> None:
    source_sha = "a" * 40
    children = []
    original_rpc = run.Child.rpc

    class FailedBootstrapChild:
        def __init__(self, label, _config):
            self.label = label
            self.sequence = 0
            self.stopped = False
            self.process = SimpleNamespace(
                pid=len(children) + 1,
                stdin=io.StringIO(),
                poll=lambda: 0 if self.stopped else None,
            )
            self.startup = {"node_id": label, "source_sha": source_sha}
            children.append(self)

        def _next(self, timeout):
            assert timeout == 45  # The existing RPC deadline remains in force.
            return {"id": self.sequence, "ok": False,
                    "error_type": worker_error, "error_code": worker_code}

        def rpc(self, operation, **values):
            return original_rpc(self, operation, **values)

        def stop(self):
            self.stopped = True

    monkeypatch.setattr(run, "source_identity", lambda _args: {"source_sha": source_sha})
    monkeypatch.setattr(run, "provision", lambda *_: {
        label: {} for label in ("voter-a", "voter-b", "voter-c", "reviewer")
    })
    monkeypatch.setattr(run, "Child", FailedBootstrapChild)
    output = tmp_path / "network-output"
    assert run.campaign(argparse.Namespace(output=output, step_pause=0)) == 1

    summary = json.loads((output / "public/summary.json").read_text(encoding="utf-8"))
    assert summary["result"] == "FAIL"
    assert summary["failure"]["command"] == {
        "worker": "voter-a", "operation": "bootstrap",
        "error_type": public_error, "error_code": public_code,
    }
    assert summary["all_owned_processes_stopped"]
    assert len(children) == 4 and all(child.stopped for child in children)
    assert "authenticated_quorum_bootstrap" not in summary["checks"]

    visible = "".join(path.read_text(encoding="utf-8") for path in (output / "public").iterdir())
    captured = capsys.readouterr()
    visible += captured.out + captured.err
    assert "secret-enrollment-marker" not in visible
    assert "secret-invitation-marker" not in visible
    private = (output / "private-state/driver-failure.log").read_text(encoding="utf-8")
    assert str(worker_error) in private
