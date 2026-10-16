from __future__ import annotations

import json
import subprocess
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from scripts import zero_touch_federation_start as start


@pytest.fixture(autouse=True)
def isolated_host_setup(monkeypatch):
    events: list[object] = []
    original_loader = start.load_host_module

    @contextmanager
    def lock(root):
        assert root == start.ROOT
        events.append("lock")
        try:
            yield
        finally:
            events.append("unlock")

    def load(name):
        if name == "host_mutation":
            return SimpleNamespace(host_mutation_lock=lock)
        return original_loader(name)

    def run(command, **_kwargs):
        assert command in (
            ["docker", "compose", "stop", "flask"],
            ["docker", "compose", "start", "flask"],
        )
        events.append(tuple(command))
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(start, "load_host_module", load)
    monkeypatch.setattr(start.subprocess, "run", run)
    return events


def _completed_capability() -> dict[str, object]:
    return {
        "startup_completed": True,
        "benchmarks_run": 3,
        "contributions_enabled": 2,
    }


def test_existing_member_reuses_membership_and_bootstraps_idempotently(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        start,
        "_status",
        lambda: {
            "state": "connected",
            "node_id": "node-a",
            "federation_id": "fed-a",
        },
    )
    monkeypatch.setattr(
        start,
        "_discover",
        lambda **_kwargs: pytest.fail("connected device must not rediscover to enroll"),
    )
    monkeypatch.setattr(start, "_bootstrap_capabilities", _completed_capability)

    result = start.zero_touch_start(initialize_federation=False, web_port=5000)

    assert result["federation_state"] == "connected"
    assert result["capability"]["startup_completed"] is True


def test_joining_device_auto_joins_the_single_discovered_federation(
    monkeypatch,
) -> None:
    statuses = iter(
        [
            {"state": "not-connected", "node_id": None, "federation_id": None},
            {"state": "connected", "node_id": "node-b", "federation_id": "fed-a"},
        ]
    )
    snapshot = {"federations": [{"federation_id": "fed-a"}]}
    joined: list[dict[str, object]] = []
    monkeypatch.setattr(start, "_status", lambda: next(statuses))
    monkeypatch.setattr(start, "_discover", lambda **_kwargs: snapshot)
    monkeypatch.setattr(start, "_auto_join", lambda value: joined.append(value))
    monkeypatch.setattr(start, "_bootstrap_capabilities", _completed_capability)
    monkeypatch.setattr(
        start,
        "_initialize_first_federation",
        lambda: pytest.fail("joining device must not create human credentials"),
    )

    result = start.zero_touch_start(initialize_federation=False, web_port=5000)

    assert joined == [snapshot]
    assert result["node_id"] == "node-b"


def test_join_only_device_refuses_to_create_when_discovery_finds_nothing(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        start,
        "_status",
        lambda: {"state": "not-connected", "node_id": None, "federation_id": None},
    )
    monkeypatch.setattr(start, "_discover", lambda **_kwargs: {"federations": []})
    monkeypatch.setattr(
        start,
        "_initialize_first_federation",
        lambda: pytest.fail("join-only device must never initialize a Federation"),
    )

    with pytest.raises(start.ZeroTouchStartError, match="join-only"):
        start.zero_touch_start(initialize_federation=False, web_port=5000)


def test_first_device_initializes_only_when_explicitly_allowed(monkeypatch) -> None:
    statuses = iter(
        [
            {"state": "not-connected", "node_id": None, "federation_id": None},
            {"state": "connected", "node_id": "node-a", "federation_id": "fed-a"},
        ]
    )
    initialized: list[bool] = []
    monkeypatch.setattr(start, "_status", lambda: next(statuses))
    monkeypatch.setattr(start, "_discover", lambda **_kwargs: {"federations": []})
    monkeypatch.setattr(
        start, "_initialize_first_federation", lambda: initialized.append(True)
    )
    monkeypatch.setattr(start, "_bootstrap_capabilities", _completed_capability)

    result = start.zero_touch_start(initialize_federation=True, web_port=5000)

    assert initialized == [True]
    assert result["federation_id"] == "fed-a"


def test_ambiguous_discovery_never_silently_selects_a_federation(monkeypatch) -> None:
    class Client:
        @staticmethod
        def join_discovered_federation(**_kwargs):
            pytest.fail("ambiguous discovery must fail before requesting a grant")

    monkeypatch.setattr(
        start,
        "load_host_module",
        lambda _name: Client,
    )

    with pytest.raises(start.ZeroTouchStartError, match="more than one"):
        start._auto_join(
            {"federations": [{"federation_id": "one"}, {"federation_id": "two"}]}
        )


def test_machine_setup_checks_live_membership_without_competing_with_flask(
    monkeypatch,
    isolated_host_setup,
) -> None:
    events = isolated_host_setup
    flask_running = True

    def run(command, **kwargs):
        nonlocal flask_running
        assert events[0] == "lock"
        assert "unlock" not in events
        events.append(tuple(command))
        if command == ["docker", "compose", "stop", "flask"]:
            flask_running = False
            return subprocess.CompletedProcess(command, 0, "", "")
        if command == ["docker", "compose", "start", "flask"]:
            flask_running = True
            return subprocess.CompletedProcess(command, 0, "", "")
        assert flask_running is False, "a helper must not compete with live Flask"
        assert command[:10] == [
            "docker",
            "compose",
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "--entrypoint",
            "python",
            "flask",
            "-m",
        ]
        assert kwargs["cwd"] == start.ROOT
        module = command[10]
        if module == "catalog.flask_app.services.zero_touch_federation_cli":
            assert command[11:] == ["--json", "status"]
            value = {
                "state": "connected",
                "node_id": "node-a",
                "federation_id": "fed-a",
            }
        else:
            assert module == "catalog.flask_app.services.automatic_capability_bootstrap"
            assert command[11:] == ["--json"]
            value = _completed_capability()
        return subprocess.CompletedProcess(command, 0, json.dumps(value), "")

    monkeypatch.setattr(start.subprocess, "run", run)

    result = start.zero_touch_start(initialize_federation=False, web_port=5000)

    assert result["federation_state"] == "connected"
    assert flask_running is True
    assert events[-2:] == [("docker", "compose", "start", "flask"), "unlock"]
    assert (
        len(events) == 7
    )  # lock, stop, two live status checks, bootstrap, start, unlock


@pytest.mark.parametrize("failed_stage", ["status", "bootstrap"])
def test_setup_failure_restarts_flask_without_join_or_initialization_fallback(
    monkeypatch,
    isolated_host_setup,
    failed_stage,
) -> None:
    events = isolated_host_setup

    def fail():
        raise start.ZeroTouchStartError("actual setup failure")

    monkeypatch.setattr(
        start,
        "_status",
        fail if failed_stage == "status" else lambda: {"state": "connected"},
    )
    monkeypatch.setattr(start, "_bootstrap_capabilities", fail)
    monkeypatch.setattr(
        start,
        "_discover",
        lambda **_kwargs: pytest.fail("no fallback after setup error"),
    )
    monkeypatch.setattr(
        start,
        "_initialize_first_federation",
        lambda: pytest.fail("no implicit initialization"),
    )

    with pytest.raises(start.ZeroTouchStartError, match="actual setup failure"):
        start.zero_touch_start(initialize_federation=False, web_port=5000)

    assert events == [
        "lock",
        ("docker", "compose", "stop", "flask"),
        ("docker", "compose", "start", "flask"),
        "unlock",
    ]


def test_failed_stop_prevents_machine_setup(monkeypatch, isolated_host_setup) -> None:
    events = isolated_host_setup

    def run(command, **_kwargs):
        assert command == ["docker", "compose", "stop", "flask"]
        events.append(tuple(command))
        return subprocess.CompletedProcess(command, 1, "", "stop refused")

    monkeypatch.setattr(start.subprocess, "run", run)
    monkeypatch.setattr(start, "_status", lambda: pytest.fail("stop did not succeed"))

    with pytest.raises(start.ZeroTouchStartError, match="stop refused"):
        start.zero_touch_start(initialize_federation=False, web_port=5000)

    assert events == ["lock", ("docker", "compose", "stop", "flask"), "unlock"]


@pytest.mark.parametrize("setup_fails", [False, True])
def test_restart_failure_is_reported_without_masking_setup_failure(
    monkeypatch,
    isolated_host_setup,
    capsys,
    setup_fails,
) -> None:
    def run(command, **_kwargs):
        if command == ["docker", "compose", "start", "flask"]:
            return subprocess.CompletedProcess(command, 1, "", "restart refused")
        return subprocess.CompletedProcess(command, 0, "", "")

    def status():
        if setup_fails:
            raise start.ZeroTouchStartError("original setup failure")
        return {"state": "connected"}

    monkeypatch.setattr(start.subprocess, "run", run)
    monkeypatch.setattr(start, "_status", status)
    monkeypatch.setattr(start, "_bootstrap_capabilities", _completed_capability)
    expected = "original setup failure" if setup_fails else "restart refused"

    with pytest.raises(start.ZeroTouchStartError, match=expected):
        start.zero_touch_start(initialize_federation=False, web_port=5000)

    if setup_fails:
        assert "restart refused" in capsys.readouterr().err
