from __future__ import annotations

from types import SimpleNamespace

import pytest

from scripts import zero_touch_federation_start as start


def _completed_capability() -> dict[str, object]:
    return {
        "startup_completed": True,
        "benchmarks_run": 3,
        "contributions_enabled": 2,
    }


def test_existing_member_reuses_membership_and_bootstraps_idempotently(monkeypatch) -> None:
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


def test_joining_device_auto_joins_the_single_discovered_federation(monkeypatch) -> None:
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


def test_join_only_device_refuses_to_create_when_discovery_finds_nothing(monkeypatch) -> None:
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
