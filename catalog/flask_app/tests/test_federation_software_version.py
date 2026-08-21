"""Federation software-version selection: what a peer can and cannot ask for.

The recorder-side regressions prove what happens on a device. These prove the
coordinator side: that the dropdown can only ever offer approved-repository
branches, that the command a leader publishes has no field a path, URL, command
or environment value fits in, and that the existing "Update all devices" flow is
untouched by any of it.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from catalog.federation.software_trial import (
    TRIAL_RUNNING,
    TrialSelection,
    validate_trial_summary,
)
from catalog.federation.software_update import (
    APPROVED_REPOSITORY,
    ApprovedBranch,
    UpdateInspection,
)
from catalog.flask_app.services import (
    federation_software_version_service as module,
)
from catalog.flask_app.services.federation_software_version_service import (
    FederationSoftwareVersionService,
)
from catalog.flask_app.services.federation_update_events import (
    TRIAL_REPORT_EVENT,
    TRIAL_REQUEST_EVENT,
    report_payload,
    trial_report_payload,
    validate_trial_command_payload,
)

ACTOR = "node-owner"
RECORDER = "node-recorder"
OFFLINE = "node-offline"
SAFE = "1" * 40
TRIAL = "2" * 40
BRANCH = "fix/new-recorder-behaviour"


class _Local:
    """A local adapter that can list branches, like the host handoff can."""

    def __init__(self, branches: tuple[ApprovedBranch, ...] = ()) -> None:
        self._branches = branches
        self.failure: Exception | None = None

    def approved_branches(self) -> tuple[ApprovedBranch, ...]:
        if self.failure is not None:
            raise self.failure
        return self._branches


class _Coordinator:
    def __init__(self, events: tuple[Any, ...] = ()) -> None:
        self.appended: list[dict[str, Any]] = []
        self._events = events
        self.coordinator_id = "coordinator-one"

    def append_event(self, **kwargs: Any) -> object:
        self.appended.append(dict(kwargs))
        return SimpleNamespace(revision=len(self.appended))

    def replay_page(self, **_kwargs: Any) -> tuple[tuple[Any, ...], int]:
        return self._events, len(self._events)


class _Authority:
    devices: tuple[object, ...] = ()

    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        pass

    def snapshot(self) -> object:
        return SimpleNamespace(available=True, devices=self.devices)


def _device(node_id: str, state: str, label: str) -> object:
    return SimpleNamespace(node_id=node_id, state=state, label=label)


def _event(node_id: str, payload: dict[str, object], revision: int = 1) -> object:
    return SimpleNamespace(
        event_type=TRIAL_REPORT_EVENT,
        actor_node_id=node_id,
        payload=payload,
        revision=revision,
        session_id="session-one",
    )


def _install(
    monkeypatch: pytest.MonkeyPatch,
    coordinator: _Coordinator,
    devices: tuple[object, ...],
    *,
    leader: str = ACTOR,
    update_rows: tuple[dict[str, Any], ...] = (),
) -> None:
    context = SimpleNamespace(
        coordinator=coordinator,
        binding=SimpleNamespace(internal_session_id="session-one"),
        credentials=SimpleNamespace(identity=SimpleNamespace(node_id=ACTOR)),
    )
    monkeypatch.setattr(
        module,
        "get_capability_onboarding_service",
        lambda: SimpleNamespace(authorized_context=lambda: context),
    )
    monkeypatch.setattr(
        module,
        "require_federation_leader",
        lambda _context: SimpleNamespace(leader_node_id=leader),
    )
    _Authority.devices = devices
    monkeypatch.setattr(module, "FederationAuthorityAdapter", _Authority)
    monkeypatch.setattr(
        module,
        "get_active_update_service",
        lambda: SimpleNamespace(
            snapshot=lambda: {"devices": list(update_rows)},
            local=None,
        ),
    )


def _service(tmp_path: Path, local: _Local | None = None) -> Any:
    return FederationSoftwareVersionService(
        local or _Local(), tmp_path / "software-version.json"
    )


# --------------------------------------------------------------------------
# the dropdown
# --------------------------------------------------------------------------


def test_the_dropdown_only_ever_offers_approved_repository_branches(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    local = _Local(
        (
            ApprovedBranch(BRANCH, TRIAL),
            ApprovedBranch("main", SAFE),
            # Names this product would refuse to hand to Git are never offered.
            ApprovedBranch("--upload-pack=/bin/sh", TRIAL),
            ApprovedBranch("https://evil.example/repo.git", TRIAL),
            ApprovedBranch("main", "not-a-commit"),
        )
    )

    listed = _service(tmp_path, local).branches()

    assert [item["name"] for item in listed] == ["main", BRANCH]
    assert listed[0]["commit"] == SAFE


def test_a_host_agent_that_cannot_list_branches_leaves_only_main(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """An unavailable list is an empty dropdown, never a broken page."""

    local = _Local()
    local.failure = RuntimeError("remote_unavailable")

    assert _service(tmp_path, local).branches() == ()


# --------------------------------------------------------------------------
# what a leader may publish
# --------------------------------------------------------------------------


def test_switching_publishes_one_bounded_command_to_reachable_devices(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    coordinator = _Coordinator()
    _install(
        monkeypatch,
        coordinator,
        (
            _device(ACTOR, "connected", "Owner"),
            _device(RECORDER, "connected", "recorder-01"),
            _device(OFFLINE, "disconnected", "recorder-02"),
        ),
    )
    service = _service(tmp_path)

    result = service.switch_version(
        node_ids=[RECORDER, OFFLINE, ACTOR], branch=BRANCH, target_commit=TRIAL
    )

    assert result["expected_report_node_ids"] == [RECORDER]
    published = [
        item for item in coordinator.appended
        if item["event_type"] == TRIAL_REQUEST_EVENT
    ]
    assert len(published) == 1
    payload, selection = validate_trial_command_payload(published[0]["payload"])
    assert selection == TrialSelection(APPROVED_REPOSITORY, BRANCH, TRIAL)
    assert payload["target_node_ids"] == [RECORDER]
    # No path, URL, command, interpreter or environment value can travel here.
    assert set(payload) == {
        "schema",
        "request_id",
        "target_node_ids",
        "created_at",
        "expires_at",
        "repository",
        "branch",
        "target_commit",
    }


def test_the_coordinator_serving_the_page_is_never_a_trial_target(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    coordinator = _Coordinator()
    _install(monkeypatch, coordinator, (_device(ACTOR, "connected", "Owner"),))

    with pytest.raises(ValueError) as refused:
        _service(tmp_path).switch_version(
            node_ids=[ACTOR], branch=BRANCH, target_commit=TRIAL
        )

    assert str(refused.value) == "no_reachable_selected_devices"
    assert coordinator.appended == []


def test_an_unknown_or_offline_device_is_never_queued(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    coordinator = _Coordinator()
    _install(
        monkeypatch,
        coordinator,
        (
            _device(ACTOR, "connected", "Owner"),
            _device(OFFLINE, "disconnected", "recorder-02"),
        ),
    )

    with pytest.raises(ValueError):
        _service(tmp_path).switch_version(
            node_ids=[OFFLINE, "node-never-seen"],
            branch=BRANCH,
            target_commit=TRIAL,
        )

    assert coordinator.appended == []


@pytest.mark.parametrize(
    ("branch", "commit"),
    [
        ("--upload-pack=/bin/sh", TRIAL),
        ("../../etc/passwd", TRIAL),
        ("https://evil.example/repo.git", TRIAL),
        ("main;rm -rf /", TRIAL),
        (BRANCH, "HEAD"),
        (BRANCH, "$(rm -rf /)"),
        (BRANCH, "refs/heads/main"),
        ("", TRIAL),
        (BRANCH, ""),
    ],
)
def test_no_path_url_or_command_can_be_smuggled_through_a_switch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    branch: str,
    commit: str,
) -> None:
    coordinator = _Coordinator()
    _install(
        monkeypatch,
        coordinator,
        (_device(ACTOR, "connected", "Owner"), _device(RECORDER, "connected", "r")),
    )

    with pytest.raises(ValueError):
        _service(tmp_path).switch_version(
            node_ids=[RECORDER], branch=branch, target_commit=commit
        )

    assert coordinator.appended == []


def test_only_the_current_leader_may_change_a_software_version(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    coordinator = _Coordinator()
    _install(
        monkeypatch,
        coordinator,
        (_device(RECORDER, "connected", "recorder-01"),),
        leader="node-someone-else",
    )

    with pytest.raises(PermissionError):
        _service(tmp_path).switch_version(
            node_ids=[RECORDER], branch=BRANCH, target_commit=TRIAL
        )

    assert coordinator.appended == []


# --------------------------------------------------------------------------
# what the page shows
# --------------------------------------------------------------------------


def test_a_device_on_main_shows_its_branch_and_exact_commit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install(
        monkeypatch,
        _Coordinator(),
        (_device(RECORDER, "connected", "recorder-01"),),
        update_rows=(
            {
                "node_id": RECORDER,
                "state": "up_to_date",
                "current_commit": SAFE,
                "running_commit": SAFE,
            },
        ),
    )

    snapshot = _service(tmp_path).snapshot()

    row = snapshot["devices"][0]
    assert row["label"] == "recorder-01"
    assert row["branch"] == "main"
    assert row["commit"] == SAFE
    assert row["on_test_branch"] is False


def test_a_device_on_a_test_branch_shows_the_branch_commit_and_fallback(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install(
        monkeypatch,
        _Coordinator(),
        (_device(RECORDER, "connected", "recorder-01"),),
        update_rows=(
            {
                "node_id": RECORDER,
                "state": "trial_running",
                "current_commit": SAFE,
                "running_commit": TRIAL,
                "trial": validate_trial_summary(
                    {
                        "stage": TRIAL_RUNNING,
                        "branch": BRANCH,
                        "commit": TRIAL,
                        "safe_branch": "main",
                        "safe_commit": SAFE,
                        "active": True,
                    }
                ),
            },
        ),
    )

    row = _service(tmp_path).snapshot()["devices"][0]

    assert row["branch"] == BRANCH
    assert row["commit"] == TRIAL
    assert row["on_test_branch"] is True
    assert row["safe_branch"] == "main"
    assert row["safe_commit"] == SAFE


def test_a_reported_recovery_is_shown_exactly_as_the_device_proved_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    document = trial_report_payload(
        request_id="trial-one",
        node_id=RECORDER,
        document={
            "state": "safe_restored",
            "code": "safe_restored",
            "message": "Recorder heartbeat did not become healthy within 60 seconds.",
            "branch": BRANCH,
            "target_commit": TRIAL,
            "running_commit": SAFE,
            "safe_branch": "main",
            "safe_commit": SAFE,
            "recovery": {
                "trial_process_stopped": True,
                "safe_version_started": True,
                "recorder_running": True,
                "federation_connected": True,
                "recording_resumed": True,
            },
        },
    )
    _install(
        monkeypatch,
        _Coordinator((_event(RECORDER, document),)),
        (_device(RECORDER, "connected", "recorder-01"),),
    )
    service = _service(tmp_path)
    service._save(
        {
            "schema": module.SCHEMA,
            "status": "switching",
            "request_id": "trial-one",
            "expected_report_node_ids": [RECORDER],
            "devices": [],
        }
    )

    snapshot = service.snapshot()

    row = snapshot["devices"][0]
    assert snapshot["status"] == "reported"
    assert row["trial_state"] == "safe_restored"
    assert row["branch"] == "main"
    assert row["commit"] == SAFE
    assert row["on_test_branch"] is False
    assert row["recovery"]["recording_resumed"] is True
    assert "60 seconds" in row["message"]


def test_a_device_that_never_reports_is_marked_rather_than_assumed_healthy(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install(
        monkeypatch,
        _Coordinator(),
        (_device(RECORDER, "connected", "recorder-01"),),
    )
    service = _service(tmp_path)
    service._save(
        {
            "schema": module.SCHEMA,
            "status": "switching",
            "request_id": "trial-one",
            "expected_report_node_ids": [RECORDER],
            "report_deadline": "2000-01-01T00:00:00Z",
            "devices": [],
        }
    )

    row = service.snapshot()["devices"][0]

    assert row["trial_state"] == "trial_report_timeout"
    assert "automatic fallback" in row["message"]


# --------------------------------------------------------------------------
# the existing update contract is untouched
# --------------------------------------------------------------------------


def test_an_update_report_from_a_device_with_no_trial_carries_no_trial_field() -> None:
    payload = report_payload(
        request_id="check-one",
        node_id=RECORDER,
        result=UpdateInspection("up_to_date", SAFE, SAFE, running_commit=SAFE),
    )

    assert "trial" not in payload


def test_a_malformed_trial_summary_is_dropped_rather_than_passed_through() -> None:
    for hostile in (
        {"stage": "../../etc/passwd"},
        {"stage": "trial_running", "branch": "--upload-pack=x", "commit": TRIAL},
        "not-a-dict",
        None,
    ):
        summary = validate_trial_summary(hostile)
        assert summary is None or summary["branch"] is None


def test_a_trial_report_keeps_only_the_bounded_shape() -> None:
    payload = trial_report_payload(
        request_id="trial-one",
        node_id=RECORDER,
        document={
            "state": "trial_running",
            "branch": BRANCH,
            "target_commit": TRIAL,
            "safe_commit": SAFE,
            # None of these are fields; they must not survive.
            "launch_root": "C:/evil",
            "interpreter": "/bin/sh",
            "command": "rm -rf /",
            "env": {"PATH": "/evil"},
        },
    )

    assert set(payload) == {
        "schema",
        "request_id",
        "node_id",
        "state",
        "code",
        "message",
        "branch",
        "target_commit",
        "running_commit",
        "safe_branch",
        "safe_commit",
        "acceptance",
        "recovery",
        "reported_at",
    }


# --------------------------------------------------------------------------
# the browser surface
# --------------------------------------------------------------------------


class _VersionService:
    def __init__(self, snapshot: dict[str, Any]) -> None:
        self._snapshot = snapshot
        self.calls: list[dict[str, Any]] = []

    def snapshot(self) -> dict[str, Any]:
        return dict(self._snapshot)

    def switch_version(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(dict(kwargs))
        return {"expected_report_node_ids": kwargs["node_ids"]}


def _routes_context(actor: str, creator: str) -> object:
    return SimpleNamespace(
        coordinator=SimpleNamespace(
            store=SimpleNamespace(
                get_session=lambda _s: SimpleNamespace(created_by_node_id=creator)
            )
        ),
        binding=SimpleNamespace(internal_session_id="session-one"),
        credentials=SimpleNamespace(identity=SimpleNamespace(node_id=actor)),
    )


def _install_routes(
    monkeypatch: pytest.MonkeyPatch,
    service: _VersionService,
    *,
    actor: str = ACTOR,
    creator: str = ACTOR,
) -> Any:
    from catalog.flask_app import federation_routes as routes
    from catalog.flask_app.app import create_app

    monkeypatch.setattr(
        routes, "get_federation_software_version_service", lambda: service
    )
    monkeypatch.setattr(
        routes,
        "get_capability_onboarding_service",
        lambda: SimpleNamespace(
            authorized_context=lambda: _routes_context(actor, creator)
        ),
    )
    monkeypatch.setattr(
        routes,
        "get_federation_update_service",
        lambda: SimpleNamespace(
            snapshot=lambda: {"status": "checked", "devices": [], "eligible_count": 0}
        ),
    )
    app = create_app()
    app.config.update(TESTING=True)
    return app


def _overview(app: Any) -> str:
    return app.test_client().get("/federation").get_data(as_text=True)


def test_the_page_shows_branch_commit_state_and_fallback_per_device(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _VersionService(
        {
            "status": "reported",
            "repository": APPROVED_REPOSITORY,
            "branches": [
                {"name": "main", "commit": SAFE},
                {"name": BRANCH, "commit": TRIAL},
            ],
            "devices": [
                {
                    "node_id": RECORDER,
                    "label": "recorder-01",
                    "connected": True,
                    "branch": BRANCH,
                    "commit": TRIAL,
                    "on_test_branch": True,
                    "software_state": "trial_running",
                    "safe_branch": "main",
                    "safe_commit": SAFE,
                    "failure_reason": None,
                    "message": None,
                    "recovery": None,
                }
            ],
        }
    )

    page = _overview(_install_routes(monkeypatch, service))

    assert "Software version" in page
    assert "recorder-01" in page
    assert BRANCH in page
    assert TRIAL[:8] in page
    assert "Fallback:" in page
    assert SAFE[:8] in page
    assert "Test branch" in page
    assert "Switch version" in page
    # The exact commit travels with the branch it was published on.
    assert f'value="{TRIAL}:{BRANCH}"' in page


def test_the_page_shows_the_automatic_recovery_that_actually_happened(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _VersionService(
        {
            "status": "reported",
            "branches": [{"name": "main", "commit": SAFE}],
            "devices": [
                {
                    "node_id": RECORDER,
                    "label": "recorder-01",
                    "connected": True,
                    "branch": "main",
                    "commit": SAFE,
                    "on_test_branch": False,
                    "software_state": "safe_restored",
                    "safe_branch": "main",
                    "safe_commit": SAFE,
                    "failure_reason": (
                        "Recorder heartbeat did not become healthy within 60 seconds."
                    ),
                    "message": None,
                    "recovery": {
                        "trial_process_stopped": True,
                        "safe_version_started": True,
                        "recorder_running": True,
                        "federation_connected": True,
                        "recording_resumed": True,
                    },
                }
            ],
        }
    )

    page = _overview(_install_routes(monkeypatch, service))

    assert "Recorder heartbeat did not become healthy within 60 seconds." in page
    assert "✓ test process stopped" in page
    assert "✓ known-good restored" in page
    assert "✓ recording resumed" in page


def test_a_member_that_is_not_the_leader_never_sees_the_control(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _VersionService(
        {"status": "idle", "branches": [{"name": "main", "commit": SAFE}], "devices": []}
    )

    page = _overview(
        _install_routes(monkeypatch, service, actor=RECORDER, creator=ACTOR)
    )

    assert "Switch version" not in page


def test_the_switch_form_requires_csrf_and_never_reaches_the_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _VersionService({"status": "idle", "branches": [], "devices": []})
    app = _install_routes(monkeypatch, service)

    response = app.test_client().post(
        "/federation/software-version/switch",
        data={"version": f"{TRIAL}:{BRANCH}", "node_id": RECORDER},
    )

    assert response.status_code == 403
    assert service.calls == []


def test_the_selected_version_reaches_the_service_as_a_branch_and_a_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _VersionService({"status": "idle", "branches": [], "devices": []})
    app = _install_routes(monkeypatch, service)
    client = app.test_client()
    from catalog.flask_app.capability_onboarding_routes import _CSRF_SESSION_KEY

    client.get("/federation")
    with client.session_transaction() as browser:
        token = str(browser[_CSRF_SESSION_KEY])

    response = client.post(
        "/federation/software-version/switch",
        data={
            "_csrf_token": token,
            "version": f"{TRIAL}:{BRANCH}",
            "node_id": [RECORDER, OFFLINE],
        },
    )

    assert response.status_code == 303
    assert service.calls == [
        {"node_ids": [RECORDER, OFFLINE], "branch": BRANCH, "target_commit": TRIAL}
    ]


# --------------------------------------------------------------------------
# the bounded host handoff
# --------------------------------------------------------------------------


def _handoff(tmp_path: Path) -> Any:
    from catalog.flask_app.services.federation_update_handoff import (
        HostUpdateHandoff,
    )

    return HostUpdateHandoff(tmp_path / "update-agent", timeout=1.0, poll_interval=0.02)


def test_a_trial_is_queued_as_one_declarative_document(tmp_path: Path) -> None:
    import json

    handoff = _handoff(tmp_path)

    queued = handoff.trial(
        TrialSelection(APPROVED_REPOSITORY, BRANCH, TRIAL), request_id="host-trial-1"
    )

    assert queued["state"] == "trial_requested"
    written = json.loads(handoff.request_file.read_text(encoding="utf-8"))
    assert written["schema"] == "fcp.host-trial-request.v1"
    assert written["repository"] == APPROVED_REPOSITORY
    assert written["branch"] == BRANCH
    assert written["target_commit"] == TRIAL
    # Not one field for a path, command, interpreter or environment value.
    assert set(written) == {
        "schema",
        "request_id",
        "action",
        "repository",
        "branch",
        "target_commit",
        "created_at",
        "expires_at",
    }


def test_a_malformed_selection_is_refused_without_queueing_anything(
    tmp_path: Path,
) -> None:
    handoff = _handoff(tmp_path)

    refused = handoff.trial(
        TrialSelection("attacker/evil", BRANCH, TRIAL), request_id="host-trial-2"
    )

    assert refused["state"] == "refused"
    assert refused["code"] == "unapproved_source"
    assert not handoff.request_file.exists()


def test_a_host_agent_that_never_answers_lists_no_branches(tmp_path: Path) -> None:
    """The Flask process still runs no Git, so silence means an empty list."""

    assert _handoff(tmp_path).approved_branches() == ()


def test_a_trial_result_is_only_read_back_for_its_own_request(
    tmp_path: Path,
) -> None:
    import hashlib
    import json

    handoff = _handoff(tmp_path)
    handoff.directory.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(b"host-trial-3").hexdigest()
    (handoff.directory / f"trial-result-{digest}.json").write_text(
        json.dumps(
            {
                "schema": "fcp.host-trial-result.v1",
                "request_id": "someone-elses-request",
                "state": TRIAL_RUNNING,
            }
        ),
        encoding="utf-8",
    )

    assert handoff.trial_result_for("host-trial-3") is None


# --------------------------------------------------------------------------
# the POSIX host agent answers the same read-only question
# --------------------------------------------------------------------------


def test_the_posix_host_agent_lists_branches_read_only_and_approved_only() -> None:
    root = Path(__file__).resolve().parents[3]
    posix = (root / "scripts/posix/fcp_update_agent.py").read_text(encoding="utf-8")

    assert 'BRANCHES_REQUEST_SCHEMA = "fcp.host-branches-request.v1"' in posix
    # Behind the same approved-remote check as everything else in that agent.
    assert 'if not approved_remote(git(root, "remote", "get-url", "origin").stdout):' in posix
    assert 'raise RuntimeError("unapproved_remote")' in posix
    # Read-only: a listing, never a fetch, checkout or mutation.
    assert 'git(root, "ls-remote", "--heads", "--", "origin", check=False)' in posix
    assert "BRANCH_RE.fullmatch(name)" in posix
    for forbidden in ("reset --hard", "git clean", "git stash"):
        assert forbidden not in posix
