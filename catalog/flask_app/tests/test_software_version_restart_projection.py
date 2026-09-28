"""A later authenticated main check supersedes retained trial UI state only."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from catalog.federation.software_trial import TrialSelection, trial_summary
from catalog.federation.software_update import APPROVED_REPOSITORY, UpdateInspection
from catalog.flask_app.services import federation_software_version_service as module
from catalog.flask_app.services.federation_update_events import (
    CHECK_REPORT_EVENT,
    CHECK_REQUEST_EVENT,
    TRIAL_REPORT_EVENT,
    TRIAL_REQUEST_EVENT,
    command_payload,
    report_payload,
    trial_command_payload,
    trial_report_payload,
)
from catalog.flask_app.tests.test_federation_software_version import (
    ACTOR,
    RECORDER,
    SAFE,
    TRIAL,
    _Coordinator,
    _device,
    _install,
    _install_routes,
    _overview,
    _service,
    _VersionService,
)


def _projection(monkeypatch, tmp_path, order, *, fault=None):
    now = datetime.now(timezone.utc)
    trial = trial_summary(
        stage="trial_running", branch="fix/old-trial", commit=TRIAL,
        safe_branch="main", safe_commit=SAFE, failure_reason=None, active=False,
    )
    result = UpdateInspection(
        "up_to_date", current_commit=SAFE, target_commit=SAFE,
        running_commit=SAFE, trial=trial,
    )
    check = command_payload(
        request_id="current-check", target_commit=SAFE,
        target_node_ids=(RECORDER,), created_at=now,
        expires_at=now + timedelta(minutes=5),
    )
    checked = report_payload(request_id="current-check", node_id=RECORDER, result=result)
    requested = trial_command_payload(
        request_id="old-restore", selection=TrialSelection(APPROVED_REPOSITORY, "main", SAFE),
        target_node_ids=(RECORDER,), created_at=now,
        expires_at=now + timedelta(minutes=5),
    )
    old = trial_report_payload(
        request_id="old-restore", node_id=RECORDER,
        document={"state": "trial_requested", "branch": "main", "target_commit": SAFE,
                  "message": "The old restore request was queued."},
    )
    if fault == "wrong-target":
        checked["target_commit"] = TRIAL
    elif fault == "wrong-request":
        checked["request_id"] = "unrelated-check"
    elif fault == "missing-runtime":
        checked["running_commit"] = None
    elif fault == "still-trial":
        checked["trial"] = {**trial, "active": True}
    elif fault == "untargeted":
        check["target_node_ids"] = ["other"]
    elif fault == "failed-check":
        checked["state"] = "error"
    elif fault == "wrong-running":
        checked["running_commit"] = TRIAL
    elif fault == "no-trial-proof":
        checked.pop("trial")
    new_trial = trial_command_payload(
        request_id="new-trial", selection=TrialSelection(APPROVED_REPOSITORY, "fix/new", TRIAL),
        target_node_ids=(RECORDER,), created_at=now,
        expires_at=now + timedelta(minutes=5),
    )
    definitions = {
        "trial-request": (TRIAL_REQUEST_EVENT, ACTOR, requested),
        "trial-report": (TRIAL_REPORT_EVENT, RECORDER, old),
        "check-request": (CHECK_REQUEST_EVENT, "other" if fault == "wrong-authority" else ACTOR, check),
        "check-report": (CHECK_REPORT_EVENT, "other" if fault == "wrong-actor" else RECORDER, checked),
        "new-trial-request": (TRIAL_REQUEST_EVENT, ACTOR, new_trial),
        "later-check-error": (CHECK_REPORT_EVENT, RECORDER, {**checked, "state": "error"}),
    }
    events = []
    for revision, kind in enumerate(order, 1):
        event_type, actor, payload = definitions[kind]
        events.append(SimpleNamespace(event_type=event_type, actor_node_id=actor,
                                      payload=payload, revision=revision, session_id="session-one"))
    _install(monkeypatch, _Coordinator(tuple(events)), (_device(RECORDER, "connected", "native"),))
    snapshot = {"operation": "check", "request_id": "current-check", "target_commit": SAFE,
                "devices": [{"node_id": RECORDER, **result.to_dict()}]}
    if fault == "not-current-check":
        snapshot["request_id"] = "a-newer-check"
    monkeypatch.setattr(module, "get_active_update_service", lambda: SimpleNamespace(snapshot=lambda: snapshot))
    service = _service(tmp_path)
    service._save({"schema": module.SCHEMA, "status": "switching", "request_id": "old-restore",
                   "expected_report_node_ids": [RECORDER], "devices": []})
    return service.snapshot(), old


def test_later_correlated_main_check_releases_stale_restore_ui(monkeypatch, tmp_path):
    snapshot, old = _projection(monkeypatch, tmp_path, (
        "trial-request", "trial-report", "check-request", "check-report",
    ))
    row = snapshot["devices"][0]
    assert row["on_test_branch"] is False
    assert row["branch"] == "main" and row["commit"] == SAFE
    assert row["software_state"] == "up_to_date"
    assert row["trial_state"] is None
    assert row["trial_history"]["state"] == old["state"]
    assert row["trial_history"]["target_commit"] == old["target_commit"]
    assert snapshot["status"] == "reported"


@pytest.mark.parametrize("order", [
    ("check-request", "check-report", "trial-request", "trial-report"),
    ("check-request", "trial-request", "trial-report", "check-report"),
    ("trial-request", "trial-report", "check-request", "check-report", "new-trial-request"),
    ("trial-request", "check-request", "check-report", "trial-report"),
])
def test_older_check_cannot_hide_newer_trial_even_when_reply_is_late(monkeypatch, tmp_path, order):
    snapshot, _ = _projection(monkeypatch, tmp_path, order)
    assert snapshot["devices"][0]["on_test_branch"] is True
    assert snapshot["status"] == "switching"


@pytest.mark.parametrize("fault", [
    "wrong-target", "wrong-request", "missing-runtime", "still-trial", "wrong-authority", "wrong-actor",
    "untargeted", "failed-check", "wrong-running", "no-trial-proof", "not-current-check",
])
def test_unproven_check_cannot_release_retained_trial(monkeypatch, tmp_path, fault):
    snapshot, _ = _projection(monkeypatch, tmp_path, (
        "trial-request", "trial-report", "check-request", "check-report",
    ), fault=fault)
    assert snapshot["devices"][0]["on_test_branch"] is True
    assert snapshot["status"] == "switching"


def test_later_failed_check_report_withdraws_earlier_main_proof(monkeypatch, tmp_path):
    snapshot, _ = _projection(monkeypatch, tmp_path, (
        "trial-request", "trial-report", "check-request", "check-report", "later-check-error",
    ))
    assert snapshot["devices"][0]["on_test_branch"] is True


def test_page_keeps_prior_trial_visible_but_allows_new_selection(monkeypatch, tmp_path):
    snapshot, _ = _projection(monkeypatch, tmp_path, (
        "trial-request", "trial-report", "check-request", "check-report",
    ))
    snapshot["branches"] = [{"name": "main", "commit": SAFE}]
    page = _overview(_install_routes(monkeypatch, _VersionService(snapshot)))
    assert "Earlier trial report: trial requested" in page
    assert "The old restore request was queued." in page
    assert "Previous fallback:" in page
    assert f'name="node_id" value="{RECORDER}" disabled' not in page
    assert 'class="federation-software-version-restore"' not in page
    assert "Switch version" in page
