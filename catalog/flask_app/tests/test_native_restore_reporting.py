"""A restore report keeps request, running-trial and safe-version identities apart."""

from __future__ import annotations

import pytest

from catalog.flask_app.services.federation_software_version_service import (
    FederationSoftwareVersionService,
)
from catalog.flask_app.services.federation_update_events import (
    trial_from_report,
    trial_report_payload,
)

SAFE = "1" * 40
TRIAL = "2" * 40
TRIAL_BRANCH = "fix/native-recorder"


def _report(**changes: object) -> dict[str, object]:
    document = {
        "state": "refused",
        "code": "recorder_runtime_unavailable",
        "branch": "main",
        "target_commit": SAFE,
        "trial_branch": TRIAL_BRANCH,
        "trial_commit": TRIAL,
        "running_commit": TRIAL,
        "safe_branch": "main",
        "safe_commit": SAFE,
        **changes,
    }
    payload = trial_report_payload(
        request_id="restore-request", node_id="node-recorder", document=document
    )
    parsed = trial_from_report(payload)
    assert parsed is not None
    return parsed[2]


def _row(report: dict[str, object]) -> dict[str, object]:
    return FederationSoftwareVersionService._row(
        "node-recorder",
        "Recorder",
        connected=True,
        # The last update check predates the successful branch trial.
        update_row={"running_commit": SAFE, "state": "up_to_date"},
        report=report,
    )


@pytest.mark.parametrize("state", ["refused", "error"])
def test_restore_refusal_keeps_the_proved_running_trial_and_restore_pin(state: str) -> None:
    report = _report(state=state)
    row = _row(report)

    assert report["target_commit"] == SAFE
    assert report["branch"] == "main"
    assert report["trial_commit"] == TRIAL
    assert report["trial_branch"] == TRIAL_BRANCH
    assert row["software_state"] == state
    assert row["on_test_branch"] is True
    assert row["branch"] == TRIAL_BRANCH
    assert row["commit"] == TRIAL
    assert row["safe_commit"] == SAFE


@pytest.mark.parametrize("running_commit", [None, SAFE, "3" * 40])
def test_restore_refusal_does_not_infer_a_running_trial_from_history(
    running_commit: str | None,
) -> None:
    row = _row(_report(running_commit=running_commit))

    assert row["on_test_branch"] is False
    assert row["branch"] == "main"


def test_successful_restore_uses_the_safe_version_despite_retained_trial_history() -> None:
    row = _row(_report(state="safe_restored", running_commit=SAFE))

    assert row["on_test_branch"] is False
    assert row["branch"] == "main"
    assert row["commit"] == SAFE


@pytest.mark.parametrize(
    "fields",
    [
        {"trial_branch": "../../bad"},
        {"trial_branch": "--upload-pack=bad"},
        {"trial_commit": "HEAD"},
        {"trial_commit": {"command": "bad"}},
    ],
)
def test_restore_trial_provenance_is_bounded_like_other_report_fields(
    fields: dict[str, object],
) -> None:
    report = _report(**fields)

    assert "trial_branch" not in report
    assert "trial_commit" not in report
    assert _row(report)["on_test_branch"] is False
