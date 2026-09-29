"""Persisted native check results retain bounded trial evidence in reports."""

import hashlib
import json

import pytest

from catalog.flask_app.services.federation_update_events import (
    inspection_from_report,
    report_payload,
)
from catalog.flask_app.services.federation_update_handoff import (
    MAX_HANDOFF_BYTES,
    RESULT_SCHEMA,
    HostUpdateHandoff,
)

MAIN = "1" * 40
TRIAL = "2" * 40
PREVIOUS_MAIN = "3" * 40
REQUEST = "fed-current-check"


def _document(active=False):
    # Retained trial history can name an older safe build after a normal update.
    return {
        "schema": RESULT_SCHEMA,
        "request_id": REQUEST,
        "state": "update_available" if active else "up_to_date",
        "current_commit": TRIAL if active else MAIN,
        "target_commit": MAIN,
        "running_commit": TRIAL if active else MAIN,
        "code": None,
        "message": "verified",
        "trial": {
            "stage": "trial_running",
            "branch": "fix/earlier-trial",
            "commit": TRIAL,
            "safe_branch": "main",
            "safe_commit": PREVIOUS_MAIN,
            "failure_reason": None,
            "active": active,
        },
    }


def _persist(tmp_path, document):
    raw = json.dumps(document).encode("utf-8")
    digest = hashlib.sha256(REQUEST.encode("utf-8")).hexdigest()
    paths = (tmp_path / f"result-{digest}.json", tmp_path / "result.json")
    for path in paths:
        path.write_bytes(raw)
    return HostUpdateHandoff(tmp_path), paths, raw


def _read(handoff, reader):
    return handoff.result_for(REQUEST) if reader == "request" else handoff.latest_result()


@pytest.mark.parametrize("reader", ["request", "latest"])
@pytest.mark.parametrize("active", [False, True])
def test_persisted_trial_summary_survives_handoff_and_authoritative_report(tmp_path, reader, active):
    document = _document(active)
    handoff, paths, raw = _persist(tmp_path, document)

    result = _read(handoff, reader)
    assert result is not None and result.request_id == REQUEST
    assert result.trial == document["trial"]
    reported = report_payload(request_id="current-check", node_id="native", result=result)
    decoded = inspection_from_report(reported)
    assert decoded is not None
    request_id, node_id, inspection = decoded
    assert (request_id, node_id) == ("current-check", "native")
    assert inspection.trial == document["trial"]
    for field in ("state", "current_commit", "target_commit", "running_commit", "code", "message"):
        assert getattr(inspection, field) == document[field]
    assert all(path.read_bytes() == raw for path in paths)
    assert not handoff.request_file.exists()


@pytest.mark.parametrize("reader", ["request", "latest"])
@pytest.mark.parametrize("summary", [None, [], "invalid", {}, {"stage": "unknown"}, {"stage": []}])
def test_invalid_or_legacy_trial_summary_does_not_fabricate_inactive_evidence(tmp_path, reader, summary):
    document = _document()
    document["trial"] = summary
    handoff, paths, raw = _persist(tmp_path, document)
    result = _read(handoff, reader)
    assert result is not None and result.trial is None
    reported = report_payload(request_id="current-check", node_id="native", result=result)
    assert "trial" not in reported
    assert inspection_from_report(reported)[2].trial is None
    assert all(path.read_bytes() == raw for path in paths)


def test_legacy_result_without_trial_remains_additive(tmp_path):
    document = _document()
    del document["trial"]
    handoff, _, _ = _persist(tmp_path, document)
    for result in (handoff.result_for(REQUEST), handoff.latest_result()):
        assert result is not None and result.trial is None
        assert "trial" not in report_payload(request_id="current-check", node_id="native", result=result)


def test_handoff_bounds_summary_fields_and_does_not_coerce_inactive_claim(tmp_path):
    document = _document()
    document["trial"].update({
        "active": "false", "branch": "--invalid", "commit": "invalid",
        "safe_branch": [], "safe_commit": 123, "failure_reason": "x" * 300,
        "untrusted_extra": {"command": "never-forward"},
    })
    handoff, paths, raw = _persist(tmp_path, document)
    result = handoff.result_for(REQUEST)
    expected = {
        "stage": "trial_running", "branch": None, "commit": None,
        "safe_branch": None, "safe_commit": None, "failure_reason": "x" * 256,
        "active": True,
    }
    assert result is not None and result.trial == expected
    assert report_payload(request_id="current-check", node_id="native", result=result)["trial"] == expected
    assert all(path.read_bytes() == raw for path in paths)


@pytest.mark.parametrize("fault", ["request", "schema", "oversize"])
def test_trial_summary_does_not_bypass_existing_result_admission(tmp_path, fault):
    document = _document()
    if fault == "request":
        document["request_id"] = "fed-some-other-check"
    elif fault == "schema":
        document["schema"] = "unknown"
    else:
        document["message"] = "x" * MAX_HANDOFF_BYTES
    handoff, paths, raw = _persist(tmp_path, document)
    assert handoff.result_for(REQUEST) is None
    if fault != "request":
        assert handoff.latest_result() is None
    assert handoff.result_for("absent-check") is None
    assert all(path.read_bytes() == raw for path in paths)
