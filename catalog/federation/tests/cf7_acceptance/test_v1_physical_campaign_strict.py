from __future__ import annotations

from pathlib import Path

import pytest

from scripts.acceptance import v1_physical_campaign as campaign
from scripts.acceptance import v1_physical_campaign_strict as strict

COMMIT = "a" * 40


def _ready(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Path, Path]:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    root = checkout / "evidence" / "v1-physical"
    monkeypatch.setattr(
        campaign,
        "verify_checkout",
        lambda *_args, **_kwargs: {"commit_sha": COMMIT},
    )
    monkeypatch.setattr(campaign.platform, "system", lambda: "Linux")
    monkeypatch.setattr(campaign.platform, "node", lambda: "private-host")
    campaign.initialize(checkout, root, commit=COMMIT, operator="operator")
    campaign.register_host(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        role="test",
    )
    return checkout, root


def _packet(
    root: Path,
    *,
    kind: str,
    recorded_at: str,
    run_id: str,
    assertion: str | None = None,
    status: str | None = None,
) -> None:
    packet = campaign.base_packet(
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P07",
        kind=kind,
    )
    packet["recorded_at"] = recorded_at
    packet["run_id"] = run_id
    if assertion is not None:
        packet["assertion"] = assertion
        packet["status"] = status
    campaign.write_packet(root, packet)


def _complete_run(
    root: Path,
    *,
    run_id: str,
    start: str,
    finish: str,
    assertions: list[str],
) -> None:
    _packet(root, kind="begin", recorded_at=start, run_id=run_id)
    _packet(
        root,
        kind="sample",
        recorded_at="2026-09-02T10:20:00Z",
        run_id=run_id,
    )
    _packet(
        root,
        kind="sample",
        recorded_at="2026-09-02T10:40:00Z",
        run_id=run_id,
    )
    for index, assertion in enumerate(assertions, start=1):
        _packet(
            root,
            kind="assertion",
            recorded_at=f"2026-09-02T10:{index:02d}:00Z",
            run_id=run_id,
            assertion=assertion,
            status="pass",
        )
    _packet(root, kind="finish", recorded_at=finish, run_id=run_id)


def test_timed_observe_requires_active_matching_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = _ready(monkeypatch, tmp_path)
    with pytest.raises(campaign.CampaignError, match="matching begin"):
        strict.timed_observe(
            checkout,
            root,
            commit=COMMIT,
            host="nitro",
            scenario="P07",
            run_id="missing",
            assertion="aged-corpus",
            status="pass",
            note="no active run",
        )


def test_timed_observe_writes_run_binding(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = _ready(monkeypatch, tmp_path)
    run_id, _path = campaign.begin_session(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P07",
    )
    path = strict.timed_observe(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P07",
        run_id=run_id,
        assertion="aged-corpus",
        status="pass",
        note="bound",
    )
    stored = campaign._load_json(path)
    assert stored["run_id"] == run_id
    assert stored["assertion"] == "aged-corpus"


def test_unbound_assertions_cannot_pass_timed_scenario(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _checkout, root = _ready(monkeypatch, tmp_path)
    spec = campaign.SCENARIOS["P07"]
    _complete_run(
        root,
        run_id="run-a",
        start="2026-09-02T10:00:00Z",
        finish="2026-09-02T11:00:00Z",
        assertions=[],
    )
    for assertion in spec.assertions:
        packet = campaign.base_packet(
            root,
            commit=COMMIT,
            host="nitro",
            scenario="P07",
            kind="assertion",
        )
        packet.update(
            {
                "recorded_at": "2026-09-02T10:30:00Z",
                "assertion": assertion,
                "status": "pass",
            }
        )
        campaign.write_packet(root, packet)

    status = strict.strict_scenario_status(
        root,
        "P07",
        expected_commit=COMMIT,
    )
    assert status["passed"] is False
    assert status["missing_assertions"] == list(spec.assertions)


def test_assertions_from_two_runs_cannot_be_combined(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _checkout, root = _ready(monkeypatch, tmp_path)
    assertions = list(campaign.SCENARIOS["P07"].assertions)
    midpoint = len(assertions) // 2

    _complete_run(
        root,
        run_id="run-a",
        start="2026-09-02T10:00:00Z",
        finish="2026-09-02T11:00:00Z",
        assertions=assertions[:midpoint],
    )

    # Second run uses a disjoint time range so every packet is unambiguously
    # attributable to one session.
    _packet(
        root,
        kind="begin",
        recorded_at="2026-09-02T12:00:00Z",
        run_id="run-b",
    )
    _packet(
        root,
        kind="sample",
        recorded_at="2026-09-02T12:20:00Z",
        run_id="run-b",
    )
    _packet(
        root,
        kind="sample",
        recorded_at="2026-09-02T12:40:00Z",
        run_id="run-b",
    )
    for index, assertion in enumerate(assertions[midpoint:], start=1):
        _packet(
            root,
            kind="assertion",
            recorded_at=f"2026-09-02T12:{index:02d}:00Z",
            run_id="run-b",
            assertion=assertion,
            status="pass",
        )
    _packet(
        root,
        kind="finish",
        recorded_at="2026-09-02T13:00:00Z",
        run_id="run-b",
    )

    status = strict.strict_scenario_status(
        root,
        "P07",
        expected_commit=COMMIT,
    )
    assert status["passed"] is False
    assert len(status["timed_sessions"]) == 2
    assert all(not item["passed"] for item in status["timed_sessions"])


def test_one_complete_run_passes_strict_timed_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _checkout, root = _ready(monkeypatch, tmp_path)
    assertions = list(campaign.SCENARIOS["P07"].assertions)
    _complete_run(
        root,
        run_id="run-complete",
        start="2026-09-02T10:00:00Z",
        finish="2026-09-02T11:00:00Z",
        assertions=assertions,
    )

    status = strict.strict_scenario_status(
        root,
        "P07",
        expected_commit=COMMIT,
    )
    assert status["passed"] is True
    assert status["missing_assertions"] == []
    assert status["failing_assertions"] == []
    assert status["elapsed_seconds"] == 3600
    assert status["sample_count"] == 2
