from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.acceptance import v1_physical_campaign as campaign

COMMIT = "a" * 40


def _ready(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    system: str = "Linux",
) -> tuple[Path, Path]:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    root = checkout / "evidence" / "v1-physical"
    monkeypatch.setattr(
        campaign,
        "verify_checkout",
        lambda *_args, **_kwargs: {"commit_sha": COMMIT},
    )
    monkeypatch.setattr(campaign.platform, "system", lambda: system)
    monkeypatch.setattr(campaign.platform, "node", lambda: "private-hostname")
    campaign.initialize(checkout, root, commit=COMMIT, operator="Martin")
    campaign.register_host(
        checkout,
        root,
        commit=COMMIT,
        host="test-host",
        role="test",
    )
    return checkout, root


def test_campaign_defines_all_corrected_p01_p12_scenarios() -> None:
    assert list(campaign.SCENARIOS) == [
        f"P{index:02d}" for index in range(1, 13)
    ]
    assert campaign.SCENARIOS["P07"].minimum_elapsed_seconds == 3600
    assert campaign.SCENARIOS["P12"].minimum_elapsed_seconds == 24 * 60 * 60
    assert "failed-copy-safe" in campaign.SCENARIOS["P11"].assertions
    assert "accelerated-ceilings" in campaign.SCENARIOS["P12"].assertions


def test_observation_is_commit_bound_and_redacted(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = _ready(monkeypatch, tmp_path)
    path = campaign.observe(
        checkout,
        root,
        commit=COMMIT,
        host="test-host",
        scenario="P05",
        assertion="global-invariant",
        status="pass",
        note="probe=http://192.168.1.50:5000 token=private-token",
    )
    packet = json.loads(path.read_text(encoding="utf-8"))
    serialized = json.dumps(packet)
    assert packet["candidate_sha"] == COMMIT
    assert packet["status"] == "pass"
    assert "192.168.1.50" not in serialized
    assert "private-token" not in serialized


def test_os_specific_assertions_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = _ready(monkeypatch, tmp_path, system="Linux")
    with pytest.raises(campaign.CampaignError, match="windows"):
        campaign.observe(
            checkout,
            root,
            commit=COMMIT,
            host="test-host",
            scenario="P06",
            assertion="unexpected-child-restart",
            status="pass",
            note="should not be accepted from POSIX",
        )


def test_p07_requires_all_assertions_duration_and_samples(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = _ready(monkeypatch, tmp_path)
    spec = campaign.SCENARIOS["P07"]
    for assertion in spec.assertions:
        campaign.observe(
            checkout,
            root,
            commit=COMMIT,
            host="test-host",
            scenario="P07",
            assertion=assertion,
            status="pass",
            note="observed",
        )
    begin = campaign.base_packet(
        root,
        commit=COMMIT,
        host="test-host",
        scenario="P07",
        kind="begin",
    )
    begin.update(
        {"run_id": "run", "recorded_at": "2026-09-02T10:00:00Z"}
    )
    campaign.write_packet(root, begin)
    finish = campaign.base_packet(
        root,
        commit=COMMIT,
        host="test-host",
        scenario="P07",
        kind="finish",
    )
    finish.update(
        {
            "run_id": "run",
            "recorded_at": "2026-09-02T11:00:00Z",
            "elapsed_seconds": 3600,
        }
    )
    campaign.write_packet(root, finish)
    for index in range(2):
        sample = campaign.base_packet(
            root,
            commit=COMMIT,
            host="test-host",
            scenario="P07",
            kind="sample",
        )
        sample["label"] = f"sample-{index}"
        campaign.write_packet(root, sample)
    status = campaign.scenario_status(root, "P07")
    assert status["passed"] is True
    assert status["elapsed_seconds"] == 3600
    assert status["sample_count"] == 2


def test_failed_assertion_overrides_earlier_pass(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _checkout, root = _ready(monkeypatch, tmp_path)
    first = campaign.base_packet(
        root,
        commit=COMMIT,
        host="test-host",
        scenario="P08",
        kind="assertion",
    )
    first.update(
        {
            "assertion": "allocation-floor",
            "status": "pass",
            "recorded_at": "2026-09-02T10:00:00Z",
        }
    )
    campaign.write_packet(root, first)
    second = campaign.base_packet(
        root,
        commit=COMMIT,
        host="test-host",
        scenario="P08",
        kind="assertion",
    )
    second.update(
        {
            "assertion": "allocation-floor",
            "status": "fail",
            "recorded_at": "2026-09-02T10:01:00Z",
        }
    )
    campaign.write_packet(root, second)
    status = campaign.scenario_status(root, "P08")
    assert "allocation-floor" in status["failing_assertions"]
    assert status["passed"] is False


def test_privacy_digest_invalidates_after_new_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = _ready(monkeypatch, tmp_path)
    campaign.privacy_check(checkout, root, commit=COMMIT)
    digest_before = json.loads(
        (root / "privacy.json").read_text(encoding="utf-8")
    )["evidence_sha256"]

    extra = campaign.base_packet(
        root,
        commit=COMMIT,
        host="test-host",
        scenario="P01",
        kind="sample",
    )
    extra["label"] = "later-safe-sample"
    campaign.write_packet(root, extra)

    digest_after, _count = campaign._privacy_digest(root)
    assert digest_after != digest_before
    result = campaign.validate_campaign(checkout, root, commit=COMMIT)
    assert result["privacy_ok"] is False
    assert result["accepted"] is False


def test_not_applicable_is_allowed_only_when_declared(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = _ready(monkeypatch, tmp_path)
    campaign.observe(
        checkout,
        root,
        commit=COMMIT,
        host="test-host",
        scenario="P02",
        assertion="inode-pressure",
        status="not-applicable",
        note="filesystem does not expose inode accounting",
    )
    with pytest.raises(campaign.CampaignError, match="cannot be marked"):
        campaign.observe(
            checkout,
            root,
            commit=COMMIT,
            host="test-host",
            scenario="P02",
            assertion="data-pressure",
            status="not-applicable",
            note="not allowed",
        )
