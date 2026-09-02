from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.acceptance import v1_physical_campaign as campaign

COMMIT = "a" * 40
OTHER_COMMIT = "b" * 40


def _ready(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Path, Path]:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    root = checkout / "evidence" / "v1-physical"
    monkeypatch.setattr(campaign, "verify_checkout", lambda *_args, **_kwargs: {"commit_sha": COMMIT})
    monkeypatch.setattr(campaign.platform, "system", lambda: "Linux")
    monkeypatch.setattr(campaign.platform, "node", lambda: "private-linux-host")
    campaign.initialize(checkout, root, commit=COMMIT, operator="Martin")
    campaign.register_host(checkout, root, commit=COMMIT, host="nitro", role="school-control")
    return checkout, root


def test_imported_packet_from_other_candidate_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _checkout, root = _ready(monkeypatch, tmp_path)
    path = root / "observations" / "P08" / "wrong-candidate.json"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "schema": campaign.PACKET_SCHEMA,
                "kind": "assertion",
                "candidate_sha": OTHER_COMMIT,
                "scenario": "P08",
                "host_id": "nitro",
                "host_fingerprint": "deadbeef",
                "os_category": "posix",
                "recorded_at": "2026-09-02T12:00:00Z",
                "assertion": "allocation-floor",
                "status": "pass",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(campaign.CampaignError, match="different candidate"):
        campaign.scenario_status(root, "P08", expected_commit=COMMIT)


def test_wrong_os_run_is_rejected_before_command_execution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = _ready(monkeypatch, tmp_path)
    monkeypatch.setattr(
        campaign,
        "_run",
        lambda *_args, **_kwargs: pytest.fail("wrong-OS command must not execute"),
    )

    with pytest.raises(campaign.CampaignError, match="windows"):
        campaign.run_command(
            checkout,
            root,
            commit=COMMIT,
            host="nitro",
            scenario="P06",
            assertion="unexpected-child-restart",
            label="must not execute",
            command=["python", "-c", "print('unsafe')"],
            expected_exit=0,
            timeout=1,
        )


def test_host_alias_cannot_be_reused_for_another_machine(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = _ready(monkeypatch, tmp_path)
    monkeypatch.setattr(campaign.platform, "node", lambda: "another-private-host")

    with pytest.raises(campaign.CampaignError, match="another machine"):
        campaign.register_host(
            checkout,
            root,
            commit=COMMIT,
            host="nitro",
            role="other",
        )
