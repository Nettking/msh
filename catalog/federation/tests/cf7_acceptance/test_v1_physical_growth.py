"""Backing-volume growth must not depend on the number of bound directories."""

from __future__ import annotations

import copy
import json
from dataclasses import replace
from pathlib import Path

import pytest

from scripts.acceptance import v1_physical_campaign as campaign
from scripts.acceptance import v1_physical_probes as probes


def resource(used: int, alias: str = "a" * 64) -> dict[str, object]:
    return {"resource_alias": alias, "used_bytes": used, "total_bytes": 10**13}


def samples(delta: int, *, separate: bool = False) -> list[dict[str, object]]:
    return [
        {
            "recorded_at": f"2026-09-13T{hour:02}:00:00Z",
            "resources": {
                "data": resource(10**9 + index * delta // 3),
                "results": resource(
                    10**9 + index * delta // 3, ("b" if separate else "a") * 64
                ),
            },
        }
        for index, hour in enumerate(range(10, 14))
    ]


def run_probe(tmp_path, monkeypatch, packets, probe_id="growth-analysis"):
    monkeypatch.setattr(probes, "_sample_packets", lambda _: packets)
    monkeypatch.setattr(probes, "_restart_states", lambda _: {})
    context = probes.ProbeContext(
        checkout=tmp_path, evidence_root=tmp_path / "evidence", commit="a" * 40,
        host_id="nettking", os_category="windows", profile="local-ai",
        scenario="P01", assertion="windows-growth-bounded",
        options={"activations": "3"},
    )
    return probes.PROBES[probe_id].run(context)


@pytest.mark.parametrize("probe_id,ceiling", [
    ("growth-analysis", probes.GIBIBYTE),
    ("activation-growth", 2 * probes.GIBIBYTE),
])
def test_bound_directories_share_one_growth_envelope(tmp_path, monkeypatch, probe_id, ceiling):
    delta = 3 * ceiling * 3 // 4
    outcome = run_probe(tmp_path, monkeypatch, samples(delta), probe_id)
    assert outcome.status == probes.PASS
    assert outcome.detail["used_bytes_delta"] == delta
    distinct = run_probe(tmp_path, monkeypatch, samples(delta, separate=True), probe_id)
    assert distinct.status == probes.FAIL
    assert distinct.detail["used_bytes_delta"] == 2 * delta


def test_windows_p01_observed_interval_is_not_counted_twice(tmp_path, monkeypatch):
    packets = [
        {"recorded_at": timestamp, "resources": {
            name: resource(used) for name in ("data", "results")
        }}
        for timestamp, used in [
            ("2026-09-13T12:08:34.302316Z", 1_947_508_125_696),
            ("2026-09-13T12:12:09.920326Z", 1_947_549_683_712),
        ]
    ]
    outcome = run_probe(tmp_path, monkeypatch, packets)
    assert outcome.status == probes.PASS
    assert outcome.detail["used_bytes_delta"] == 41_558_016
    assert outcome.detail["growth_bytes_per_hour"] == 693_860_673
    assert outcome.detail["max_growth_bytes_per_hour"] == probes.GIBIBYTE


@pytest.mark.parametrize("probe_id", ["growth-analysis", "activation-growth"])
@pytest.mark.parametrize("invalid", [
    "missing_identity", "empty_resources", "invalid_used", "over_capacity",
    "contradictory_alias", "changed_volume", "changed_capacity", "missing_root",
    "intermediate_bad_sample",
])
def test_ambiguous_or_changed_measurements_cannot_pass(tmp_path, monkeypatch, probe_id, invalid):
    packets = copy.deepcopy(samples(0))
    roots = packets[-1]["resources"]
    if invalid == "missing_identity":
        roots["data"].pop("resource_alias")
    elif invalid == "empty_resources":
        roots.clear()
    elif invalid == "invalid_used":
        roots["data"]["used_bytes"] = True
    elif invalid == "over_capacity":
        roots["data"]["used_bytes"] = 10**14
    elif invalid == "contradictory_alias":
        roots["data"]["used_bytes"] += 1
    elif invalid == "changed_volume":
        roots["data"]["resource_alias"] = "c" * 64
    elif invalid == "changed_capacity":
        roots["data"]["total_bytes"] += 1
    elif invalid == "missing_root":
        roots.pop("results")
    else:
        packets[1]["resources"]["data"].pop("resource_alias")
    outcome = run_probe(tmp_path, monkeypatch, packets, probe_id)
    assert outcome.status == probes.UNAVAILABLE


def test_native_snapshot_exposes_an_opaque_shared_volume_identity(tmp_path: Path):
    first, second = tmp_path / "data", tmp_path / "results"
    first.mkdir()
    second.mkdir()
    one, two = campaign._disk_snapshot(first), campaign._disk_snapshot(second)
    assert one["resource_alias"] == two["resource_alias"]
    assert len(one["resource_alias"]) == 64
    assert str(tmp_path) not in json.dumps([one, two])


def test_unavailable_volume_identity_cannot_produce_a_sample(tmp_path, monkeypatch):
    from catalog.federation import host_resources

    measurement = host_resources.measure_filesystem(tmp_path)
    monkeypatch.setattr(host_resources, "measure_filesystem", lambda _: replace(
        measurement, available=False, resource_id="unavailable"
    ))
    with pytest.raises(campaign.CampaignError, match="identity is unavailable"):
        campaign._disk_snapshot(tmp_path)
