from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.capabilities.benchmarking.adapters.ollama import (
    OLLAMA_BENCHMARK_ID,
    OllamaBenchmarkAdapter,
    OllamaProbeError,
    OllamaProbeTarget,
)
from catalog.capabilities.benchmarking.adapters.storage import (
    STORAGE_BENCHMARK_ID,
    StorageCandidateAdapter,
    StorageCandidateTarget,
)
from catalog.capabilities.contributions import (
    StorageCandidateSource,
    StorageCandidateSpec,
    StorageContributionAdapter,
)
from catalog.federation.errors import FederationOperationError
from catalog.federation.onboarding_models import BenchmarkState
from catalog.flask_app.app import create_app
from catalog.flask_app.services.capability_contribution_service import (
    CapabilityContributionService,
)
from catalog.flask_app.services.run_once_capability_evidence import (
    RunOnceCapabilityBenchmarkService,
    RunOnceCapabilityContributionService,
    RunOnceCapabilityInspectionService,
)
from catalog.flask_app.tests.test_capability_benchmark_route import (
    NOW,
    _app,
    _benchmark_service,
    _connect_and_inspect,
    _inspection_service,
    _onboarding_service,
)

STORAGE_ID = "local-storage-candidate"
UNAVAILABLE_KEY = (OLLAMA_BENCHMARK_ID, "unavailable-target")


def _harness(
    tmp_path: Path, *, benchmark_state: Path | None = None, installed: bool = False
):
    """Real adapters and local storage I/O; Ollama never touches the network."""
    current = [NOW]

    def clock():
        return current[0]

    probe = tmp_path / "bounded-storage-probe"
    probe_calls = []
    ollama_calls = []
    authority_calls = []

    def unavailable_ollama(method, url, _payload, _timeout, _limit):
        ollama_calls.append((method, url))
        raise OllamaProbeError("Configured local Ollama is unavailable")

    def write(_probe_id, payload, _context):
        probe_calls.append("write")
        probe.write_bytes(payload)

    def read(_probe_id, _context):
        probe_calls.append("read")
        return probe.read_bytes()

    def cleanup(_probe_id, _context):
        probe_calls.append("cleanup")
        probe.unlink()
        return not probe.exists()

    ollama = OllamaBenchmarkAdapter(
        (
            OllamaProbeTarget(
                service_id="ollama-configured",
                display_label="Configured local Ollama",
                base_url="http://127.0.0.1:11434",
                model="fixture-model",
            ),
        ),
        requester=unavailable_ollama,
    )
    storage = StorageCandidateAdapter(
        (
            StorageCandidateTarget(
                candidate_id=STORAGE_ID,
                display_label="Local storage candidate",
                candidate_fingerprint="local-storage-v1",
                write_probe=write,
                read_probe=read,
                cleanup_probe=cleanup,
            ),
        )
    )
    onboarding = _onboarding_service(tmp_path, clock=clock)
    sources = (
        StorageCandidateSource(
            {
                STORAGE_ID: StorageCandidateSpec(
                    provider_id=STORAGE_ID,
                    protocol="fcp-storage-v1",
                    display_label="Local storage candidate",
                    capacity_envelope={"capacity_band": "small"},
                )
            }
        ),
    )
    adapters = (
        StorageContributionAdapter(
            is_assigned=lambda provider_id: (
                authority_calls.append(("is_assigned", provider_id)) or False
            ),
            fence_candidate=lambda provider_id: authority_calls.append(
                ("fence", provider_id)
            ),
        ),
    )
    if installed:
        app = create_app()
        app.config.update(
            TESTING=True,
            CAPABILITY_ONBOARDING_SERVICE=onboarding,
            CAPABILITY_ONBOARDING_STATE_DATABASE=tmp_path / "onboarding.sqlite3",
            CAPABILITY_ONBOARDING_BENCHMARK_DATABASE=(benchmark_state or tmp_path)
            / "onboarding.sqlite3",
            CAPABILITY_ONBOARDING_CONTRIBUTION_DATABASE=tmp_path / "onboarding.sqlite3",
            CAPABILITY_ONBOARDING_INSPECTION_ADAPTERS=(ollama, storage),
            CAPABILITY_ONBOARDING_SYSTEM_OBSERVER=lambda: {
                "cpu": {"logical_cores_band": "8-15"},
                "memory": {"capacity_band": "16-31-gib"},
            },
            CAPABILITY_ONBOARDING_CONTRIBUTION_SOURCES=sources,
            CAPABILITY_ONBOARDING_CONTRIBUTION_ADAPTERS=adapters,
        )
        # Exercise the factory's normal lazy installer, never inject the base
        # benchmark service (which would conceal the installed-path regression).
        assert "CAPABILITY_BENCHMARK_SERVICE" not in app.config
        with app.app_context():
            inspection = app.config["CAPABILITY_INSPECTION_SERVICE"]
            benchmarks = app.config["CAPABILITY_BENCHMARK_SERVICE"]
            contributions = app.config["CAPABILITY_CONTRIBUTION_SERVICE"]
            assert isinstance(inspection, RunOnceCapabilityInspectionService)
            assert isinstance(benchmarks, RunOnceCapabilityBenchmarkService)
            assert isinstance(contributions, RunOnceCapabilityContributionService)
    else:
        inspection = _inspection_service(
            tmp_path, onboarding, adapters=(ollama, storage), clock=clock
        )
        benchmarks = _benchmark_service(
            benchmark_state or tmp_path, onboarding, inspection, clock=clock
        )
        contributions = CapabilityContributionService(
            onboarding_service=onboarding,
            inspection_service=inspection,
            benchmark_service=benchmarks,
            state_database=tmp_path / "onboarding.sqlite3",
            sources=sources,
            adapters=adapters,
            clock=clock,
        )
        app = _app(onboarding, inspection, benchmarks)
        app.config["CAPABILITY_CONTRIBUTION_SERVICE"] = contributions
    client = app.test_client()
    csrf, _command_id = _connect_and_inspect(client)
    assert (
        client.post(
            "/onboarding/benchmarks/run",
            data={
                "_csrf_token": csrf,
                "benchmark_id": STORAGE_BENCHMARK_ID,
                "target_service_id": STORAGE_ID,
            },
        ).status_code
        == 303
    )
    results = benchmarks.list_results()
    device_id = inspection.load().device_id
    storage_results = [result for result in results if result.device_id == device_id]
    assert len(storage_results) == 1
    assert storage_results[0].state is BenchmarkState.PASSED
    assert storage_results[0].metrics["round_trip_match"] is True
    assert storage_results[0].metrics["payload_bytes"] == 256
    assert probe_calls == ["write", "read", "cleanup"]
    assert not probe.exists()
    return SimpleNamespace(
        app=app,
        client=client,
        csrf=csrf,
        current=current,
        onboarding=onboarding,
        inspection=inspection,
        benchmarks=benchmarks,
        contributions=contributions,
        results=results,
        probe_calls=probe_calls,
        ollama_calls=ollama_calls,
        authority_calls=authority_calls,
    )


def _review(harness):
    return harness.benchmarks.view_model(harness.inspection.load(), connected=True)


def _skips(harness):
    snapshot = harness.inspection.load()
    return harness.benchmarks.skip_store.list_for_revision(
        device_id=snapshot.device_id, inspection_revision=snapshot.revision
    )


def _skip_through_route(harness):
    response = harness.client.post(
        "/onboarding/benchmarks/skip", data={"_csrf_token": harness.csrf}
    )
    assert response.status_code == 303
    assert _skips(harness) == frozenset({UNAVAILABLE_KEY})


@pytest.mark.parametrize("installed", [False, True], ids=["base", "installed-run-once"])
def test_unavailable_optional_check_requires_explicit_review_without_authority(
    tmp_path: Path,
    installed: bool,
) -> None:
    harness = _harness(tmp_path, installed=installed)
    summary, cards, complete = _review(harness)
    unavailable = next(
        card for card in cards if card["benchmark_id"] == OLLAMA_BENCHMARK_ID
    )
    assert not complete
    assert unavailable["state"] == "blocked"
    assert unavailable["can_run"] is False
    original_diagnostic = unavailable["diagnostic"]
    assert _skips(harness) == frozenset()
    with harness.app.app_context(), pytest.raises(FederationOperationError) as error:
        harness.contributions.recommend()
    assert error.value.code == "contribution-benchmarks-required"
    assert summary["can_skip"] is True

    _skip_through_route(harness)
    summary, cards, complete = _review(harness)
    unavailable = next(
        card for card in cards if card["benchmark_id"] == OLLAMA_BENCHMARK_ID
    )
    assert complete
    assert unavailable["state"] == "skipped"
    assert unavailable["can_run"] is False
    assert unavailable["diagnostic"] == original_diagnostic
    assert original_diagnostic
    assert unavailable["metrics"] == []
    assert summary["can_skip"] is False
    with harness.app.app_context():
        candidates = harness.contributions.recommend()
    assert len(candidates) == 1
    assert candidates[0].capability_type == "storage"
    assert candidates[0].capacity_envelope["authority"] == "candidate-only"
    assert harness.contributions.intents() == ()
    assert harness.authority_calls == []
    assert harness.benchmarks.list_results() == harness.results
    assert harness.probe_calls == ["write", "read", "cleanup"]
    assert all(method == "GET" for method, _url in harness.ollama_calls)
    with pytest.raises(FederationOperationError) as error:
        harness.benchmarks.run(
            benchmark_id=OLLAMA_BENCHMARK_ID, target_service_id="unavailable-target"
        )
    assert error.value.code == "benchmark-target-unavailable"
    assert harness.benchmarks.list_results() == harness.results
    assert harness.benchmarks.skip_all() == 0


@pytest.mark.parametrize("installed", [False, True], ids=["base", "installed-run-once"])
def test_unavailable_review_does_not_carry_to_new_inspection(
    tmp_path: Path,
    installed: bool,
) -> None:
    harness = _harness(tmp_path, installed=installed)
    _skip_through_route(harness)
    assert _review(harness)[2]
    previous_revision = harness.inspection.load().revision
    new_snapshot = harness.inspection.run()
    assert new_snapshot.revision == previous_revision + 1
    assert _skips(harness) == frozenset()
    _summary, cards, complete = _review(harness)
    assert not complete
    assert {card["benchmark_id"]: card["state"] for card in cards} == {
        OLLAMA_BENCHMARK_ID: "blocked",
        STORAGE_BENCHMARK_ID: "passed",
    }
    with harness.app.app_context(), pytest.raises(FederationOperationError) as error:
        harness.contributions.recommend()
    assert error.value.code == "contribution-benchmarks-required"
    assert harness.benchmarks.list_results() == harness.results


@pytest.mark.parametrize("installed", [False, True], ids=["base", "installed-run-once"])
def test_unavailable_review_does_not_carry_to_other_device(
    tmp_path: Path,
    installed: bool,
) -> None:
    first = _harness(tmp_path / "first", installed=installed)
    _skip_through_route(first)
    assert _review(first)[2]
    second = _harness(
        tmp_path / "second", benchmark_state=tmp_path / "first", installed=installed
    )
    assert first.inspection.load().revision == second.inspection.load().revision
    assert first.inspection.load().device_id != second.inspection.load().device_id
    assert _skips(second) == frozenset()
    _summary, cards, complete = _review(second)
    assert not complete
    assert {card["benchmark_id"]: card["state"] for card in cards} == {
        OLLAMA_BENCHMARK_ID: "blocked",
        STORAGE_BENCHMARK_ID: "passed",
    }
    with second.app.app_context(), pytest.raises(FederationOperationError) as error:
        second.contributions.recommend()
    assert error.value.code == "contribution-benchmarks-required"


def test_expired_inspection_cannot_skip_unavailable_check(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    harness.current[0] += timedelta(seconds=901)
    response = harness.client.post(
        "/onboarding/benchmarks/skip", data={"_csrf_token": harness.csrf}
    )
    assert response.status_code == 409
    assert _skips(harness) == frozenset()
    assert not _review(harness)[2]
    assert harness.benchmarks.list_results() == harness.results


@pytest.mark.parametrize("installed", [False, True], ids=["base", "installed-run-once"])
def test_revoked_membership_cannot_skip_unavailable_check(
    tmp_path: Path,
    installed: bool,
) -> None:
    harness = _harness(tmp_path, installed=installed)
    device_id = harness.inspection.load().device_id
    assert harness.onboarding.coordinator.revoke_node(
        node_id=device_id, reason="test revocation", request_id="revoke-review-test"
    )
    with pytest.raises(FederationOperationError):
        harness.benchmarks.skip_all()
    response = harness.client.post(
        "/onboarding/benchmarks/skip", data={"_csrf_token": harness.csrf}
    )
    # The existing route flashes domain authentication errors and redirects;
    # the rejected operation must not be mistaken for a successful skip.
    assert response.status_code == 303
    with harness.client.session_transaction() as browser:
        assert any(category == "error" for category, _message in browser["_flashes"])
    assert _skips(harness) == frozenset()
    assert harness.benchmarks.list_results() == harness.results
    assert harness.authority_calls == []


@pytest.mark.parametrize("csrf", [None, "invalid-csrf"])
@pytest.mark.parametrize("installed", [False, True], ids=["base", "installed-run-once"])
def test_skip_unavailable_check_keeps_csrf_boundary(
    tmp_path: Path,
    csrf,
    installed: bool,
) -> None:
    harness = _harness(tmp_path, installed=installed)
    response = harness.client.post(
        "/onboarding/benchmarks/skip",
        data={} if csrf is None else {"_csrf_token": csrf},
    )
    assert response.status_code == 403
    assert _skips(harness) == frozenset()
    assert not _review(harness)[2]
    assert harness.benchmarks.list_results() == harness.results
    assert harness.authority_calls == []


def test_installed_skip_keeps_run_once_evidence_across_age_only_expiry(
    tmp_path: Path,
) -> None:
    harness = _harness(tmp_path, installed=True)
    _skip_through_route(harness)
    assert _review(harness)[2]
    revision = harness.inspection.load().revision
    harness.current[0] += timedelta(days=2)

    summary, cards, complete = _review(harness)
    assert complete
    assert summary["can_skip"] is False
    assert {card["benchmark_id"]: card["state"] for card in cards} == {
        OLLAMA_BENCHMARK_ID: "skipped",
        STORAGE_BENCHMARK_ID: "passed",
    }
    storage_card = next(
        card for card in cards if card["benchmark_id"] == STORAGE_BENCHMARK_ID
    )
    assert storage_card["expires_label"].startswith("Saved evidence")
    assert harness.inspection.load().revision == revision
    assert _skips(harness) == frozenset({UNAVAILABLE_KEY})
    assert harness.benchmarks.list_results() == harness.results
    assert harness.benchmarks.accepted_results(harness.inspection.load()) == tuple(
        harness.results
    )
    with harness.app.app_context():
        candidates = harness.contributions.recommend()
    assert len(candidates) == 1
    assert candidates[0].capacity_envelope["authority"] == "candidate-only"
    assert harness.contributions.intents() == ()
    assert harness.authority_calls == []
    assert harness.probe_calls == ["write", "read", "cleanup"]
