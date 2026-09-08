"""Adversarial tests for explicit physical-runtime binding.

These tests model the deployment topologies that the checked-in candidate
worktree cannot discover from its own cwd.  A binding is accepted only when it
names the operator-registered surface and pins both candidate and harness
identities; no arbitrary container or data-root scan is permitted.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from scripts.acceptance import v1_physical_campaign as campaign
from scripts.acceptance import v1_physical_probes as probes
from scripts.acceptance import v1_physical_runner as runner
from scripts.acceptance import v1_physical_runtime_binding as binding
from scripts.acceptance.v1_physical_automation import ProbeBinding

from .test_v1_physical_runner import COMMIT, ready

HARNESS = "b" * 40


def compose_document(tmp_path: Path, *, project: str = "fcp-new") -> dict[str, object]:
    config = tmp_path / "deployment" / "docker-compose.yml"
    config.parent.mkdir()
    config.write_text("services: {}\n", encoding="utf-8")
    return {
        "schema": binding.BINDING_SCHEMA,
        "host_id": "nitro",
        "target_candidate_sha": COMMIT,
        "acceptance_harness_sha": HARNESS,
        "harness_checkout": str(tmp_path),
        "runtime_kind": "compose",
        "runtime": {
            "project": project,
            "working_directory": str(config.parent),
            "config_files": [str(config)],
        },
    }


def test_binding_requires_explicit_project_and_config(tmp_path: Path) -> None:
    document = compose_document(tmp_path)
    del document["runtime"]["config_files"]  # type: ignore[index]
    with pytest.raises(binding.RuntimeBindingError):
        binding.RuntimeBinding.from_mapping(document)


def test_alternate_compose_project_is_used_instead_of_checkout_cwd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = binding.RuntimeBinding.from_mapping(compose_document(tmp_path, project="external-fcp"))
    calls: list[tuple[list[str], Path]] = []

    def fake_run(command, *, cwd, timeout=60.0):
        calls.append((list(command), cwd))
        return 0, json.dumps([{"Name": "fcp-new-flask-1", "Service": "flask", "State": "running", "ID": "c1"}])

    monkeypatch.setattr(probes, "_run", fake_run)
    containers = probes._compose_containers(
        probes.ProbeContext(
            checkout=tmp_path / "harness-checkout",
            evidence_root=tmp_path / "evidence",
            commit=COMMIT,
            host_id="nitro",
            os_category="posix",
            profile="school-control",
            scenario="P01",
            assertion="posix-runtime-state",
            runtime_binding=runtime,
        )
    )
    assert containers
    command, cwd = calls[0]
    assert "--project-name" in command
    assert command[command.index("--project-name") + 1] == "external-fcp"
    assert cwd == runtime.compose_working_directory
    assert str(tmp_path / "harness-checkout") not in command


def test_running_identity_rejects_wrong_sha_and_missing_labels(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = binding.RuntimeBinding.from_mapping(compose_document(tmp_path))
    monkeypatch.setattr(probes, "_docker_available", lambda: True)
    monkeypatch.setattr(
        probes,
        "_compose_containers",
        lambda _context: [
            {"Name": "flask", "Service": "flask", "State": "running", "ID": "wrong"},
            {"Name": "relay", "Service": "relay", "State": "running", "ID": "missing"},
            {"Name": "recorder", "Service": "recorder", "State": "running", "ID": "recorder"},
        ],
    )
    labels = {"wrong": "c" * 40, "missing": "", "recorder": COMMIT}
    monkeypatch.setattr(
        probes,
        "_container_label",
        lambda _context, container, label: (
            str(runtime.compose_working_directory)
            if label == "com.docker.compose.project.working_dir"
            else labels[container]
        ),
    )
    outcome = probes._probe_running_commit_identity(
        probes.ProbeContext(
            checkout=tmp_path,
            evidence_root=tmp_path,
            commit=COMMIT,
            host_id="nitro",
            os_category="posix",
            profile="school-control",
            scenario="P01",
            assertion="posix-runtime-state",
            runtime_binding=runtime,
        )
    )
    assert outcome.status == probes.FAIL
    matches = {
        item["service"]: item["build_commit_matches_candidate"]
        for item in outcome.detail["containers"]
    }
    assert matches == {"flask": False, "relay": False, "recorder": True}


def _compose_identity_context(tmp_path: Path) -> probes.ProbeContext:
    runtime = binding.RuntimeBinding.from_mapping(compose_document(tmp_path))
    return probes.ProbeContext(
        checkout=tmp_path,
        evidence_root=tmp_path,
        commit=COMMIT,
        host_id="nitro",
        os_category="posix",
        profile="school-control",
        scenario="P01",
        assertion="posix-runtime-state",
        runtime_binding=runtime,
    )


def _patch_identity_runtime(
    monkeypatch: pytest.MonkeyPatch,
    containers: list[dict[str, object]],
    labels: dict[str, str],
) -> None:
    monkeypatch.setattr(probes, "_docker_available", lambda: True)
    monkeypatch.setattr(probes, "_compose_containers", lambda _context: containers)
    monkeypatch.setattr(
        probes,
        "_container_label",
        lambda context, container, label: (
            str(context.runtime_binding.compose_working_directory)
            if label == "com.docker.compose.project.working_dir"
            else labels[container]
        ),
    )


def _candidate_containers() -> list[dict[str, object]]:
    return [
        {"Service": service, "State": "running", "ID": service}
        for service in ("flask", "recorder", "relay")
    ]


def test_running_identity_accepts_candidate_services_plus_pinned_external_ollama(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    containers = _candidate_containers()
    containers.append(
        {
            "Service": "ollama",
            "State": "running",
            "ID": "ollama",
            "Image": "ollama/ollama:0.32.6@sha256:" + "a" * 64,
        }
    )
    _patch_identity_runtime(monkeypatch, containers, {service: COMMIT for service in ("flask", "recorder", "relay")})

    outcome = probes._probe_running_commit_identity(_compose_identity_context(tmp_path))

    assert outcome.status == probes.PASS
    assert any(
        item.get("classification") == "pinned-external-image"
        for item in outcome.detail["containers"]
    )


def test_running_identity_rejects_unlabeled_candidate_service_even_with_external_image(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    containers = _candidate_containers()
    containers.append(
        {
            "Service": "ollama",
            "State": "running",
            "ID": "ollama",
            "Image": "ollama/ollama:0.32.6@sha256:" + "a" * 64,
        }
    )
    _patch_identity_runtime(
        monkeypatch,
        containers,
        {"flask": "", "recorder": COMMIT, "relay": COMMIT},
    )

    outcome = probes._probe_running_commit_identity(_compose_identity_context(tmp_path))

    assert outcome.status == probes.FAIL


def test_running_identity_rejects_missing_candidate_service_even_with_external_image(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    containers = [
        {"Service": service, "State": "running", "ID": service}
        for service in ("recorder", "relay")
    ]
    containers.append(
        {
            "Service": "ollama",
            "State": "running",
            "ID": "ollama",
            "Image": "ollama/ollama:0.32.6@sha256:" + "a" * 64,
        }
    )
    _patch_identity_runtime(
        monkeypatch,
        containers,
        {"recorder": COMMIT, "relay": COMMIT},
    )

    outcome = probes._probe_running_commit_identity(_compose_identity_context(tmp_path))

    assert outcome.status == probes.FAIL
    assert outcome.detail["missing_candidate_services"] == ["flask"]


@pytest.mark.parametrize("service", ["relay", "recorder"])
def test_running_identity_rejects_wrong_candidate_service_sha(
    service: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    labels = {name: COMMIT for name in ("flask", "recorder", "relay")}
    labels[service] = "d" * 40
    _patch_identity_runtime(monkeypatch, _candidate_containers(), labels)

    outcome = probes._probe_running_commit_identity(_compose_identity_context(tmp_path))

    assert outcome.status == probes.FAIL


def test_running_identity_fails_closed_for_unknown_unpinned_service(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    containers = _candidate_containers()
    containers.append({"Service": "mystery", "State": "running", "ID": "mystery"})
    _patch_identity_runtime(
        monkeypatch,
        containers,
        {service: COMMIT for service in ("flask", "recorder", "relay")},
    )

    outcome = probes._probe_running_commit_identity(_compose_identity_context(tmp_path))

    assert outcome.status == probes.FAIL
    assert any(
        item.get("classification") == "unknown-unclassified"
        for item in outcome.detail["containers"]
    )


def test_native_binding_uses_external_status_and_data_root_without_leaking_paths(
    tmp_path: Path,
) -> None:
    status = tmp_path / "runtime" / "status.json"
    data = tmp_path / "runtime" / "data"
    document = {
        "schema": binding.BINDING_SCHEMA,
        "host_id": "nitro",
        "target_candidate_sha": COMMIT,
        "acceptance_harness_sha": HARNESS,
        "harness_checkout": str(tmp_path),
        "runtime_kind": "native-recorder",
        "runtime": {"recorder_status_file": str(status), "data_root": str(data)},
    }
    runtime = binding.RuntimeBinding.from_mapping(document)
    context = probes.ProbeContext(
        checkout=tmp_path / "harness",
        evidence_root=tmp_path / "evidence",
        commit=COMMIT,
        host_id="nitro",
        os_category="posix",
        profile="cnc-recorder",
        scenario="P04",
        assertion="native-recorder-state",
        runtime_binding=runtime,
    )
    assert context.data_dir == data
    assert context.runtime_binding.recorder_status_file == status
    summary = binding.public_summary(runtime)
    serialized = json.dumps(summary)
    assert str(status) not in serialized
    assert str(data) not in serialized
    assert summary["target_candidate_sha"] == COMMIT
    assert summary["acceptance_harness_sha"] == HARNESS


def test_external_harness_gate_does_not_require_harness_checkout_to_be_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    runtime = binding.RuntimeBinding(
        host_id="nitro",
        target_candidate_sha=COMMIT,
        acceptance_harness_sha=HARNESS,
        harness_checkout=tmp_path / "tooling-checkout",
        kind="compose",
        compose_project="external-fcp",
        compose_working_directory=tmp_path,
        compose_config_files=(tmp_path / "docker-compose.yml",),
    )
    monkeypatch.setattr(binding, "load", lambda *_args, **_kwargs: runtime)
    monkeypatch.setattr(
        campaign,
        "verify_checkout",
        lambda *_args, **_kwargs: pytest.fail("external harness must not be checked as candidate"),
    )
    expected, record = runner._gate(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        runtime_binding_file=tmp_path / "binding.json",
    )
    assert expected == COMMIT
    assert record["__runtime_binding"] is runtime
    assert campaign._load_json(root / "campaign.json")["harness_sha"] == HARNESS


def test_external_harness_report_does_not_require_candidate_checkout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    runtime = binding.RuntimeBinding(
        host_id="nitro",
        target_candidate_sha=COMMIT,
        acceptance_harness_sha=HARNESS,
        harness_checkout=tmp_path / "tooling-checkout",
        kind="compose",
        compose_project="external-fcp",
        compose_working_directory=tmp_path,
        compose_config_files=(tmp_path / "docker-compose.yml",),
    )
    monkeypatch.setattr(binding, "load", lambda *_args, **_kwargs: runtime)
    monkeypatch.setattr(
        campaign,
        "verify_checkout",
        lambda *_args, **_kwargs: pytest.fail(
            "external harness must not be checked as candidate"
        ),
    )
    result = runner.report(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        runtime_binding_file=tmp_path / "binding.json",
    )
    assert result["candidate_sha"] == COMMIT
    assert campaign._load_json(root / "campaign.json")["harness_sha"] == HARNESS


def test_external_harness_skips_local_checkout_probe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = binding.RuntimeBinding(
        host_id="nitro",
        target_candidate_sha=COMMIT,
        acceptance_harness_sha=HARNESS,
        harness_checkout=tmp_path,
        kind="compose",
        compose_project="external-fcp",
        compose_working_directory=tmp_path,
        compose_config_files=(tmp_path / "docker-compose.yml",),
    )
    calls: list[str] = []

    def execute(spec: probes.ProbeSpec, _context: probes.ProbeContext) -> probes.ProbeOutcome:
        calls.append(spec.probe_id)
        return probes.ProbeOutcome(spec.probe_id, probes.PASS, "stubbed", {})

    monkeypatch.setattr(probes, "execute", execute)
    runner.run_bindings(
        tmp_path,
        tmp_path / "evidence",
        commit=COMMIT,
        record={
            "__runtime_binding": runtime,
            "host_id": "nitro",
            "os_category": "posix",
            "profile": "school-control",
        },
        scenario="P01",
        assertion="windows-runtime-state",
        run_id=None,
        bindings=(ProbeBinding("checkout-identity"), ProbeBinding("service-health")),
        overrides={},
    )
    assert calls == ["service-health"]


def test_compose_binding_excludes_container_from_another_working_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = binding.RuntimeBinding(
        host_id="nitro",
        target_candidate_sha=COMMIT,
        acceptance_harness_sha=HARNESS,
        harness_checkout=tmp_path,
        kind="compose",
        compose_project="external-fcp",
        compose_working_directory=tmp_path,
        compose_config_files=(tmp_path / "docker-compose.yml",),
    )
    context = probes.ProbeContext(
        checkout=tmp_path,
        evidence_root=tmp_path / "evidence",
        commit=COMMIT,
        host_id="nitro",
        os_category="posix",
        profile="school-control",
        scenario="P01",
        assertion="posix-runtime-state",
        runtime_binding=runtime,
    )
    monkeypatch.setattr(
        probes,
        "_container_label",
        lambda *_args: str(tmp_path / "other-deployment"),
    )
    assert probes._container_matches_runtime_binding(context, "container-id") is False
    monkeypatch.setattr(probes, "_container_label", lambda *_args: str(tmp_path))
    assert probes._container_matches_runtime_binding(context, "container-id") is True


# ---------------------------------------------------------------------------
# P1 review findings: each of the four is pinned by an adversarial case that
# fails on the pre-correction code.
# ---------------------------------------------------------------------------


def _init_repo(path: Path) -> str:
    """Create a real one-commit git repository and return its HEAD SHA."""

    path.mkdir(parents=True, exist_ok=True)

    def run(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", *args], cwd=path, check=True, capture_output=True, text=True
        )

    run("init", "--quiet")
    run("config", "user.email", "cf7@example.invalid")
    run("config", "user.name", "CF7")
    (path / "harness.txt").write_text("harness\n", encoding="utf-8")
    run("add", "-A")
    run("commit", "--quiet", "-m", "harness")
    return run("rev-parse", "HEAD").stdout.strip()


def _binding_file(tmp_path: Path, document: dict[str, object]) -> Path:
    target = tmp_path / "binding.json"
    target.write_text(json.dumps(document), encoding="utf-8")
    return target


def _native_document(
    harness: Path, harness_sha: str, status: Path, data: Path
) -> dict[str, object]:
    return {
        "schema": binding.BINDING_SCHEMA,
        "host_id": "nitro",
        "target_candidate_sha": COMMIT,
        "acceptance_harness_sha": harness_sha,
        "harness_checkout": str(harness),
        "runtime_kind": "native-recorder",
        "runtime": {"recorder_status_file": str(status), "data_root": str(data)},
    }


# --- P1 finding 1: a dirty external harness must be refused ---------------


def test_a_clean_external_harness_at_the_declared_sha_is_accepted(
    tmp_path: Path,
) -> None:
    harness = tmp_path / "harness"
    harness_sha = _init_repo(harness)
    status = tmp_path / "runtime" / "status.json"
    data = tmp_path / "runtime" / "data"
    path = _binding_file(
        tmp_path, _native_document(harness, harness_sha, status, data)
    )
    loaded = binding.load(path, host_id="nitro", target_candidate_sha=COMMIT)
    assert loaded.acceptance_harness_sha == harness_sha


def test_an_external_harness_with_uncommitted_changes_is_refused(
    tmp_path: Path,
) -> None:
    harness = tmp_path / "harness"
    harness_sha = _init_repo(harness)
    # HEAD still reports the reviewed SHA, so identity alone still passes.
    # The probe code that would actually execute is no longer that commit.
    (harness / "harness.txt").write_text("tampered\n", encoding="utf-8")
    status = tmp_path / "runtime" / "status.json"
    data = tmp_path / "runtime" / "data"
    path = _binding_file(
        tmp_path, _native_document(harness, harness_sha, status, data)
    )
    with pytest.raises(binding.RuntimeBindingError, match="uncommitted"):
        binding.load(path, host_id="nitro", target_candidate_sha=COMMIT)


def test_an_untracked_file_in_the_external_harness_is_refused(
    tmp_path: Path,
) -> None:
    harness = tmp_path / "harness"
    harness_sha = _init_repo(harness)
    (harness / "extra_probe.py").write_text("# smuggled\n", encoding="utf-8")
    status = tmp_path / "runtime" / "status.json"
    data = tmp_path / "runtime" / "data"
    path = _binding_file(
        tmp_path, _native_document(harness, harness_sha, status, data)
    )
    with pytest.raises(binding.RuntimeBindingError, match="uncommitted"):
        binding.load(path, host_id="nitro", target_candidate_sha=COMMIT)


def test_a_harness_checkout_that_is_not_a_repository_is_refused(
    tmp_path: Path,
) -> None:
    harness = tmp_path / "not-a-repo"
    harness.mkdir()
    status = tmp_path / "runtime" / "status.json"
    data = tmp_path / "runtime" / "data"
    path = _binding_file(tmp_path, _native_document(harness, "c" * 40, status, data))
    with pytest.raises(binding.RuntimeBindingError):
        binding.load(path, host_id="nitro", target_candidate_sha=COMMIT)


# --- P1 finding 2: samples must measure the bound runtime roots -----------


def test_a_bound_sample_measures_the_runtime_roots_and_not_the_harness_checkout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    data = tmp_path / "runtime" / "data"
    results = tmp_path / "runtime" / "results"
    data.mkdir(parents=True)
    results.mkdir(parents=True)
    # A checkout-relative surface exists too. It must be ignored entirely,
    # because folding harness storage into the series would let harness churn
    # register as product growth.
    (checkout / "data").mkdir(exist_ok=True)
    (checkout / "results").mkdir(exist_ok=True)

    seen: list[Path] = []
    real_snapshot = campaign._disk_snapshot

    def recording_snapshot(path: Path) -> dict[str, object]:
        seen.append(path)
        return real_snapshot(path)

    monkeypatch.setattr(campaign, "_disk_snapshot", recording_snapshot)
    written = campaign.sample_resources(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P04",
        label="bound-sample",
        allow_external_harness=True,
        data_root=data,
        results_root=results,
    )
    packet = json.loads(written.read_text(encoding="utf-8"))
    assert packet["resource_roots_bound"] is True
    assert sorted(packet["resources"]) == ["data", "results"]
    assert "checkout" not in packet["resources"]
    assert set(seen) == {data, results}
    assert checkout / "data" not in seen
    assert checkout not in seen


def test_an_unbound_sample_keeps_the_checkout_relative_surface(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    (checkout / "data").mkdir(exist_ok=True)
    written = campaign.sample_resources(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P04",
        label="local-sample",
    )
    packet = json.loads(written.read_text(encoding="utf-8"))
    assert packet["resource_roots_bound"] is False
    assert "checkout" in packet["resources"]


def test_a_bound_root_that_does_not_exist_fails_closed_instead_of_measuring_nothing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    with pytest.raises(campaign.CampaignError, match="bound data_root"):
        campaign.sample_resources(
            checkout,
            root,
            commit=COMMIT,
            host="nitro",
            scenario="P04",
            label="missing-root",
            allow_external_harness=True,
            data_root=tmp_path / "runtime" / "absent",
        )


def test_a_bound_sample_publishes_no_absolute_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    data = tmp_path / "private" / "runtime-data"
    data.mkdir(parents=True)
    written = campaign.sample_resources(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P04",
        label="privacy-sample",
        allow_external_harness=True,
        data_root=data,
    )
    serialized = written.read_text(encoding="utf-8")
    assert str(data) not in serialized
    assert "private" not in serialized


def test_the_baseline_drops_the_harness_checkout_anchor_when_roots_are_bound(
    tmp_path: Path,
) -> None:
    harness = tmp_path / "harness"
    harness.mkdir()
    data = tmp_path / "runtime" / "data"
    data.mkdir(parents=True)
    runtime = binding.RuntimeBinding.from_mapping(
        _native_document(harness, HARNESS, tmp_path / "s.json", data)
    )
    bound = probes.ProbeContext(
        checkout=harness,
        evidence_root=tmp_path / "evidence",
        commit=COMMIT,
        host_id="nitro",
        os_category="posix",
        profile="cnc-recorder",
        scenario="P12",
        assertion="host-resource-baseline",
        runtime_binding=runtime,
    )
    assert bound.roots_are_bound is True
    assert "checkout" not in probes._baseline_anchors(bound)
    unbound = probes.ProbeContext(
        checkout=harness,
        evidence_root=tmp_path / "evidence",
        commit=COMMIT,
        host_id="nitro",
        os_category="posix",
        profile="cnc-recorder",
        scenario="P12",
        assertion="host-resource-baseline",
    )
    assert unbound.roots_are_bound is False
    assert "checkout" in probes._baseline_anchors(unbound)


def test_a_missing_bound_data_root_fails_the_storage_anchor_rather_than_falling_back(
    tmp_path: Path,
) -> None:
    harness = tmp_path / "harness"
    harness.mkdir()
    runtime = binding.RuntimeBinding.from_mapping(
        _native_document(
            harness, HARNESS, tmp_path / "s.json", tmp_path / "runtime" / "absent"
        )
    )
    context = probes.ProbeContext(
        checkout=harness,
        evidence_root=tmp_path / "evidence",
        commit=COMMIT,
        host_id="nitro",
        os_category="posix",
        profile="cnc-recorder",
        scenario="P12",
        assertion="inode-capacity",
        runtime_binding=runtime,
    )
    assert context.storage_anchor() is None
    assert probes._probe_inode_capacity(context).status == probes.FAIL


# --- P1 finding 3: native-recorder identity without Docker ---------------


def _native_context(
    tmp_path: Path, status: Path, *, assertion: str = "running-commit-identity"
) -> probes.ProbeContext:
    harness = tmp_path / "harness"
    harness.mkdir(exist_ok=True)
    data = tmp_path / "runtime" / "data"
    data.mkdir(parents=True, exist_ok=True)
    runtime = binding.RuntimeBinding.from_mapping(
        _native_document(harness, HARNESS, status, data)
    )
    return probes.ProbeContext(
        checkout=harness,
        evidence_root=tmp_path / "evidence",
        commit=COMMIT,
        host_id="nitro",
        os_category="posix",
        profile="cnc-recorder",
        scenario="P04",
        assertion=assertion,
        runtime_binding=runtime,
    )


def _write_status(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_a_native_recorder_proves_identity_with_no_docker_present(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    status = tmp_path / "runtime" / "status.json"
    _write_status(status, {"native_runtime": {"build_commit": COMMIT}})

    def refuse() -> bool:
        raise AssertionError("native-recorder identity must not consult Docker")

    monkeypatch.setattr(probes, "_docker_available", refuse)
    monkeypatch.setattr(
        probes,
        "_compose_containers",
        lambda *_a, **_k: (_ for _ in ()).throw(
            AssertionError("native-recorder identity must not list containers")
        ),
    )
    outcome = probes._probe_running_commit_identity(_native_context(tmp_path, status))
    assert outcome.status == probes.PASS
    assert outcome.detail["runtime_kind"] == "native-recorder"
    assert outcome.detail["build_commit_matches_candidate"] is True


def test_a_native_recorder_without_a_build_identity_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    status = tmp_path / "runtime" / "status.json"
    _write_status(status, {"native_runtime": {"state": "running"}})
    monkeypatch.setattr(probes, "_docker_available", lambda: False)
    outcome = probes._probe_running_commit_identity(_native_context(tmp_path, status))
    assert outcome.status == probes.FAIL
    assert outcome.detail["build_commit_present"] is False


def test_a_native_recorder_on_another_commit_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    status = tmp_path / "runtime" / "status.json"
    _write_status(status, {"native_runtime": {"build_commit": "d" * 40}})
    monkeypatch.setattr(probes, "_docker_available", lambda: False)
    outcome = probes._probe_running_commit_identity(_native_context(tmp_path, status))
    assert outcome.status == probes.FAIL
    assert outcome.detail["build_commit_matches_candidate"] is False


def test_a_native_recorder_with_no_status_surface_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    status = tmp_path / "runtime" / "status.json"
    monkeypatch.setattr(probes, "_docker_available", lambda: False)
    outcome = probes._probe_running_commit_identity(_native_context(tmp_path, status))
    assert outcome.status == probes.FAIL


def test_a_native_recorder_without_a_native_runtime_block_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    status = tmp_path / "runtime" / "status.json"
    _write_status(status, {"state": "running"})
    monkeypatch.setattr(probes, "_docker_available", lambda: False)
    outcome = probes._probe_running_commit_identity(_native_context(tmp_path, status))
    assert outcome.status == probes.FAIL


def test_native_identity_evidence_names_no_private_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    status = tmp_path / "private" / "status.json"
    _write_status(status, {"native_runtime": {"build_commit": COMMIT}})
    monkeypatch.setattr(probes, "_docker_available", lambda: False)
    outcome = probes._probe_running_commit_identity(_native_context(tmp_path, status))
    serialized = json.dumps(outcome.detail)
    assert str(status) not in serialized
    assert "private" not in serialized


# --- P1 finding 4: recorder status must follow the bound surface ----------


def test_recorder_status_reads_the_bound_file_not_the_harness_checkout(
    tmp_path: Path,
) -> None:
    bound_status = tmp_path / "runtime" / "status.json"
    context = _native_context(tmp_path, bound_status, assertion="service-health")
    # A decoy at the checkout-relative location the harness used to read.
    decoy = context.data_dir / "source_state" / "mtconnect_recorder_status.json"
    _write_status(decoy, {"state": "decoy"})
    assert probes._recorder_status_path(context) == bound_status
    assert probes._recorder_status_path(context) != decoy


def test_recorder_status_falls_back_to_the_data_root_only_without_a_bound_file(
    tmp_path: Path,
) -> None:
    context = probes.ProbeContext(
        checkout=tmp_path / "harness",
        evidence_root=tmp_path / "evidence",
        commit=COMMIT,
        host_id="nitro",
        os_category="posix",
        profile="cnc-recorder",
        scenario="P04",
        assertion="service-health",
    )
    assert probes._recorder_status_path(context) == (
        context.data_dir / "source_state" / "mtconnect_recorder_status.json"
    )


def test_health_continuity_and_sample_paths_all_use_the_bound_status_surface(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    bound_status = tmp_path / "runtime" / "status.json"
    _write_status(
        bound_status,
        {
            "present": True,
            "state": "running",
            "build_commit": COMMIT,
            "native_runtime": {"build_commit": COMMIT},
        },
    )
    context = _native_context(tmp_path, bound_status, assertion="service-health")
    decoy = context.data_dir / "source_state" / "mtconnect_recorder_status.json"
    _write_status(decoy, {"present": False, "state": "decoy"})

    read: list[Path] = []
    original = probes._recorder_status_path

    def recording(ctx: probes.ProbeContext) -> Path:
        resolved = original(ctx)
        read.append(resolved)
        return resolved

    monkeypatch.setattr(probes, "_recorder_status_path", recording)
    probes._recorder_status(context)
    probes._probe_running_commit_identity(context)
    assert read, "no recorder status path was resolved"
    assert set(read) == {bound_status}
    assert decoy not in read
