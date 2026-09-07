"""Adversarial tests for explicit physical-runtime binding.

These tests model the deployment topologies that the checked-in candidate
worktree cannot discover from its own cwd.  A binding is accepted only when it
names the operator-registered surface and pins both candidate and harness
identities; no arbitrary container or data-root scan is permitted.
"""

from __future__ import annotations

import json
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
        return 0, json.dumps([{"Name": "fcp-new-web-1", "Service": "web", "State": "running", "ID": "c1"}])

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
            {"Name": "web", "Service": "web", "State": "running", "ID": "wrong"},
            {"Name": "relay", "Service": "relay", "State": "running", "ID": "missing"},
        ],
    )
    labels = {"wrong": "c" * 40, "missing": ""}
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
    assert all(not item["build_commit_matches_candidate"] for item in outcome.detail["containers"])


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
