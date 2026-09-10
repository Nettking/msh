"""Publication-boundary tests; all passing records below are synthetic fixtures."""

from __future__ import annotations

import hashlib
import json
import subprocess
import zipfile
from pathlib import Path

import pytest

from demo.icse import bundle
from demo.icse.network.provenance import verify_source

SOURCE_SHA = "a" * 40
REQUIRED_NETWORK_CHECKS = (
    "independent_processes",
    "authenticated_quorum_bootstrap",
    "authenticated_enrollment_and_join",
    "discovery_and_owner_authorization",
    "authenticated_payload_delivery",
    "automatic_quorum_failover_and_continuity",
    "successor_reconnect_and_delivery",
    "returning_leader_fenced",
    "minority_mutation_refused",
    "source_identity_unchanged",
)


def _write(path: Path, value: object) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")


@pytest.fixture
def evidence(tmp_path: Path) -> tuple[Path, Path]:
    components = tmp_path / "component-input"
    components.mkdir()
    for label in ("Linux", "Windows", "compose"):
        directory = components / f"icse-summary-{label}"
        directory.mkdir()
        _write(directory / "icse-summary.json", {
            "schema": "fcp.icse-demo-summary.v1",
            "implementation_commit": SOURCE_SHA,
            "passed": 4,
            "total": 4,
            "scenarios": [
                {"scenario": name, "result": "pass"}
                for name in (
                    "E1-selective-contribution", "E2-authority-boundary",
                    "E3-runtime-eligibility", "E4-ownership-boundary",
                )
            ],
        })

    network = tmp_path / "network-input"
    network.mkdir()
    for label in ("Linux", "Windows"):
        directory = network / f"icse-network-summary-{label}"
        directory.mkdir()
        events = [{"step": "fixture-only", "observation": {"synthetic_test_fixture": True}}]
        _write(directory / "summary.json", {
            "schema": "fcp.icse-network-demo.v1",
            "source_sha": SOURCE_SHA,
            "result": "PASS",
            "checks": dict.fromkeys(REQUIRED_NETWORK_CHECKS, "PASS"),
            "all_owned_processes_stopped": True,
            "events": events,
        })
        (directory / "events.jsonl").write_text(json.dumps(events[0]) + "\n", encoding="utf-8")
        (directory / "operator-report.html").write_text(
            "<!doctype html><p>Synthetic test fixture only.</p>", encoding="utf-8"
        )
    return components, network


@pytest.fixture
def publish(monkeypatch, tmp_path: Path, evidence):
    # These tests validate evidence/manifest boundaries, not Git or live networks.
    monkeypatch.chdir(Path(__file__).resolve().parents[3])
    monkeypatch.setattr(bundle, "_create_publication_archive", lambda **_kwargs: None)
    components, network = evidence

    def invoke(*, include_network: bool = True):
        output = tmp_path / "publication"
        bundle.build_bundle(
            source_revision=SOURCE_SHA,
            source_ref="refs/tags/fcp-icse-tool-demo-v0.1.0",
            workflow_run="fixture-run",
            version="0.1.0",
            evidence_root=components,
            output=output,
            network_evidence_root=network if include_network else None,
        )
        return output

    return invoke


def test_bundle_binds_both_network_executions_and_only_public_files(publish):
    output = publish()
    manifest = json.loads((output / "artifact-manifest.json").read_text(encoding="utf-8"))
    assert manifest["schema"] == "fcp.icse-artifact-manifest.v1"
    assert len(manifest["evidence"]) == 3
    assert {entry["execution"] for entry in manifest["network_evidence"]} == {"Linux", "Windows"}
    checksums = (output / "SHA256SUMS").read_text(encoding="utf-8")
    for entry in manifest["network_evidence"]:
        assert entry["source_revision"] == SOURCE_SHA
        assert tuple(entry["checks"]) == REQUIRED_NETWORK_CHECKS
        assert {Path(item["file"]).name for item in entry["files"]} == {
            "summary.json", "events.jsonl", "operator-report.html",
        }
        for item in entry["files"]:
            assert hashlib.sha256((output / item["file"]).read_bytes()).hexdigest() == item["sha256"]
            assert f'{item["sha256"]}  {item["file"]}' in checksums
    assert not list(output.rglob("private-state"))
    assert (output / "DEMONSTRATION.md").is_file()
    assert (output / "network/README.md").is_file()
    assert (output / "RELEASE.md").is_file()
    assert (output / "figures/federation-v1-overview.svg").is_file()


@pytest.mark.parametrize("field,value", [
    ("source_sha", "b" * 40),
    ("schema", "wrong-schema"),
    ("result", "STARTED"),
    ("result", "FAIL"),
    ("all_owned_processes_stopped", False),
    ("all_owned_processes_stopped", "true"),
    ("failure", {"type": "FixtureFailure"}),
    ("shutdown_errors", [{"label": "fixture-voter"}]),
    ("events", []),
])
def test_rejects_mixed_revision_partial_run_or_failed_teardown(evidence, publish, field, value):
    _, network = evidence
    path = network / "icse-network-summary-Windows/summary.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload[field] = value
    _write(path, payload)
    with pytest.raises(ValueError):
        publish()


@pytest.mark.parametrize("missing", REQUIRED_NETWORK_CHECKS)
def test_every_network_assertion_is_required(evidence, publish, missing):
    _, network = evidence
    path = network / "icse-network-summary-Linux/summary.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    del payload["checks"][missing]
    _write(path, payload)
    with pytest.raises(ValueError, match="network checks"):
        publish()


def test_rejects_extra_nonpassing_check(evidence, publish):
    _, network = evidence
    path = network / "icse-network-summary-Linux/summary.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["checks"]["future_check"] = "FAIL"
    _write(path, payload)
    with pytest.raises(ValueError, match="network checks"):
        publish()


def test_rejects_missing_windows_execution(evidence, publish):
    _, network = evidence
    (network / "icse-network-summary-Windows").rename(network / "icse-network-summary-other")
    with pytest.raises(ValueError, match="exactly Linux and Windows"):
        publish()


@pytest.mark.parametrize("private_name", ["private-state", "control-plane.secret", "worker.stderr.log"])
def test_rejects_private_material_in_network_artifact(evidence, publish, private_name):
    _, network = evidence
    artifact = network / "icse-network-summary-Linux"
    if private_name == "private-state":
        (artifact / private_name).mkdir()
        (artifact / private_name / "identity.json").write_text("fixture-only-private", encoding="utf-8")
    else:
        (artifact / private_name).write_text("fixture-only-private", encoding="utf-8")
    with pytest.raises(ValueError, match="only the three public"):
        publish()


def test_rejects_mixed_event_log(evidence, publish):
    _, network = evidence
    (network / "icse-network-summary-Linux/events.jsonl").write_text(
        '{"step":"different-fixture-run"}\n', encoding="utf-8"
    )
    with pytest.raises(ValueError, match="event log does not match"):
        publish()


def test_existing_component_requirement_is_not_replaced_by_network_pass(evidence, publish):
    components, _ = evidence
    path = components / "icse-summary-compose/icse-summary.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["passed"] = 3
    _write(path, payload)
    with pytest.raises(ValueError, match="four passing scenarios"):
        publish()


def test_component_only_bundle_remains_supported(publish):
    output = publish(include_network=False)
    manifest = json.loads((output / "artifact-manifest.json").read_text(encoding="utf-8"))
    assert len(manifest["evidence"]) == 3
    assert manifest["network_evidence"] == []


def test_rejects_stale_or_private_output_directory(tmp_path: Path, publish):
    output = tmp_path / "publication"
    output.mkdir()
    (output / "private-state").mkdir()
    with pytest.raises(ValueError, match="output must be new or empty"):
        publish()


def test_publication_archive_excludes_capture_without_removing_source(monkeypatch, tmp_path: Path):
    """Exercise the real Git archive boundary using synthetic telemetry only."""
    repository = Path(__file__).resolve().parents[3]
    source = tmp_path / "repository"
    source.mkdir()
    (source / ".gitattributes").write_bytes((repository / ".gitattributes").read_bytes())
    capture = source / "example-data/2026-03-23.jsonl"
    capture.parent.mkdir()
    capture.write_text('{"synthetic_test_fixture":true}\n', encoding="utf-8")
    (capture.parent / "README.md").write_text("Synthetic archive fixture.\n", encoding="utf-8")
    entrypoint = source / "demo/icse/network/run.py"
    entrypoint.parent.mkdir(parents=True)
    entrypoint.write_text("# Synthetic reviewer entrypoint fixture.\n", encoding="utf-8")
    private_note = source / "docs/implementation/digital_twin_from_recorder_data.md"
    private_note.parent.mkdir(parents=True)
    private_note.write_text("Synthetic private-capture analysis fixture.\n", encoding="utf-8")
    original_capture = capture.read_bytes()
    original_note = private_note.read_bytes()

    # Isolate the fixture from developer signing, hooks and line-ending policy.
    hooks = tmp_path / "empty-hooks"
    hooks.mkdir()
    global_config = tmp_path / "empty-gitconfig"
    global_config.write_text("", encoding="utf-8")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(global_config))
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.chdir(source)

    def git(*arguments: str) -> str:
        return subprocess.check_output(
            ["git", "-c", "user.name=Archive fixture", "-c", "user.email=archive@example.invalid",
             "-c", "commit.gpgSign=false", "-c", f"core.hooksPath={hooks}",
             "-c", "core.autocrlf=false", *arguments],
            text=True, encoding="utf-8", timeout=30,
        ).strip()

    git("init")
    git("add", ".gitattributes", "example-data", "demo", "docs")
    git("commit", "-m", "Synthetic publication fixture")
    revision = git("rev-parse", "HEAD")
    output = tmp_path / "publication"
    output.mkdir()
    prefix = "fcp-icse-tool-demo-candidate"
    _write(output / "artifact-manifest.json", {
        "schema": bundle.MANIFEST_SCHEMA,
        "artifact_version": "candidate",
        "source_revision": revision,
        "source_ref": "refs/heads/main",
        "publication_archive": {
            "file": prefix + ".zip",
            "source_prefix": prefix + "/source/",
            "artifact_prefix": prefix + "/artifact/",
        },
    })
    name, digest = bundle._create_publication_archive(
        source_revision=revision, version="candidate", output=output,
    )
    archive_path = output / name
    with zipfile.ZipFile(archive_path) as archive:
        names = set(archive.namelist())
        assert f"{prefix}/source/example-data/2026-03-23.jsonl" not in names
        assert f"{prefix}/source/docs/implementation/digital_twin_from_recorder_data.md" not in names
        assert f"{prefix}/source/example-data/README.md" in names
        assert f"{prefix}/source/demo/icse/network/run.py" in names
        archive.extractall(tmp_path / "extracted")
    assert capture.read_bytes() == original_capture
    assert private_note.read_bytes() == original_note
    assert git("ls-files", "example-data/2026-03-23.jsonl") == "example-data/2026-03-23.jsonl"
    assert not git("status", "--porcelain")
    assert (output / "ZENODO_SHA256").read_text(encoding="utf-8") == f"{digest}  {name}\n"
    identity = verify_source(
        tmp_path / "extracted" / prefix / "source", revision,
        source_archive=archive_path, archive_sha256=digest,
    )
    assert identity["source_files_verified"] == 3
    assert identity["source_sha"] == revision
