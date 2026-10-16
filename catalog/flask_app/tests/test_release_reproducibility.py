from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
PYTHON_IMAGE = (
    "python:3.12.13-slim@"
    "sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de"
)
OLLAMA_IMAGE = (
    "ollama/ollama:0.32.6@"
    "sha256:b88c73ace3e115f8ec53dc8761ae1c0aabfa675406e3681786b98757ce050f42"
)


def test_release_container_inputs_are_immutable_by_default() -> None:
    for name in ("Dockerfile", "Dockerfile.cli"):
        text = (ROOT / name).read_text(encoding="utf-8")
        assert text.startswith(f"FROM {PYTHON_IMAGE}\n")
        assert "constraints-release.txt" in text
        assert "pip==26.2.1" in text
        assert "-c /app/constraints-release.txt" in text

    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "ollama/ollama:latest" not in compose
    assert compose.count(f"image: {OLLAMA_IMAGE}") == 4
    assert "image: ollama/ollama:0.32.6\n" not in compose


def test_release_constraint_file_is_exact() -> None:
    text = (ROOT / "constraints-release.txt").read_text(encoding="utf-8")
    lines = [
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.startswith("#")
    ]
    assert lines
    exact = re.compile(
        r'^[A-Za-z0-9_.-]+==[^;\s]+(?:; platform_system == "Windows")?$'
    )
    assert all(exact.fullmatch(line) for line in lines)
    for required in (
        "Flask==3.1.3",
        "pandas==3.0.5",
        "numpy==2.5.2",
        "cryptography==43.0.3",
        "pytest==9.1.1",
        "pytest-randomly==4.1.0",
        "ruff==0.16.3",
        "pip==26.2.1",
    ):
        assert required in lines


def test_release_workflow_uses_locked_inputs_and_covers_build_files() -> None:
    text = (ROOT / ".github/workflows/federation-v1-release.yml").read_text(
        encoding="utf-8"
    )
    assert 'python: "3.12.13"' in text
    assert 'python: "3.12.10"' in text
    assert "python-version: ${{ matrix.python }}" in text
    assert "pip install --upgrade pip==26.2.1" in text
    assert "constraints-release.txt" in text
    assert "pip install pytest ruff" not in text
    for required in (
        '"Dockerfile"',
        '"Dockerfile.cli"',
        '"constraints-release.txt"',
        '"migrate.cmd"',
        '"start.sh"',
        '"setup_fcp.py"',
    ):
        assert required in text


def test_v1_ci_does_not_depend_on_a_named_runner_or_archive_device() -> None:
    release = (ROOT / ".github/workflows/federation-v1-release.yml").read_text(
        encoding="utf-8"
    )
    assert "runs-on: ${{ matrix.runner }}" in release
    assert release.count("runs-on: [self-hosted, Linux, X64, fcp-linux-fast]") == 6
    assert "runs-on: [self-hosted, Windows, X64, fcp-windows]" in release
    assert "NITRO_ARTIFACT_" not in release
    assert ".github/actions/nitro-artifact" not in release
    assert "runner: [self-hosted, Linux, X64, fcp-linux-fast]" in release
    assert "runner: [self-hosted, Windows, X64, fcp-windows]" in release
    assert "actions/self-hosted-python" in release
    assert "pull_request:" not in release
    assert 'branches: ["**"]' in release
    assert "actions/upload-artifact@v4" in release
    assert "actions/download-artifact@v4" in release
    assert release.count("retention-days: 90") == 4
    assert "${{ github.run_id }}" in release
    assert "${{ github.run_attempt }}" in release
    assert "scripts/ci_release_disk_preflight.py C:\\" in release
    path_compatibility = release.split(
        "- name: Windows path-sensitive startup and migration regressions", 1
    )[1].split("- name: Windows transport, storage, and failover regressions", 1)[0]
    assert "if: ${{ !cancelled() && matrix.suite == 'capability-product' }}" in (
        path_compatibility
    )
    assert "TEMP: '${{ env.FCP_CI_JOB_ROOT }}\\windows-compat'" in path_compatibility
    assert "TMP: '${{ env.FCP_CI_JOB_ROOT }}\\windows-compat'" in path_compatibility
    assert "\\\\?\\" not in path_compatibility
    assert "windows-path-compat.xml" in path_compatibility
    assert "windows-path-sensitive-compat" in release
    for windows_only_suite in (
        "catalog/federation/tests/test_host_mutation_activation.py",
        "catalog/federation/tests/test_windows_controlled_build_contract.py",
        "catalog/flask_app/tests/test_first_federation_start.py",
        "catalog/flask_app/tests/test_federated_jsonl_candidate_filter.py",
        "catalog/flask_app/tests/test_model_resource_admission_paths.py",
        "catalog/flask_app/tests/test_windows_fresh_shutdown.py",
        "catalog/flask_app/tests/test_windows_fresh_url_output.py",
        "catalog/flask_app/tests/test_windows_migration_script.py",
        "catalog/flask_app/tests/test_windows_relay_volume_selector.py",
        "catalog/flask_app/tests/test_windows_runtime_state_resolver.py",
    ):
        assert windows_only_suite in release
        assert windows_only_suite in path_compatibility

    self_hosted_python = (ROOT / ".github/actions/self-hosted-python/action.yml").read_text(
        encoding="utf-8"
    )
    assert "/opt/fcp-ci/python/3.12.13/bin/python3.12" in self_hosted_python
    assert "v == '3.12.10'" in self_hosted_python
    assert "python -m venv" in self_hosted_python
    assert "Git Bash is required" in self_hosted_python
    assert "./.github/actions/self-hosted-python/job-environment" in self_hosted_python
    assert "actions/setup-python" not in self_hosted_python

    postgres = (ROOT / ".github/actions/self-hosted-postgres/action.yml").read_text(
        encoding="utf-8"
    )
    assert "uses: ./.github/actions/self-hosted-postgres" in release
    assert "systemctl" not in release
    assert "openssl rand -hex 24" in postgres
    assert "test \"$version_num\" -ge 160000" in postgres
    assert "test \"$version_num\" -lt 170000" in postgres
    assert "--publish 127.0.0.1::5432" in postgres
    assert "operation: stop" in release

    tooling = (
        ROOT / ".github/workflows/federation-v1-physical-campaign.yml"
    ).read_text(encoding="utf-8")
    assert "runner: [self-hosted, Linux, X64, fcp-linux-fast]" in tooling
    assert "runner: [self-hosted, Windows, X64, fcp-windows]" in tooling
    assert "actions/self-hosted-python" in tooling


def test_release_image_metadata_gate_checks_exact_digests() -> None:
    workflow = (ROOT / ".github/workflows/release-image-metadata.yml").read_text(
        encoding="utf-8"
    )
    assert "runs-on: [self-hosted, Linux, X64, fcp-linux-fast]" in workflow
    assert "docker buildx imagetools inspect" in workflow
    assert PYTHON_IMAGE in workflow
    assert OLLAMA_IMAGE in workflow
    assert "linux/amd64" in workflow
    assert "linux/arm64" in workflow


def test_v1_release_finalization_does_not_require_a_follow_up_source_commit() -> None:
    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    assert "## [1.0.0]" in changelog
    assert "publication status and release date" in changelog
    assert "before" in changelog and "physical release acceptance" in changelog
    assert "same accepted\ncommit" in changelog
    assert "Do not create a source-only commit" in changelog
    assert "no `v1.0.0` tag exists yet" not in changelog

    procedure = (ROOT / "docs/release_process.md").read_text(encoding="utf-8")
    normalized_procedure = " ".join(procedure.split())
    for required in (
        "source tree is finalized **before** physical acceptance",
        "Record `C` in the physical acceptance evidence",
        "create the Git tag `v1.0.0` **at `C`**",
        "GitHub Release from tag `v1.0.0`",
        "must not require another source commit",
    ):
        assert required in normalized_procedure

    docs_index = (ROOT / "docs/index.md").read_text(encoding="utf-8")
    assert "[FCP v1 release process](release_process.md)" in docs_index
