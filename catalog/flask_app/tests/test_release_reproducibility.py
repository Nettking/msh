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
        assert text.count(required) >= 2


def test_release_image_metadata_gate_checks_exact_digests() -> None:
    text = (ROOT / ".github/workflows/release-image-metadata.yml").read_text(
        encoding="utf-8"
    )
    assert "docker buildx imagetools inspect" in text
    assert PYTHON_IMAGE in text
    assert OLLAMA_IMAGE in text
    assert "linux/amd64" in text
    assert "linux/arm64" in text


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
