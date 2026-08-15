from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PYTHON_IMAGE = (
    "python:3.12.13-slim@"
    "sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de"
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
    assert compose.count("ollama/ollama:0.32.6") == 4


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


def test_changelog_does_not_claim_v1_is_released() -> None:
    text = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    assert "## [Unreleased]" in text
    assert "Candidate for v1.0.0" in text
    assert "not a published release" in text
    assert "no `v1.0.0` tag exists yet" in text
