"""Physical identities and protocol constants must not waive product branding."""

from pathlib import Path

import pytest

from scripts import check_product_branding as branding

LEGACY = bytes((109, 115, 104)).decode('ascii')
CASES = [
    ('catalog/federation/control_plane_credentials.py', LEGACY.upper() + ' FCP Federation v1 private human credential replica'),
    ('catalog/federation/control_plane_transport.py', LEGACY.upper() + ' FCP Federation v1 replicated authority transport'),
    ('catalog/federation/recorder_control_plane_voter.py', LEGACY.upper() + ' Recorder'),
    ('catalog/federation/tests/cf7_acceptance/test_b01_b09_physical_contract.py', 'C:\\\\' + LEGACY + '\\\\secret'),
    ('catalog/federation/tests/test_c03_offline_creator_migration.py', LEGACY + '-recorder'),
    ('catalog/federation/tests/test_recorder_control_plane_voter.py', LEGACY + '-recorder'),
    ('docs/implementation/v1_b01_b09_physical_acceptance.md', LEGACY.upper() + ' Recorder'),
    ('docs/implementation/v1_b01_b09_physical_acceptance.md', LEGACY + '-recorder'),
    ('scripts/acceptance/b01_b09_physical_contract.py', LEGACY.upper() + ' Recorder restart'),
]


def _scan(tmp_path: Path, monkeypatch, path: str, text: str) -> int:
    target = tmp_path / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding='utf-8')
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(branding.subprocess, 'check_output', lambda *args, **kwargs: path.encode() + b'\0')
    return branding.main()


@pytest.mark.parametrize(('path', 'literal'), CASES)
def test_preserves_exact_existing_non_product_identity(tmp_path, monkeypatch, path, literal):
    assert _scan(tmp_path, monkeypatch, path, '"' + literal + '"') == 0


@pytest.mark.parametrize(('path', 'literal'), CASES)
def test_same_file_still_rejects_retired_product_branding(tmp_path, monkeypatch, path, literal):
    assert _scan(tmp_path, monkeypatch, path, '"' + literal + '"\nWelcome to ' + LEGACY.upper()) == 1


@pytest.mark.parametrize(('path', 'literal'), CASES)
def test_non_product_exception_cannot_spread_to_other_files(tmp_path, monkeypatch, path, literal):
    assert _scan(tmp_path, monkeypatch, 'catalog/flask_app/app.py', literal) == 1


@pytest.mark.parametrize('suffix', ['-clone', 's'])
def test_hardware_exception_requires_the_exact_host_name(tmp_path, monkeypatch, suffix):
    path = 'catalog/federation/tests/test_recorder_control_plane_voter.py'
    assert _scan(tmp_path, monkeypatch, path, LEGACY + '-recorder' + suffix) == 1


def test_retired_product_path_remains_forbidden(tmp_path, monkeypatch):
    assert _scan(tmp_path, monkeypatch, LEGACY + '.py', 'FCP') == 1


@pytest.mark.parametrize('path', [
    'scripts/artifact_archive.py',
    'scripts/artifact_archive_ci.py',
    'scripts/tests/test_artifact_archive.py',
    'docs/implementation/nitro_artifact_archive.md',
])
def test_archive_repository_identity_does_not_waive_product_branding(tmp_path, monkeypatch, path):
    assert _scan(tmp_path, monkeypatch, path, branding.REPOSITORY_SLUG) == 0
    assert _scan(tmp_path, monkeypatch, path, branding.REPOSITORY_SLUG + '\nWelcome to ' + LEGACY.upper()) == 1
    assert _scan(tmp_path, monkeypatch, 'unrelated.py', branding.REPOSITORY_SLUG) == 1


WORKFLOW_GUARD_PATHS = (
    ".github/workflows/cfi3-device-inspection-composition.yml",
    ".github/workflows/cfi4-benchmark-composition.yml",
    ".github/workflows/cfi5-contribution-composition.yml",
    ".github/workflows/docs-portal.yml",
)
REPOSITORY_GUARD_FIELDS = (
    "github.repository",
    "github.event.pull_request.head.repo.full_name",
)


def _repository_guard(repository: str = branding.REPOSITORY_SLUG) -> str:
    return (
        "    if: >-\n"
        f"      github.repository == '{repository}' &&\n"
        "      (github.event_name != 'pull_request' ||\n"
        f"       github.event.pull_request.head.repo.full_name == '{repository}')\n"
    )


@pytest.mark.parametrize("path", WORKFLOW_GUARD_PATHS)
def test_workflow_exact_repository_security_guard_is_not_branding(
    tmp_path, monkeypatch, path
):
    assert _scan(tmp_path, monkeypatch, path, _repository_guard()) == 0


@pytest.mark.parametrize("path", WORKFLOW_GUARD_PATHS)
@pytest.mark.parametrize("product_name", [LEGACY.upper(), " ".join(LEGACY)])
def test_workflow_guard_does_not_waive_additional_product_branding(
    tmp_path, monkeypatch, path, product_name
):
    text = _repository_guard() + "    name: Welcome to " + product_name
    assert _scan(tmp_path, monkeypatch, path, text) == 1


@pytest.mark.parametrize("field", REPOSITORY_GUARD_FIELDS)
def test_repository_security_expression_exception_does_not_spread_to_other_files(
    tmp_path, monkeypatch, field
):
    expression = f"{field} == '{branding.REPOSITORY_SLUG}'"
    assert _scan(tmp_path, monkeypatch, ".github/workflows/unrelated.yml", expression) == 1


@pytest.mark.parametrize("path", WORKFLOW_GUARD_PATHS)
@pytest.mark.parametrize("field", REPOSITORY_GUARD_FIELDS)
@pytest.mark.parametrize("suffix", ["-fork", "s"])
def test_repository_security_expression_requires_exact_repository_identity(
    tmp_path, monkeypatch, path, field, suffix
):
    expression = f"{field} == '{branding.REPOSITORY_SLUG}{suffix}'"
    assert _scan(tmp_path, monkeypatch, path, expression) == 1


@pytest.mark.parametrize("path", WORKFLOW_GUARD_PATHS)
def test_workflow_repository_slug_is_not_exempt_outside_security_expression(
    tmp_path, monkeypatch, path
):
    assert _scan(tmp_path, monkeypatch, path, branding.REPOSITORY_SLUG) == 1
