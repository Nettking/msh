from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

LEGACY = bytes((109, 115, 104)).decode("ascii")
SPACED_LEGACY = " ".join(LEGACY)
REPOSITORY_SLUG = "Nettking/" + LEGACY
ALLOWED_REPOSITORY_FILES = frozenset(
    {
        "catalog/federation/software_update.py",
        "scripts/posix/fcp_update_agent.py",
        "scripts/posix/fcp_update_engine.py",
        "scripts/windows/fcp_update_agent.ps1",
        "scripts/windows/fcp_update_engine.ps1",
        "scripts/windows/migrate_existing_fcp.ps1",
        "cmd/fcp-peer-sidecar/go.mod",
        "catalog/federation/tests/test_software_update.py",
        "catalog/flask_app/tests/test_federation_update_host_agents.py",
        "catalog/flask_app/tests/test_federation_update_runtime.py",
        "catalog/flask_app/tests/test_termux_federation_update_agent.py",
        "catalog/flask_app/tests/test_windows_migration_script.py",
        "termux/fcp-phone-update-agent.sh",
        "termux/fcp_phone_update_codec.py",
        # Exact repository identity in archive protocol/provenance, not branding.
        "scripts/artifact_archive.py",
        "scripts/artifact_archive_ci.py",
        "scripts/tests/test_artifact_archive.py",
        # This regression fixture binds synthetic manifests to the exact repo.
        "scripts/tests/test_icse_workflow_evidence_verifier.py",
        "docs/implementation/nitro_artifact_archive.md",
    }
)

# These exact strings identify an existing physical host, a hostile-path
# redaction fixture, frozen cryptographic domain separators, or repository
# comparisons that protect self-hosted runners. They are not
# product branding. Renaming the host would rewrite acceptance provenance;
# changing a domain separator would break protocol/credential compatibility.
# Keep each exception scoped to its existing file and reject surrounding or
# additional retired product spellings normally.
_TRUSTED_REPOSITORY_GUARD_LITERALS = (
    "github.repository == '" + REPOSITORY_SLUG + "'",
    "github.event.pull_request.head.repo.full_name == '" + REPOSITORY_SLUG + "'",
)
ALLOWED_NON_PRODUCT_LITERALS = {
    ".github/workflows/cfi3-device-inspection-composition.yml": (
        _TRUSTED_REPOSITORY_GUARD_LITERALS
    ),
    ".github/workflows/cfi4-benchmark-composition.yml": (
        _TRUSTED_REPOSITORY_GUARD_LITERALS
    ),
    ".github/workflows/cfi5-contribution-composition.yml": (
        _TRUSTED_REPOSITORY_GUARD_LITERALS
    ),
    ".github/workflows/docs-portal.yml": _TRUSTED_REPOSITORY_GUARD_LITERALS,
    "catalog/federation/control_plane_credentials.py": (
        LEGACY.upper() + " FCP Federation v1 private human credential replica",
    ),
    "catalog/federation/control_plane_transport.py": (
        LEGACY.upper() + " FCP Federation v1 replicated authority transport",
    ),
    "catalog/federation/recorder_control_plane_voter.py": (LEGACY.upper() + " Recorder",),
    "catalog/federation/tests/cf7_acceptance/test_b01_b09_physical_contract.py": (
        "C:\\\\" + LEGACY + "\\\\secret",
    ),
    "catalog/federation/tests/test_c03_offline_creator_migration.py": (LEGACY + "-recorder",),
    "catalog/federation/tests/test_recorder_control_plane_voter.py": (LEGACY + "-recorder",),
    "docs/implementation/v1_b01_b09_physical_acceptance.md": (
        LEGACY.upper() + " Recorder", LEGACY + "-recorder",
    ),
    # Existing physical host/path labels are operational provenance, not the
    # product name. Keep the exceptions literal and file-scoped.
    "docs/implementation/nitro_artifact_archive.md": (
        LEGACY.upper() + "-to-Nitro Recorder backup",
    ),
    "scripts/windows/tests/test_recorder_pause_resume_guard.py": (
        "C:\\" + LEGACY + "\\git",
    ),
    "scripts/acceptance/b01_b09_physical_contract.py": (LEGACY.upper() + " Recorder restart",),
}


def searchable_text(path_text: str, text: str) -> str:
    if path_text in ALLOWED_REPOSITORY_FILES:
        text = re.sub(re.escape(REPOSITORY_SLUG), "", text, flags=re.IGNORECASE)
    for literal in ALLOWED_NON_PRODUCT_LITERALS.get(path_text, ()):
        text = re.sub(r"(?<![\w-])" + re.escape(literal) + r"(?![\w-])", "", text)
    return text


def main() -> int:
    failures: list[str] = []
    tracked = subprocess.check_output(
        ["git", "ls-files", "-z"], text=False
    ).split(b"\0")
    for raw in tracked:
        if not raw:
            continue
        path_text = raw.decode("utf-8")
        if LEGACY in path_text.lower():
            failures.append(f"retired product spelling in path: {path_text}")
            continue
        path = Path(path_text)
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        searchable = searchable_text(path_text, text)
        lowered = searchable.casefold()
        if LEGACY in lowered or SPACED_LEGACY in lowered:
            failures.append(f"retired product spelling in file: {path_text}")
    if failures:
        print("\n".join(failures))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
