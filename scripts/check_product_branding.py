from __future__ import annotations

from pathlib import Path
import subprocess
import sys

LEGACY = bytes((109, 115, 104)).decode("ascii")
REPOSITORY_SLUG = "Nettking/" + LEGACY
ALLOWED_REPOSITORY_FILES = frozenset(
    {
        "catalog/federation/software_update.py",
        "scripts/posix/fcp_update_agent.py",
        "scripts/windows/fcp_update_agent.ps1",
        "cmd/fcp-peer-sidecar/go.mod",
        "catalog/federation/tests/test_software_update.py",
        "catalog/flask_app/tests/test_federation_update_host_agents.py",
        "catalog/flask_app/tests/test_federation_update_runtime.py",
    }
)


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
        searchable = text
        if path_text in ALLOWED_REPOSITORY_FILES:
            searchable = searchable.replace(REPOSITORY_SLUG, "")
        if LEGACY in searchable.lower():
            failures.append(f"retired product spelling in file: {path_text}")
    if failures:
        print("\n".join(failures))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
