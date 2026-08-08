from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[3]

RELEASE_DOCS = (
    ROOT / "README.md",
    ROOT / "docs" / "index.md",
    ROOT / "docs" / "quick_start.md",
    ROOT / "docs" / "server_setup.md",
    ROOT / "docs" / "releases" / "federation_v1_scope.md",
    ROOT
    / "docs"
    / "implementation"
    / "federation"
    / "active"
    / "capability_first_federation_plan.md",
    ROOT
    / "docs"
    / "implementation"
    / "federation"
    / "active"
    / "federation_v1_closeout_plan.md",
    ROOT / "docs" / "implementation" / "current_task_handoff.md",
)

MARKDOWN_LINK = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")


def _local_target(source: Path, raw_target: str) -> Path | None:
    target = raw_target.strip().strip("<>")
    if not target:
        return None
    if target.startswith(("http://", "https://", "mailto:", "#", "/")):
        return None

    target = target.split(" ", 1)[0]
    target = target.split("#", 1)[0]
    target = target.split("?", 1)[0]
    if not target:
        return None

    return (source.parent / unquote(target)).resolve()


def test_release_critical_documentation_links_resolve() -> None:
    missing_docs = [str(path.relative_to(ROOT)) for path in RELEASE_DOCS if not path.exists()]
    assert not missing_docs, f"Missing release-critical documentation: {missing_docs}"

    broken: list[str] = []
    for source in RELEASE_DOCS:
        content = source.read_text(encoding="utf-8")
        for raw_target in MARKDOWN_LINK.findall(content):
            target = _local_target(source, raw_target)
            if target is None:
                continue
            try:
                target.relative_to(ROOT)
            except ValueError:
                broken.append(
                    f"{source.relative_to(ROOT)} -> {raw_target} (outside repository)"
                )
                continue
            if not target.exists():
                broken.append(f"{source.relative_to(ROOT)} -> {raw_target}")

    assert not broken, "Broken release-critical documentation links:\n" + "\n".join(broken)
