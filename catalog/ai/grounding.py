"""Grounding guardrails for AI explainer answers."""

from __future__ import annotations

import re
from pathlib import Path

from .repo_index import Chunk

PATH_RE = re.compile(r"\b[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)+(?:\.[A-Za-z0-9_]+)?\b")


def allowed_source_paths(chunks: list[Chunk]) -> set[str]:
    """Return exact file paths that were supplied to the model as context."""
    return {chunk.path for chunk in chunks}


def referenced_paths(answer: str) -> set[str]:
    """Extract path-like references from an answer."""
    return {match.group(0).strip("`.,:;)]") for match in PATH_RE.finditer(answer)}


def unsupported_references(answer: str, chunks: list[Chunk]) -> set[str]:
    """Return path-like references that were not in the retrieved context."""
    allowed = allowed_source_paths(chunks)
    unsupported: set[str] = set()
    for ref in referenced_paths(answer):
        normalized = Path(ref).as_posix()
        if normalized not in allowed:
            unsupported.add(ref)
    return unsupported


def append_grounding_warning(answer: str, chunks: list[Chunk]) -> str:
    """Append a warning when the model mentions files outside retrieved context."""
    unsupported = sorted(unsupported_references(answer, chunks))
    if not unsupported:
        return answer
    warning = [
        "",
        "Grounding warning:",
        "The model mentioned file paths that were not present in the retrieved context:",
    ]
    warning.extend(f"- {path}" for path in unsupported)
    warning.append("Rerun with --dry-run --show-sources and treat those references as unsupported.")
    return answer.rstrip() + "\n" + "\n".join(warning)
