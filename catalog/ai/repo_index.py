"""Small repository index for the read-only MSH AI explainer."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

INCLUDE_SUFFIXES = {".md", ".py", ".yml", ".yaml", ".txt"}
INCLUDE_FILES = {"Dockerfile", "Dockerfile.cli", "requirements.txt"}
EXCLUDED_DIRS = {".git", "__pycache__", "data", "results", "legacy"}
EXCLUDED_SUFFIXES = {".pyc", ".parquet", ".duckdb", ".jsonl"}
TOKEN_RE = re.compile(r"[A-Za-z0-9_./-]+")


@dataclass(frozen=True)
class Chunk:
    path: str
    start_line: int
    end_line: int
    text: str

    def format_for_prompt(self) -> str:
        return f"--- {self.path}:{self.start_line}-{self.end_line} ---\n{self.text.strip()}"


def repo_root_from(start: Path | None = None) -> Path:
    current = (start or Path.cwd()).resolve()
    for candidate in (current, *current.parents):
        if (candidate / "README.md").exists() and (candidate / "catalog").exists():
            return candidate
    raise FileNotFoundError("Could not find repository root containing README.md and catalog/.")


def should_index(path: Path, root: Path) -> bool:
    if not path.is_file():
        return False
    relative = path.relative_to(root)
    if set(relative.parts) & EXCLUDED_DIRS:
        return False
    if path.suffix in EXCLUDED_SUFFIXES:
        return False
    if path.name in INCLUDE_FILES:
        return True
    if path.suffix not in INCLUDE_SUFFIXES:
        return False
    if relative.parts[0] in {"docs", "catalog"}:
        return True
    return path.name == "README.md"


def iter_indexed_files(root: Path) -> list[Path]:
    return sorted(path for path in root.rglob("*") if should_index(path, root))


def chunk_text(path: Path, root: Path, max_lines: int = 80) -> list[Chunk]:
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    chunks: list[Chunk] = []
    rel = path.relative_to(root).as_posix()
    for start in range(0, len(lines), max_lines):
        block = lines[start : start + max_lines]
        text = "\n".join(block).strip()
        if text:
            chunks.append(Chunk(rel, start + 1, start + len(block), text))
    return chunks


def build_chunks(root: Path) -> list[Chunk]:
    chunks: list[Chunk] = []
    for path in iter_indexed_files(root):
        chunks.extend(chunk_text(path, root))
    return chunks


def tokenize(text: str) -> set[str]:
    return {token.lower() for token in TOKEN_RE.findall(text) if len(token) > 2}
