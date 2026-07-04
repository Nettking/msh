"""Small repository index for the read-only MSH AI explainer."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
import re
from typing import Any

INCLUDE_SUFFIXES = {".md", ".py", ".yml", ".yaml", ".txt"}
INCLUDE_FILES = {"Dockerfile", "Dockerfile.cli", "requirements.txt"}
EXCLUDED_DIRS = {".git", ".cache", "__pycache__", "data", "results", "legacy"}
EXCLUDED_SUFFIXES = {".pyc", ".parquet", ".duckdb", ".jsonl"}
TOKEN_RE = re.compile(r"[A-Za-z0-9_./-]+")
INDEX_VERSION = 1
DEFAULT_INDEX_PATH = Path(".cache") / "msh_ai_index.json"


@dataclass(frozen=True)
class Chunk:
    path: str
    start_line: int
    end_line: int
    text: str

    def format_for_prompt(self) -> str:
        return f"--- {self.path}:{self.start_line}-{self.end_line} ---\n{self.text.strip()}"

    def source_label(self) -> str:
        return f"{self.path}:{self.start_line}-{self.end_line}"


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


def _file_fingerprint(root: Path) -> list[dict[str, Any]]:
    fingerprint: list[dict[str, Any]] = []
    for path in iter_indexed_files(root):
        stat = path.stat()
        fingerprint.append(
            {
                "path": path.relative_to(root).as_posix(),
                "mtime_ns": stat.st_mtime_ns,
                "size": stat.st_size,
            }
        )
    return fingerprint


def _chunk_from_dict(payload: dict[str, Any]) -> Chunk:
    return Chunk(
        path=str(payload["path"]),
        start_line=int(payload["start_line"]),
        end_line=int(payload["end_line"]),
        text=str(payload["text"]),
    )


def load_cached_chunks(root: Path, index_path: Path | None = None) -> list[Chunk] | None:
    path = root / (index_path or DEFAULT_INDEX_PATH)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if payload.get("version") != INDEX_VERSION:
        return None
    if payload.get("fingerprint") != _file_fingerprint(root):
        return None
    chunks = payload.get("chunks")
    if not isinstance(chunks, list):
        return None
    try:
        return [_chunk_from_dict(item) for item in chunks if isinstance(item, dict)]
    except (KeyError, TypeError, ValueError):
        return None


def save_cached_chunks(root: Path, chunks: list[Chunk], index_path: Path | None = None) -> Path:
    path = root / (index_path or DEFAULT_INDEX_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": INDEX_VERSION,
        "fingerprint": _file_fingerprint(root),
        "chunks": [asdict(chunk) for chunk in chunks],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def load_or_build_chunks(root: Path, *, use_cache: bool = True, rebuild: bool = False) -> list[Chunk]:
    if use_cache and not rebuild:
        cached = load_cached_chunks(root)
        if cached is not None:
            return cached
    chunks = build_chunks(root)
    if use_cache:
        save_cached_chunks(root, chunks)
    return chunks


def tokenize(text: str) -> set[str]:
    return {token.lower() for token in TOKEN_RE.findall(text) if len(token) > 2}
