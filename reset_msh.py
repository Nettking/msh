"""Reset an MSH checkout while retaining recorded MTConnect JSONL data.

Run from the repository root:

    python reset_msh.py --yes

The command stops the Compose stack, removes named volumes (including Ollama
models and Federation relay state), clears generated local state, and preserves
only ``data/sources/mtconnect_recorder/jsonl/**/*.jsonl``.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ResetSummary:
    preserved_jsonl_files: int
    removed_files: int
    removed_directories: int


def _is_preserved_jsonl(path: Path, preserved_root: Path) -> bool:
    try:
        path.relative_to(preserved_root)
    except ValueError:
        return False
    return path.is_file() and not path.is_symlink() and path.suffix.casefold() == ".jsonl"


def _clear_tree(
    root: Path,
    *,
    preserved_root: Path | None = None,
) -> ResetSummary:
    if not root.exists():
        return ResetSummary(0, 0, 0)
    if root.is_symlink() or not root.is_dir():
        raise RuntimeError(f"Refusing to reset unsafe path: {root}")

    preserved = 0
    removed_files = 0
    removed_directories = 0
    paths = sorted(root.rglob("*"), key=lambda item: len(item.parts), reverse=True)
    for path in paths:
        if preserved_root is not None and _is_preserved_jsonl(path, preserved_root):
            preserved += 1
            continue
        if path.is_symlink() or path.is_file():
            path.unlink(missing_ok=True)
            removed_files += 1
            continue
        if path.is_dir():
            try:
                path.rmdir()
            except OSError:
                # A directory containing preserved JSONL files must remain.
                continue
            removed_directories += 1

    return ResetSummary(preserved, removed_files, removed_directories)


def reset_repository_state(
    repository_root: Path | str,
    *,
    stop_compose: bool = True,
) -> ResetSummary:
    root = Path(repository_root).resolve()
    if not (root / "docker-compose.yml").is_file():
        raise RuntimeError(
            "Run the reset from an MSH repository root containing docker-compose.yml."
        )

    if stop_compose:
        subprocess.run(
            [
                "docker",
                "compose",
                "down",
                "--remove-orphans",
                "--volumes",
            ],
            cwd=root,
            check=True,
        )

    data_root = root / "data"
    preserved_root = data_root / "sources" / "mtconnect_recorder" / "jsonl"
    data_summary = _clear_tree(data_root, preserved_root=preserved_root)
    results_summary = _clear_tree(root / "results")
    return ResetSummary(
        preserved_jsonl_files=data_summary.preserved_jsonl_files,
        removed_files=data_summary.removed_files + results_summary.removed_files,
        removed_directories=(
            data_summary.removed_directories + results_summary.removed_directories
        ),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Reset MSH identity, Federation, configuration, generated results, "
            "and Docker volumes while preserving recorder JSONL files."
        )
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Confirm the destructive reset.",
    )
    parser.add_argument(
        "--skip-docker",
        action="store_true",
        help="Do not stop Compose or remove its named volumes.",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    if not args.yes:
        print(
            "Reset cancelled. Re-run with --yes after verifying that only recorder "
            "JSONL data should remain."
        )
        return 2
    summary = reset_repository_state(
        Path(__file__).resolve().parent,
        stop_compose=not args.skip_docker,
    )
    print("MSH fresh-install reset complete.")
    print(f"Preserved recorder JSONL files: {summary.preserved_jsonl_files}")
    print(f"Removed files: {summary.removed_files}")
    print(f"Removed directories: {summary.removed_directories}")
    print("Start again with: docker compose up -d --build")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
