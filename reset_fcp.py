"""Factory-reset an FCP checkout while retaining immutable machine recordings.

Run from the repository root::

    python reset_fcp.py --yes

``--fresh`` has one deliberately narrow persistence exception: the complete
``data/sources/mtconnect_recorder`` recording tree is retained.  Everything
else that belongs to the previous local FCP installation is removed, including
human authentication state, device/Federation identity and trust, onboarding
state, capability configuration, benchmarks, jobs, activity history, recorder
runtime/checkpoints/configuration, generated results, relay state, and model
volumes.

The complete recorder tree is retained rather than only its derived JSONL
projection because raw MTConnect batches, probes, manifests/digests and other
integrity material are part of the verifiable recording.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import uuid
from dataclasses import dataclass
from pathlib import Path


RECORDER_RECORDINGS_RELATIVE = Path("data") / "sources" / "mtconnect_recorder"


@dataclass(frozen=True)
class ResetSummary:
    preserved_recording_files: int
    removed_files: int
    removed_directories: int

    @property
    def preserved_jsonl_files(self) -> int:
        """Backward-compatible alias for callers of the pre-v1 reset helper."""

        return self.preserved_recording_files


def _tree_counts(root: Path) -> tuple[int, int]:
    if not root.exists():
        return 0, 0
    files = 0
    directories = 0
    for path in root.rglob("*"):
        if path.is_symlink():
            raise RuntimeError(f"Refusing to preserve symlinked recorder content: {path}")
        if path.is_file():
            files += 1
        elif path.is_dir():
            directories += 1
    return files, directories


def _remove_tree(root: Path) -> tuple[int, int]:
    if not root.exists():
        return 0, 0
    if root.is_symlink() or not root.is_dir():
        raise RuntimeError(f"Refusing to reset unsafe path: {root}")
    files, directories = _tree_counts(root)
    shutil.rmtree(root)
    return files, directories + 1


def reset_repository_state(
    repository_root: Path | str,
    *,
    stop_compose: bool = True,
) -> ResetSummary:
    """Return the checkout to factory-new FCP state except recorder evidence.

    The recording tree is moved outside ``data`` while generated state is
    removed, then restored to the exact canonical path.  A same-filesystem
    rename is used so even a large recording archive does not need to be
    copied.  If reset fails after the move, restoration is attempted before the
    error is propagated.
    """

    root = Path(repository_root).resolve()
    if not (root / "docker-compose.yml").is_file():
        raise RuntimeError(
            "Run the reset from an FCP repository root containing docker-compose.yml."
        )

    if stop_compose:
        # Volumes contain relay authority and downloaded/model-provider state.
        # A factory reset must remove them; only machine recordings survive.
        subprocess.run(
            ["docker", "compose", "down", "--remove-orphans", "--volumes"],
            cwd=root,
            check=True,
        )

    data_root = root / "data"
    recordings_root = root / RECORDER_RECORDINGS_RELATIVE
    preserve_root = root / f".fcp-preserved-recordings-{uuid.uuid4().hex}"

    if preserve_root.exists():
        raise RuntimeError(f"Unexpected preservation path already exists: {preserve_root}")

    preserved_files, _ = _tree_counts(recordings_root)
    moved_recordings = False
    removed_files = 0
    removed_directories = 0

    try:
        if recordings_root.exists():
            if recordings_root.is_symlink() or not recordings_root.is_dir():
                raise RuntimeError(
                    f"Refusing to preserve unsafe recorder path: {recordings_root}"
                )
            recordings_root.rename(preserve_root)
            moved_recordings = True

        files, directories = _remove_tree(data_root)
        removed_files += files
        removed_directories += directories

        files, directories = _remove_tree(root / "results")
        removed_files += files
        removed_directories += directories

        if moved_recordings:
            recordings_root.parent.mkdir(parents=True, exist_ok=True)
            preserve_root.rename(recordings_root)
            moved_recordings = False
    except Exception:
        # Best-effort evidence recovery: never knowingly leave the recording
        # archive stranded at the temporary path after a failed reset.
        if moved_recordings and preserve_root.exists() and not recordings_root.exists():
            recordings_root.parent.mkdir(parents=True, exist_ok=True)
            preserve_root.rename(recordings_root)
            moved_recordings = False
        raise
    finally:
        if preserve_root.exists() and not moved_recordings:
            shutil.rmtree(preserve_root)

    return ResetSummary(
        preserved_recording_files=preserved_files,
        removed_files=removed_files,
        removed_directories=removed_directories,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Factory-reset FCP while preserving only immutable MTConnect machine "
            "recordings and their integrity metadata."
        )
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Confirm the destructive factory reset.",
    )
    parser.add_argument(
        "--skip-docker",
        action="store_true",
        help="Do not stop Compose or remove its named volumes (test/development only).",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    if not args.yes:
        print(
            "Reset cancelled. Re-run with --yes after verifying that only immutable "
            "machine recordings should survive."
        )
        return 2

    summary = reset_repository_state(
        Path(__file__).resolve().parent,
        stop_compose=not args.skip_docker,
    )
    print("FCP factory reset complete.")
    print(f"Preserved recorder evidence files: {summary.preserved_recording_files}")
    print(f"Removed files: {summary.removed_files}")
    print(f"Removed directories: {summary.removed_directories}")
    print("All previous auth, Federation and generated runtime state has been removed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
