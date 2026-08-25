"""Host-owned build lifecycle shared by supported POSIX start/update paths.

This module deliberately owns only the checkout/build critical section. Runtime
activation remains the responsibility of the launcher or host update agent.
The serialized update runner calls ``host_build.host_mutation_lock(root)`` while
this module's launcher path enters the same local ``host_mutation_lock`` below.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path

try:  # pragma: no cover - exercised only on supported POSIX hosts
    import fcntl
except ImportError:  # pragma: no cover - Windows uses the PowerShell primitive
    fcntl = None  # type: ignore[assignment]

OID_RE = re.compile(r"^[0-9a-f]{40}$")
UPDATE_REQUIRED_FREE_BYTES = 10 * 1024**3
BUILD_CACHE_KEEP_BYTES = 8 * 1024**3
HOST_MUTATION_LOCK_TIMEOUT_SECONDS = 30.0


def _run_git(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        ["git", *args],
        cwd=root,
        shell=False,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if completed.returncode:
        raise RuntimeError(f"git_failed:{args[0]}:{completed.returncode}")
    return completed


def mutation_lock_path(root: Path) -> Path:
    """Return one lock path for every supported actor using this checkout."""

    raw = _run_git(root, "rev-parse", "--git-path", "fcp-host-mutation.lock").stdout.strip()
    if not raw:
        raise RuntimeError("host_mutation_lock_unavailable")
    path = Path(raw)
    if not path.is_absolute():
        path = root / path
    return path.resolve()


@contextmanager
def host_mutation_lock(
    root: Path,
    *,
    timeout_seconds: float = HOST_MUTATION_LOCK_TIMEOUT_SECONDS,
) -> Iterator[None]:
    """Serialize supported source mutation and build-context reads for a checkout."""

    if fcntl is None:
        raise RuntimeError("host_mutation_lock_unsupported")
    if timeout_seconds < 0:
        raise ValueError("timeout_seconds must be non-negative")
    path = mutation_lock_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout_seconds
    with path.open("a+", encoding="utf-8") as stream:
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
        while True:
            try:
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise RuntimeError("host_mutation_busy") from None
                time.sleep(0.1)
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def resolve_clean_commit(root: Path) -> str:
    commit = _run_git(root, "rev-parse", "--verify", "HEAD^{commit}").stdout.strip().lower()
    if not OID_RE.fullmatch(commit):
        raise RuntimeError("unsupported_checkout")
    status = _run_git(root, "status", "--porcelain=v1", "--untracked-files=all").stdout
    if status:
        raise RuntimeError("dirty_build_context")
    return commit


def free_bytes(root: Path) -> int:
    """Preserve the supported activation preflight policy for B04.

    The wider B01 audit still owns proving/covering distinct Docker/build backing
    resources. B04 requires ordinary builds to use the same policy as supported
    activation rather than bypassing it.
    """

    return shutil.disk_usage(root).free


def prune_build_cache(root: Path, env: Mapping[str, str]) -> bool:
    try:
        subprocess.run(
            [
                "docker",
                "builder",
                "prune",
                "--force",
                f"--keep-storage={BUILD_CACHE_KEEP_BYTES}",
            ],
            cwd=root,
            env=dict(env),
            shell=False,
            check=True,
            timeout=300,
            stdout=sys.stderr,
            stderr=sys.stderr,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return True


def preflight_disk(root: Path, env: Mapping[str, str]) -> None:
    if free_bytes(root) >= UPDATE_REQUIRED_FREE_BYTES:
        return
    prune_build_cache(root, env)
    if free_bytes(root) >= UPDATE_REQUIRED_FREE_BYTES:
        return
    raise RuntimeError("insufficient_disk_for_update")


def build_core_images_locked(
    root: Path,
    env: Mapping[str, str],
    *,
    expected_commit: str | None = None,
) -> str:
    """Build core images while the caller holds ``host_mutation_lock``.

    The cache prune is attempted after every build attempt. A successful build
    is not accepted if its cache lifecycle or post-build source proof fails.
    """

    commit = resolve_clean_commit(root)
    if expected_commit is not None and commit != expected_commit.lower():
        raise RuntimeError("source_verification_failed")
    build_env = dict(env)
    build_env["FCP_BUILD_COMMIT"] = commit
    preflight_disk(root, build_env)

    build_error: BaseException | None = None
    try:
        subprocess.run(
            ["docker", "compose", "build", "relay", "flask", "recorder"],
            cwd=root,
            env=build_env,
            shell=False,
            check=True,
            timeout=900,
            stdout=sys.stderr,
            stderr=sys.stderr,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        build_error = exc

    prune_ok = prune_build_cache(root, build_env)
    if not prune_ok:
        if build_error is not None:
            raise RuntimeError("build_failed_and_cache_prune_failed") from build_error
        raise RuntimeError("build_cache_prune_failed")
    if build_error is not None:
        raise RuntimeError("core_image_build_failed") from build_error

    after = resolve_clean_commit(root)
    if after != commit:
        raise RuntimeError("build_context_changed")
    return commit


def build_core_images(
    root: Path,
    env: Mapping[str, str] | None = None,
    *,
    expected_commit: str | None = None,
    lock_timeout_seconds: float = HOST_MUTATION_LOCK_TIMEOUT_SECONDS,
) -> str:
    root = root.resolve()
    build_env = os.environ.copy() if env is None else dict(env)
    with host_mutation_lock(root, timeout_seconds=lock_timeout_seconds):
        return build_core_images_locked(
            root,
            build_env,
            expected_commit=expected_commit,
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--expected-commit")
    parser.add_argument(
        "--lock-timeout-seconds",
        type=float,
        default=HOST_MUTATION_LOCK_TIMEOUT_SECONDS,
    )
    args = parser.parse_args()
    try:
        commit = build_core_images(
            Path(args.repo_root),
            expected_commit=args.expected_commit,
            lock_timeout_seconds=args.lock_timeout_seconds,
        )
    except (RuntimeError, ValueError) as exc:
        print(f"FCP host build refused: {exc}", file=sys.stderr)
        return 1
    print(commit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
