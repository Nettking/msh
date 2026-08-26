"""Host-owned build lifecycle shared by supported POSIX start/update paths.

This module owns exact-source proof, Docker backing-resource admission, the
pressure-aware BuildKit lifecycle, and cache cleanup. A normal standalone
invocation acquires the checkout mutation lock itself. Supported launchers or
update runners may call it while their parent lease already owns that same
boundary across the wider activation transaction.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import signal
import subprocess
import sys
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path

from .docker_resources import docker_backing_resource_path
from .host_resources import PressureLevel, ProcessResourceAdmission, ResourceAssessment

try:  # pragma: no cover - exercised only on supported POSIX hosts
    import fcntl
except ImportError:  # pragma: no cover - Windows uses the PowerShell primitive
    fcntl = None  # type: ignore[assignment]

OID_RE = re.compile(r"^[0-9a-f]{40}$")
# Compatibility alias for the long-standing update minimum. Build admission now
# uses the shared B01 PRESSURE boundary, whose CRITICAL floor remains 10 GiB.
UPDATE_REQUIRED_FREE_BYTES = 10 * 1024**3
BUILD_CACHE_KEEP_BYTES = 8 * 1024**3
HOST_MUTATION_LOCK_TIMEOUT_SECONDS = 30.0
HOST_MUTATION_LEASE_ENV = "FCP_HOST_MUTATION_LEASE_ACTIVE"
BUILD_TIMEOUT_SECONDS = 900.0
BUILD_PRESSURE_POLL_SECONDS = 0.25
BUILD_CLIENT_SETTLE_SECONDS = 10.0
BUILDER_STOP_TIMEOUT_SECONDS = 30.0
BUILDER_PREFIX = "fcp-build-"
CORE_BUILD_SERVICES = ("relay", "flask", "recorder")


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


def docker_resource_assessment(
    root: Path,
    env: Mapping[str, str],
    *,
    controller: ProcessResourceAdmission | None = None,
) -> tuple[Path, ResourceAssessment]:
    """Assess the host filesystem that actually backs Docker build writes."""

    backing_path = docker_backing_resource_path(root, env=env)
    if backing_path is None:
        raise RuntimeError("docker_backing_resource_unproven")
    admission = controller or ProcessResourceAdmission()
    return backing_path, admission.assessment(backing_path)


def builder_name(root: Path) -> str:
    """Return a deterministic builder identity scoped to this checkout."""

    digest = hashlib.sha256(str(root.resolve()).encode("utf-8")).hexdigest()[:20]
    return f"{BUILDER_PREFIX}{digest}"


def _docker_run(
    root: Path,
    args: list[str],
    *,
    env: Mapping[str, str],
    timeout: float = 120.0,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=root,
        env=dict(env),
        shell=False,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _builder_inspection(
    root: Path,
    name: str,
    env: Mapping[str, str],
) -> subprocess.CompletedProcess[str]:
    return _docker_run(
        root,
        ["docker", "buildx", "inspect", name],
        env=env,
        timeout=30.0,
    )


def _builder_absent(root: Path, name: str, env: Mapping[str, str]) -> bool:
    """Prove the named builder is absent using a successful Buildx enumeration."""

    try:
        listed = _docker_run(
            root,
            ["docker", "buildx", "ls", "--format", "{{.Name}}"],
            env=env,
            timeout=30.0,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    if listed.returncode != 0:
        return False
    names = {line.strip() for line in listed.stdout.splitlines() if line.strip()}
    return name not in names


def ensure_controllable_builder(root: Path, env: Mapping[str, str]) -> str:
    """Ensure the checkout has one independently stoppable BuildKit daemon.

    FCP intentionally requires the docker-container driver. The default Docker
    builder embeds BuildKit inside the Docker daemon, so stopping the CLI cannot
    prove that build writes have ceased. New builders request ``default-load`` so
    Compose retains its existing local-image behavior.
    """

    name = builder_name(root)
    try:
        inspected = _builder_inspection(root, name, env)
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError("controllable_builder_unavailable") from exc
    if inspected.returncode == 0:
        driver = next(
            (
                line.partition(":")[2].strip()
                for line in inspected.stdout.splitlines()
                if line.strip().startswith("Driver:")
            ),
            "",
        )
        if driver != "docker-container":
            raise RuntimeError("controllable_builder_conflict")
        if not _builder_stopped(root, name, env) and not stop_build_writer(root, name, env):
            raise RuntimeError("build_writer_stop_unverified")
        return name

    try:
        created = _docker_run(
            root,
            [
                "docker",
                "buildx",
                "create",
                "--name",
                name,
                "--driver",
                "docker-container",
                "--driver-opt",
                "default-load=true",
            ],
            env=env,
            timeout=60.0,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError("controllable_builder_unavailable") from exc
    if created.returncode != 0:
        raise RuntimeError("controllable_builder_unavailable")
    try:
        inspected = _builder_inspection(root, name, env)
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError("controllable_builder_unavailable") from exc
    if inspected.returncode != 0 or "Driver:" not in inspected.stdout:
        raise RuntimeError("controllable_builder_unavailable")
    driver = next(
        (
            line.partition(":")[2].strip()
            for line in inspected.stdout.splitlines()
            if line.strip().startswith("Driver:")
        ),
        "",
    )
    if driver != "docker-container":
        raise RuntimeError("controllable_builder_conflict")
    return name


def _builder_stopped(root: Path, name: str, env: Mapping[str, str]) -> bool:
    try:
        inspected = _builder_inspection(root, name, env)
    except (OSError, subprocess.SubprocessError):
        return False
    if inspected.returncode != 0:
        return _builder_absent(root, name, env)
    statuses = [
        line.partition(":")[2].strip().casefold()
        for line in inspected.stdout.splitlines()
        if line.strip().startswith("Status:")
    ]
    return bool(statuses) and all(value not in {"running", "starting"} for value in statuses)


def _remove_builder(root: Path, name: str, env: Mapping[str, str]) -> bool:
    try:
        inspected = _builder_inspection(root, name, env)
    except (OSError, subprocess.SubprocessError):
        return False
    if inspected.returncode != 0:
        return _builder_absent(root, name, env)
    try:
        removed = _docker_run(
            root,
            ["docker", "buildx", "rm", "--force", "--timeout", "20s", name],
            env=env,
            timeout=30.0,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return removed.returncode == 0 and _builder_absent(root, name, env)


def stop_build_writer(
    root: Path,
    name: str,
    env: Mapping[str, str],
    *,
    discard_cache: bool = False,
) -> bool:
    """Stop the FCP BuildKit daemon and positively verify quiescence."""

    try:
        stopped = _docker_run(
            root,
            ["docker", "buildx", "stop", name],
            env=env,
            timeout=BUILDER_STOP_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError):
        stopped = None
    if stopped is not None and stopped.returncode == 0 and _builder_stopped(root, name, env):
        if not discard_cache:
            return True
        # Under pressure, deleting FCP's own builder cache is a bounded cleanup.
        # Failure to delete cache does not make a positively stopped writer live.
        _remove_builder(root, name, env)
        return True
    return _remove_builder(root, name, env)


def prune_build_cache(
    root: Path,
    env: Mapping[str, str],
    *,
    name: str | None = None,
) -> bool:
    """Bound only FCP-owned build cache, never unrelated host builder state."""

    selected = name or builder_name(root)
    try:
        inspected = _builder_inspection(root, selected, env)
    except (OSError, subprocess.SubprocessError):
        return False
    if inspected.returncode != 0:
        return _builder_absent(root, selected, env)
    try:
        completed = subprocess.run(
            [
                "docker",
                "buildx",
                "prune",
                "--builder",
                selected,
                "--force",
                f"--keep-storage={BUILD_CACHE_KEEP_BYTES}",
            ],
            cwd=root,
            env=dict(env),
            shell=False,
            check=False,
            timeout=300,
            stdout=sys.stderr,
            stderr=sys.stderr,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return completed.returncode == 0


def preflight_disk(root: Path, env: Mapping[str, str]) -> None:
    """Admit a core build against Docker's proven host backing resource.

    Every start first proves the checkout-scoped FCP builder is quiescent, so an
    abandoned BuildKit daemon cannot outlive the host-mutation owner and overlap a
    later build. NORMAL/WARNING preserves its cache. PRESSURE/CRITICAL additionally
    discards reconstructible FCP builder cache before remeasuring the same backing
    resource. An unproven or still-pressured resource fails closed before a new
    BuildKit writer starts.
    """

    admission = ProcessResourceAdmission()
    backing_path, before = docker_resource_assessment(root, env, controller=admission)
    name = builder_name(root)
    if before.level < PressureLevel.PRESSURE:
        if not stop_build_writer(root, name, env):
            raise RuntimeError("build_writer_stop_unverified")
        return
    if not stop_build_writer(root, name, env, discard_cache=True):
        raise RuntimeError("build_writer_stop_unverified")
    after = admission.assessment(backing_path)
    if after.level < PressureLevel.PRESSURE:
        return
    raise RuntimeError("insufficient_disk_for_update")


def _settle_build_client(process: subprocess.Popen[object]) -> bool:
    """Stop the isolated Compose/Buildx client process group and prove it exited."""

    if process.poll() is not None:
        return True
    killpg = getattr(os, "killpg", None)
    pid = getattr(process, "pid", None)
    if callable(killpg) and isinstance(pid, int):
        try:
            killpg(pid, signal.SIGTERM)
        except ProcessLookupError:
            return process.poll() is not None
        except OSError:
            try:
                process.terminate()
            except OSError:
                pass
    else:
        try:
            process.terminate()
        except OSError:
            pass
    try:
        process.wait(timeout=BUILD_CLIENT_SETTLE_SECONDS)
    except subprocess.TimeoutExpired:
        if callable(killpg) and isinstance(pid, int):
            try:
                killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except OSError:
                try:
                    process.kill()
                except OSError:
                    pass
        else:
            try:
                process.kill()
            except OSError:
                pass
        try:
            process.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            return process.poll() is not None
    return process.poll() is not None


def _verify_core_image_commits(
    root: Path,
    env: Mapping[str, str],
    expected_commit: str,
) -> None:
    """Require every local Compose core image to carry the exact source identity."""

    for service in CORE_BUILD_SERVICES:
        image = _docker_run(
            root,
            ["docker", "compose", "images", "-q", service],
            env=env,
            timeout=30.0,
        )
        image_id = image.stdout.strip().splitlines()[-1].strip() if image.returncode == 0 and image.stdout.strip() else ""
        if not image_id:
            raise RuntimeError("built_image_identity_unavailable")
        label = _docker_run(
            root,
            [
                "docker",
                "image",
                "inspect",
                "--format",
                '{{ index .Config.Labels "no.fcp.build_commit" }}',
                image_id,
            ],
            env=env,
            timeout=30.0,
        )
        if label.returncode != 0 or label.stdout.strip().lower() != expected_commit:
            raise RuntimeError("built_image_identity_mismatch")


def controlled_core_build(
    root: Path,
    env: Mapping[str, str],
    *,
    controller: ProcessResourceAdmission | None = None,
    timeout_seconds: float = BUILD_TIMEOUT_SECONDS,
    poll_seconds: float = BUILD_PRESSURE_POLL_SECONDS,
) -> None:
    """Run one unknown-size build and stop its writer before CRITICAL reserve use."""

    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise ValueError("build timing bounds must be positive")
    admission = controller or ProcessResourceAdmission()
    backing_path, initial = docker_resource_assessment(root, env, controller=admission)
    if initial.level >= PressureLevel.PRESSURE:
        raise RuntimeError("insufficient_disk_for_update")
    name = ensure_controllable_builder(root, env)
    command = [
        "docker",
        "compose",
        "build",
        "--builder",
        name,
        *CORE_BUILD_SERVICES,
    ]
    try:
        process = subprocess.Popen(
            command,
            cwd=root,
            env=dict(env),
            shell=False,
            stdout=sys.stderr,
            stderr=sys.stderr,
            start_new_session=True,
        )
    except OSError as exc:
        raise RuntimeError("core_image_build_failed") from exc

    deadline = time.monotonic() + timeout_seconds
    while True:
        returncode = process.poll()
        if returncode is not None:
            break
        current = admission.assessment(backing_path)
        if current.level >= PressureLevel.PRESSURE:
            returncode = process.poll()
            if returncode is not None:
                break
            client_stopped = _settle_build_client(process)
            writer_stopped = stop_build_writer(root, name, env, discard_cache=True)
            if not client_stopped or not writer_stopped:
                raise RuntimeError("build_writer_stop_unverified")
            raise RuntimeError("build_resource_pressure")
        if time.monotonic() >= deadline:
            returncode = process.poll()
            if returncode is not None:
                break
            client_stopped = _settle_build_client(process)
            writer_stopped = stop_build_writer(root, name, env, discard_cache=True)
            if not client_stopped or not writer_stopped:
                raise RuntimeError("build_writer_stop_unverified")
            raise RuntimeError("core_image_build_timeout")
        time.sleep(poll_seconds)

    if returncode != 0:
        prune_ok = prune_build_cache(root, env, name=name)
        writer_stopped = stop_build_writer(root, name, env)
        if not writer_stopped:
            raise RuntimeError("build_writer_stop_unverified")
        if not prune_ok:
            raise RuntimeError("build_failed_and_cache_prune_failed")
        raise RuntimeError("core_image_build_failed")
    if not prune_build_cache(root, env, name=name):
        if not stop_build_writer(root, name, env):
            raise RuntimeError("build_writer_stop_unverified")
        raise RuntimeError("build_cache_prune_failed")
    if not stop_build_writer(root, name, env):
        raise RuntimeError("build_writer_stop_unverified")


def build_core_images_locked(
    root: Path,
    env: Mapping[str, str],
    *,
    expected_commit: str | None = None,
) -> str:
    """Build core images while the caller holds ``host_mutation_lock``."""

    commit = resolve_clean_commit(root)
    if expected_commit is not None and commit != expected_commit.lower():
        raise RuntimeError("source_verification_failed")
    build_env = dict(env)
    build_env.setdefault("COMPOSE_PROJECT_NAME", "fcp")
    build_env["FCP_BUILD_COMMIT"] = commit
    preflight_disk(root, build_env)
    controlled_core_build(root, build_env)
    _verify_core_image_commits(root, build_env, commit)

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
    lease_already_held: bool = False,
) -> str:
    root = root.resolve()
    build_env = os.environ.copy() if env is None else dict(env)
    if lease_already_held:
        if build_env.get(HOST_MUTATION_LEASE_ENV) != "1":
            raise RuntimeError("host_mutation_lease_missing")
        return build_core_images_locked(
            root,
            build_env,
            expected_commit=expected_commit,
        )
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
    parser.add_argument("--lease-already-held", action="store_true")
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
            lease_already_held=args.lease_already_held,
        )
    except (RuntimeError, ValueError) as exc:
        print(f"FCP host build refused: {exc}", file=sys.stderr)
        return 1
    print(commit)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI wrapper
    raise SystemExit(main())