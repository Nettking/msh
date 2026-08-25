"""Host-owned, pressure-aware Ollama model installation.

Model downloads are intentionally treated as unbounded optional writes. FCP does
not guess a model size and reserve that guess. Instead a host-owned caller proves
which host resource backs Docker's persistent model storage, starts only while
that resource is NORMAL/WARNING, and remeasures it throughout the pull. Entering
PRESSURE stops the optional Ollama writer before the shared CRITICAL emergency
floor is intentionally spent.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from .host_resources import PressureLevel, ProcessResourceAdmission, ResourceAssessment

MODEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$")
MODEL_PULL_TIMEOUT_SECONDS = 3600.0
MODEL_PRESSURE_POLL_SECONDS = 0.25


@dataclass(frozen=True)
class ModelPullTarget:
    service: str
    profile: str
    installer: str


TARGETS = {
    "ollama": ModelPullTarget("ollama", "model-install", "ollama-pull"),
    "model-provider": ModelPullTarget(
        "model-provider", "provider", "model-provider-install"
    ),
}


@dataclass(frozen=True)
class ModelPullResult:
    ok: bool
    code: str
    message: str
    assessment: ResourceAssessment | None = None


def _subprocess_env(env: Mapping[str, str] | None) -> dict[str, str]:
    environment = dict(os.environ if env is None else env)
    # Supported launchers use the stable project name ``fcp``. Keeping the same
    # default here prevents headless/direct callers from probing or mutating a
    # second Compose project merely because the parent shell omitted the value.
    environment.setdefault("COMPOSE_PROJECT_NAME", "fcp")
    return environment


def _run(
    root: Path,
    args: list[str],
    *,
    env: Mapping[str, str] | None = None,
    timeout: float = 120.0,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=root,
        env=_subprocess_env(env),
        shell=False,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _docker_backing_resource_path(
    root: Path,
    *,
    env: Mapping[str, str] | None = None,
) -> Path | None:
    """Return a host path whose filesystem actually backs Docker model writes.

    Native POSIX Docker exposes its data root directly. Docker Desktop persists
    Linux-container storage in a host disk image; for known supported default
    locations we measure the host directory containing that image. If the
    backing resource cannot be proven, a *new* model pull fails closed.
    """

    environment = _subprocess_env(env)
    if os.name == "nt":
        local_app_data = str(environment.get("LOCALAPPDATA") or "").strip()
        candidates: list[Path] = []
        if local_app_data:
            docker = Path(local_app_data) / "Docker" / "wsl"
            candidates.extend(
                [
                    docker / "disk" / "docker_data.vhdx",
                    docker / "data" / "ext4.vhdx",
                ]
            )
        program_data = str(environment.get("PROGRAMDATA") or "").strip()
        if program_data:
            native_root = Path(program_data) / "docker"
            try:
                if native_root.is_dir():
                    return native_root.resolve()
            except OSError:
                pass
        for candidate in candidates:
            try:
                if candidate.is_file():
                    return candidate.parent.resolve()
            except OSError:
                continue
        return None

    if sys.platform == "darwin":
        home = str(environment.get("HOME") or "").strip()
        if home:
            docker_data = (
                Path(home)
                / "Library"
                / "Containers"
                / "com.docker.docker"
                / "Data"
                / "vms"
                / "0"
                / "data"
            )
            for name in ("Docker.raw", "Docker.qcow2"):
                candidate = docker_data / name
                try:
                    if candidate.is_file():
                        return candidate.parent.resolve()
                except OSError:
                    continue
        return None

    info = _run(
        root,
        ["docker", "info", "--format", "{{.DockerRootDir}}"],
        env=environment,
        timeout=30.0,
    )
    if info.returncode != 0:
        return None
    raw = info.stdout.strip()
    if not raw:
        return None
    candidate = Path(raw)
    if not candidate.is_absolute():
        return None
    try:
        if not candidate.exists():
            return None
        return candidate.resolve()
    except OSError:
        return None


def _model_ready(
    root: Path,
    target: ModelPullTarget,
    model: str,
    *,
    env: Mapping[str, str] | None = None,
) -> bool:
    result = _run(
        root,
        [
            "docker",
            "compose",
            "exec",
            "-T",
            target.service,
            "ollama",
            "show",
            model,
        ],
        env=env,
        timeout=60.0,
    )
    return result.returncode == 0


def _writer_stopped(
    root: Path,
    target: ModelPullTarget,
    container_name: str,
    *,
    env: Mapping[str, str] | None = None,
) -> bool:
    service = _run(
        root,
        ["docker", "compose", "ps", "--status", "running", "-q", target.service],
        env=env,
        timeout=30.0,
    )
    pull = _run(
        root,
        ["docker", "ps", "-q", "--filter", f"name=^/{container_name}$"],
        env=env,
        timeout=30.0,
    )
    if service.returncode != 0 or pull.returncode != 0:
        return False
    return not service.stdout.strip() and not pull.stdout.strip()


def _stop_model_writer(
    root: Path,
    target: ModelPullTarget,
    container_name: str,
    *,
    env: Mapping[str, str] | None = None,
) -> bool:
    """Stop the optional writer and positively verify that it is no longer live."""

    _run(
        root,
        ["docker", "compose", "stop", "--timeout", "5", target.service],
        env=env,
        timeout=30.0,
    )
    _run(
        root,
        ["docker", "rm", "-f", container_name],
        env=env,
        timeout=30.0,
    )
    if _writer_stopped(root, target, container_name, env=env):
        return True

    # Escalate once. A failed Docker command must never be translated into a
    # false claim that the unknown-size writer stopped.
    _run(
        root,
        ["docker", "compose", "kill", target.service],
        env=env,
        timeout=30.0,
    )
    _run(
        root,
        ["docker", "rm", "-f", container_name],
        env=env,
        timeout=30.0,
    )
    return _writer_stopped(root, target, container_name, env=env)


def _settle_pull_client(process: subprocess.Popen[object]) -> None:
    try:
        process.wait(timeout=30.0)
    except subprocess.TimeoutExpired:
        process.terminate()
        try:
            process.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            process.kill()


def admitted_model_pull(
    root: Path | str,
    *,
    model: str,
    target_name: str = "ollama",
    controller: ProcessResourceAdmission | None = None,
    env: Mapping[str, str] | None = None,
    timeout_seconds: float = MODEL_PULL_TIMEOUT_SECONDS,
    poll_seconds: float = MODEL_PRESSURE_POLL_SECONDS,
) -> ModelPullResult:
    """Install one local model without consuming the shared emergency reserve.

    Existing models are accepted without a new-write admission because no model
    download is needed. A new pull starts only after FCP can identify and admit
    the actual host resource backing Docker's persistent model storage.
    """

    resolved_root = Path(root).resolve()
    selected_model = str(model or "").strip()
    if not MODEL_RE.fullmatch(selected_model):
        return ModelPullResult(False, "invalid_model_identifier", "Invalid model identifier.")
    target = TARGETS.get(target_name)
    if target is None:
        return ModelPullResult(False, "invalid_model_target", "Invalid model installation target.")
    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise ValueError("model pull timing bounds must be positive")

    if _model_ready(resolved_root, target, selected_model, env=env):
        return ModelPullResult(
            True,
            "already_present",
            f"Ollama model is already installed: {selected_model}",
        )

    backing_path = _docker_backing_resource_path(resolved_root, env=env)
    if backing_path is None:
        return ModelPullResult(
            False,
            "resource_unproven",
            "Model installation was not started because FCP could not prove the host resource backing Docker model storage.",
        )

    admission = controller or ProcessResourceAdmission()
    assessment = admission.assessment(backing_path)
    if assessment.level >= PressureLevel.PRESSURE:
        return ModelPullResult(
            False,
            "resource_pressure",
            "Model installation was not started because the Docker backing resource is under pressure.",
            assessment,
        )

    container_name = f"fcp-model-pull-{uuid.uuid4().hex[:20]}"
    command = [
        "docker",
        "compose",
        "--profile",
        target.profile,
        "run",
        "--rm",
        "--name",
        container_name,
        "--entrypoint",
        "/bin/ollama",
        target.installer,
        "pull",
        selected_model,
    ]
    try:
        process = subprocess.Popen(
            command,
            cwd=resolved_root,
            env=_subprocess_env(env),
            shell=False,
        )
    except OSError as exc:
        return ModelPullResult(False, "model_install_failed", f"Could not start model installation: {exc}")

    deadline = time.monotonic() + timeout_seconds
    while True:
        returncode = process.poll()
        if returncode is not None:
            break
        current = admission.assessment(backing_path)
        if current.level >= PressureLevel.PRESSURE:
            stopped = _stop_model_writer(
                resolved_root,
                target,
                container_name,
                env=env,
            )
            _settle_pull_client(process)
            if not stopped:
                return ModelPullResult(
                    False,
                    "writer_stop_unverified",
                    "Docker reached host resource pressure and FCP could not prove that the optional model writer stopped.",
                    current,
                )
            return ModelPullResult(
                False,
                "resource_pressure",
                "Model installation stopped because the Docker backing resource reached pressure; core FCP can continue without AI.",
                current,
            )
        if time.monotonic() >= deadline:
            stopped = _stop_model_writer(
                resolved_root,
                target,
                container_name,
                env=env,
            )
            _settle_pull_client(process)
            if not stopped:
                return ModelPullResult(
                    False,
                    "writer_stop_unverified",
                    "The model pull deadline expired and FCP could not prove that the optional model writer stopped.",
                )
            return ModelPullResult(
                False,
                "model_install_timeout",
                "Model installation exceeded its bounded host-side deadline.",
            )
        time.sleep(poll_seconds)

    if returncode != 0:
        return ModelPullResult(
            False,
            "model_install_failed",
            f"Model installation exited with code {returncode}.",
        )
    if not _model_ready(resolved_root, target, selected_model, env=env):
        return ModelPullResult(
            False,
            "model_verification_failed",
            "Model installation finished but the selected model could not be verified.",
        )
    return ModelPullResult(
        True,
        "installed",
        f"Ollama model is installed and verified: {selected_model}",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Pressure-aware host Ollama model installation")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--model", required=True)
    parser.add_argument("--target", choices=sorted(TARGETS), default="ollama")
    args = parser.parse_args()
    result = admitted_model_pull(
        args.repo_root,
        model=args.model,
        target_name=args.target,
    )
    print(result.message)
    if result.ok:
        return 0
    if result.code == "resource_pressure":
        return 2
    if result.code in {"resource_unproven", "writer_stop_unverified"}:
        return 3
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
