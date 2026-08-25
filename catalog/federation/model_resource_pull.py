"""Host-owned, pressure-aware Ollama model installation.

Model downloads are intentionally treated as unbounded optional writes. FCP does
not guess a model size and reserve that guess. Instead a host-owned caller starts
only while the Docker backing resource is NORMAL/WARNING and remeasures while the
pull is active. Entering PRESSURE stops the Ollama writer before the shared
CRITICAL emergency floor is intentionally spent.

The supported layout stores Docker's growing data image/volumes on the same host
resource as the checkout path supplied here. Windows launchers implement the same
threshold policy in PowerShell because normal Windows startup does not require a
host Python installation.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import time
import uuid
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


def _run(
    root: Path,
    args: list[str],
    *,
    timeout: float = 120.0,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=root,
        shell=False,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _model_ready(root: Path, target: ModelPullTarget, model: str) -> bool:
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
        timeout=60.0,
    )
    return result.returncode == 0


def _stop_model_writer(root: Path, target: ModelPullTarget, container_name: str) -> None:
    # Stopping the server is deliberate: disconnecting only the pull client does
    # not prove the Ollama daemon stopped writing model blobs. The model is an
    # optional capability, so resource safety wins over keeping that capability
    # alive while the host is under pressure.
    _run(
        root,
        ["docker", "compose", "stop", "--timeout", "5", target.service],
        timeout=30.0,
    )
    _run(root, ["docker", "rm", "-f", container_name], timeout=30.0)


def admitted_model_pull(
    root: Path | str,
    *,
    model: str,
    target_name: str = "ollama",
    controller: ProcessResourceAdmission | None = None,
    timeout_seconds: float = MODEL_PULL_TIMEOUT_SECONDS,
    poll_seconds: float = MODEL_PRESSURE_POLL_SECONDS,
) -> ModelPullResult:
    """Install one local model without consuming the shared emergency reserve.

    Existing models are accepted without a new-write admission because no model
    download is needed. New pulls are refused at PRESSURE/CRITICAL and monitored
    for their whole lifetime because their final size is not assumed in advance.
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

    if _model_ready(resolved_root, target, selected_model):
        return ModelPullResult(
            True,
            "already_present",
            f"Ollama model is already installed: {selected_model}",
        )

    admission = controller or ProcessResourceAdmission()
    assessment = admission.assessment(resolved_root)
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
        process = subprocess.Popen(command, cwd=resolved_root, shell=False)
    except OSError as exc:
        return ModelPullResult(False, "model_install_failed", f"Could not start model installation: {exc}")

    deadline = time.monotonic() + timeout_seconds
    while True:
        returncode = process.poll()
        if returncode is not None:
            break
        current = admission.assessment(resolved_root)
        if current.level >= PressureLevel.PRESSURE:
            _stop_model_writer(resolved_root, target, container_name)
            try:
                process.wait(timeout=30.0)
            except subprocess.TimeoutExpired:
                process.terminate()
            return ModelPullResult(
                False,
                "resource_pressure",
                "Model installation stopped because the Docker backing resource reached pressure; core FCP can continue without AI.",
                current,
            )
        if time.monotonic() >= deadline:
            _stop_model_writer(resolved_root, target, container_name)
            try:
                process.wait(timeout=30.0)
            except subprocess.TimeoutExpired:
                process.terminate()
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
    if not _model_ready(resolved_root, target, selected_model):
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
    return 2 if result.code == "resource_pressure" else 1


if __name__ == "__main__":
    raise SystemExit(main())
