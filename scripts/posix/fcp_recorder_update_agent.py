#!/usr/bin/env python3
"""Host-owned exact-commit update agent for recorder-only FCP installations."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

APPROVED_REPOSITORY = "Nettking/msh"
APPROVED_BRANCH = "main"
REQUEST_SCHEMA = "fcp.host-update-request.v1"
RESULT_SCHEMA = "fcp.host-update-result.v1"
MAX_BYTES = 8192
OID_RE = re.compile(r"^[0-9a-f]{40}$")
REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
PROJECT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$")


def utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def run(
    argv: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    check: bool = True,
    timeout: float = 180,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        argv,
        cwd=cwd,
        env=env,
        shell=False,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if check and completed.returncode:
        raise RuntimeError(f"command_failed:{argv[0]}:{completed.returncode}")
    return completed


def git(
    root: Path,
    env: dict[str, str],
    *args: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return run(["git", *args], cwd=root, env=env, check=check)


def approved_remote(value: str) -> bool:
    value = value.strip().removesuffix("/").removesuffix(".git")
    if value.startswith("git@github.com:"):
        return (
            value[len("git@github.com:") :].casefold()
            == APPROVED_REPOSITORY.casefold()
        )
    parsed = urlsplit(value)
    return (
        parsed.scheme in {"https", "ssh"}
        and parsed.hostname == "github.com"
        and parsed.username in {None, "git"}
        and parsed.password is None
        and not parsed.query
        and not parsed.fragment
        and parsed.path.strip("/").casefold()
        == APPROVED_REPOSITORY.casefold()
    )


def ancestor(root: Path, env: dict[str, str], older: str, newer: str) -> bool:
    return (
        git(
            root,
            env,
            "merge-base",
            "--is-ancestor",
            older,
            newer,
            check=False,
        ).returncode
        == 0
    )


def running_commit(root: Path, env: dict[str, str]) -> str | None:
    try:
        result = run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "recorder",
                "python",
                "-c",
                "import os; print(os.environ.get('FCP_BUILD_COMMIT',''))",
            ],
            cwd=root,
            env=env,
        )
        lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        if not lines:
            return None
        value = lines[-1].lower()
        return value if OID_RE.fullmatch(value) else None
    except (RuntimeError, subprocess.SubprocessError, IndexError):
        return None


def inspect_checkout(
    root: Path,
    env: dict[str, str],
    requested_target: str | None,
) -> dict[str, str | None]:
    top = Path(git(root, env, "rev-parse", "--show-toplevel").stdout.strip()).resolve()
    if top != root:
        raise RuntimeError("unsupported_checkout")
    if not approved_remote(git(root, env, "remote", "get-url", "origin").stdout):
        raise RuntimeError("unapproved_remote")
    branch = git(
        root,
        env,
        "symbolic-ref",
        "--quiet",
        "--short",
        "HEAD",
    ).stdout.strip()
    if branch != APPROVED_BRANCH:
        raise RuntimeError("detached_head")
    current = git(root, env, "rev-parse", "--verify", "HEAD^{commit}").stdout.strip().lower()
    if not OID_RE.fullmatch(current):
        raise RuntimeError("unsupported_checkout")
    status = git(
        root,
        env,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    ).stdout
    if status:
        return {
            "state": "dirty",
            "current": current,
            "target": requested_target,
            "code": "dirty",
            "message": "Local changes must be reviewed before updating.",
        }
    git(root, env, "fetch", "--no-tags", "origin", APPROVED_BRANCH)
    approved_tip = git(
        root,
        env,
        "rev-parse",
        "--verify",
        "FETCH_HEAD^{commit}",
    ).stdout.strip().lower()
    target = requested_target.lower() if requested_target else approved_tip
    if not OID_RE.fullmatch(target):
        raise RuntimeError("target_unavailable")
    if (
        git(root, env, "cat-file", "-e", f"{target}^{{commit}}", check=False).returncode
        or not ancestor(root, env, target, approved_tip)
    ):
        raise RuntimeError("target_unavailable")
    if current == target:
        state, code, message = "up_to_date", None, None
    elif ancestor(root, env, current, target):
        state, code, message = "update_available", None, None
    elif ancestor(root, env, target, current):
        state = "ahead"
        code = "ahead"
        message = "This checkout is ahead of approved main."
    else:
        state = "diverged"
        code = "diverged"
        message = "This checkout has diverged from approved main."
    return {
        "state": state,
        "current": current,
        "target": target,
        "code": code,
        "message": message,
    }


def atomic_json(path: Path, value: dict[str, object]) -> None:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    if len(encoded) > MAX_BYTES:
        raise ValueError("result_too_large")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as stream:
            fd = -1
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if fd >= 0:
            os.close(fd)
        temporary.unlink(missing_ok=True)


def request_result_path(result_file: Path, request_id: str) -> Path:
    digest = hashlib.sha256(request_id.encode("utf-8")).hexdigest()
    return result_file.with_name(f"result-{digest}.json")


def write_result(
    path: Path,
    *,
    request_id: str,
    action: str,
    state: str,
    current: str | None,
    target: str | None,
    running: str | None,
    code: str | None,
    message: str,
) -> None:
    value: dict[str, object] = {
        "schema": RESULT_SCHEMA,
        "request_id": request_id,
        "action": action,
        "state": state,
        "current_commit": current,
        "target_commit": target,
        "running_commit": running,
        "code": code,
        "message": message,
        "completed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    atomic_json(request_result_path(path, request_id), value)
    atomic_json(path, value)


def validate_request(value: object) -> tuple[str, str, str | None, datetime | None]:
    if not isinstance(value, dict) or value.get("schema") != REQUEST_SCHEMA:
        raise ValueError("malformed_message")
    request_id = value.get("request_id")
    action = value.get("action")
    target = value.get("target_commit")
    if not isinstance(request_id, str) or not REQUEST_ID_RE.fullmatch(request_id):
        raise ValueError("malformed_request_id")
    if action not in {"check", "apply"}:
        raise ValueError("malformed_action")
    if (
        value.get("repository") != APPROVED_REPOSITORY
        or value.get("branch") != APPROVED_BRANCH
    ):
        raise ValueError("unapproved_source")
    if target is not None and (
        not isinstance(target, str) or not OID_RE.fullmatch(target)
    ):
        raise ValueError("malformed_target")
    if action == "apply" and target is None:
        raise ValueError("malformed_target")
    created_raw = value.get("created_at")
    expires_raw = value.get("expires_at")
    created = utc(created_raw) if isinstance(created_raw, str) else None
    expires = utc(expires_raw) if isinstance(expires_raw, str) else None
    if created is None or expires is None:
        raise ValueError("malformed_timestamp")
    now = datetime.now(timezone.utc)
    if (
        created.timestamp() > now.timestamp() + 60
        or expires <= now
        or (expires - created).total_seconds() > 900
    ):
        raise ValueError("expired_or_invalid_request")
    activate_after: datetime | None = None
    if action == "apply":
        activate_raw = value.get("activate_after")
        if not isinstance(activate_raw, str):
            raise ValueError("malformed_activation_grace")
        activate_after = utc(activate_raw)
        if (
            activate_after < created
            or activate_after > expires
            or (activate_after - created).total_seconds() > 30
        ):
            raise ValueError("invalid_activation_grace")
    return request_id, action, target, activate_after


def wait_recorder_runtime(
    root: Path,
    data_directory: Path,
    env: dict[str, str],
    target: str,
    not_before: datetime,
) -> str:
    status_file = data_directory / "source_state" / "mtconnect_recorder_status.json"
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            running = running_commit(root, env)
            if running == target:
                services = set(
                    run(
                        ["docker", "compose", "ps", "--status", "running", "--services"],
                        cwd=root,
                        env=env,
                    ).stdout.splitlines()
                )
                if "recorder" in services and status_file.exists():
                    status = json.loads(status_file.read_text(encoding="utf-8"))
                    heartbeat_raw = status.get("heartbeat_at")
                    if isinstance(heartbeat_raw, str) and heartbeat_raw:
                        heartbeat = utc(heartbeat_raw)
                        if heartbeat.timestamp() >= not_before.timestamp() - 2:
                            return running
        except (
            RuntimeError,
            subprocess.SubprocessError,
            OSError,
            ValueError,
            json.JSONDecodeError,
        ):
            pass
        time.sleep(2)
    raise RuntimeError("recorder_runtime_verification_timeout")


def process_once(
    root: Path,
    data_directory: Path,
    request_file: Path,
    result_file: Path,
    env: dict[str, str],
) -> bool:
    processing = request_file.with_name(
        f"processing-{os.getpid()}-{time.time_ns()}.json"
    )
    try:
        os.replace(request_file, processing)
    except FileNotFoundError:
        return False
    request_id, action, target = "invalid-request", "unknown", None
    try:
        if processing.stat().st_size > MAX_BYTES:
            return True
        try:
            value = json.loads(processing.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return True

        request_id, action, target, activate_after = validate_request(value)
        if activate_after is not None:
            delay = (activate_after - datetime.now(timezone.utc)).total_seconds()
            if delay > 0:
                time.sleep(min(delay, 30.0))

        inspection = inspect_checkout(root, env, target)
        before = running_commit(root, env)
        if action == "check":
            write_result(
                result_file,
                request_id=request_id,
                action=action,
                state=str(inspection["state"]),
                current=inspection["current"],
                target=inspection["target"],
                running=before,
                code=inspection["code"],
                message=str(inspection["message"] or ""),
            )
            return True

        if inspection["state"] not in {"update_available", "up_to_date"}:
            write_result(
                result_file,
                request_id=request_id,
                action=action,
                state=str(inspection["state"]),
                current=inspection["current"],
                target=inspection["target"],
                running=before,
                code=inspection["code"],
                message=str(
                    inspection["message"]
                    or "The recorder checkout is not eligible for activation."
                ),
            )
            return True

        if inspection["state"] == "update_available":
            git(root, env, "merge", "--ff-only", target)
        proven = git(
            root,
            env,
            "rev-parse",
            "--verify",
            "HEAD^{commit}",
        ).stdout.strip().lower()
        if proven != target:
            raise RuntimeError("source_verification_failed")
        if git(
            root,
            env,
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
        ).stdout:
            raise RuntimeError("dirty_build_context")

        activation_env = env.copy()
        activation_env["FCP_BUILD_COMMIT"] = target
        run(
            ["docker", "compose", "build", "recorder"],
            cwd=root,
            env=activation_env,
            timeout=900,
        )
        activation_started = datetime.now(timezone.utc)
        run(
            [
                "docker",
                "compose",
                "up",
                "-d",
                "--no-deps",
                "--force-recreate",
                "recorder",
            ],
            cwd=root,
            env=activation_env,
            timeout=300,
        )
        running = wait_recorder_runtime(
            root,
            data_directory,
            activation_env,
            target,
            activation_started,
        )
        write_result(
            result_file,
            request_id=request_id,
            action=action,
            state="runtime_verified",
            current=target,
            target=target,
            running=running,
            code="updated",
            message=(
                "Recorder source, image, container, fresh heartbeat, and running "
                "commit were updated and verified."
            ),
        )
        return True
    except Exception as exc:  # noqa: BLE001 - emit only bounded safe result text
        current = None
        try:
            current = git(
                root,
                env,
                "rev-parse",
                "--verify",
                "HEAD^{commit}",
            ).stdout.strip().lower()
        except Exception:  # noqa: BLE001
            pass
        write_result(
            result_file,
            request_id=(
                request_id if REQUEST_ID_RE.fullmatch(request_id) else "invalid-request"
            ),
            action=action if action in {"check", "apply"} else "unknown",
            state="error",
            current=current,
            target=target,
            running=running_commit(root, env),
            code="host_update_failed",
            message=(
                "The recorder host update agent stopped safely before it could "
                "verify the requested runtime."
            ),
        )
        print(
            f"FCP recorder update request failed: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return True
    finally:
        processing.unlink(missing_ok=True)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--data-directory", required=True)
    parser.add_argument("--compose-project-name", default="")
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    root = Path(args.repo_root).resolve()
    data_directory = Path(args.data_directory).resolve()
    env = os.environ.copy()
    env["FCP_DATA_DIR"] = str(data_directory)
    project = str(args.compose_project_name or "").strip()
    if project:
        if not PROJECT_RE.fullmatch(project):
            raise SystemExit("invalid compose project name")
        env["COMPOSE_PROJECT_NAME"] = project

    directory = data_directory / "federation" / "update-agent"
    directory.mkdir(parents=True, exist_ok=True)
    script_path = Path(__file__).resolve()
    initial_digest = digest(script_path)
    lock_path = directory / "recorder-agent.lock"
    with lock_path.open("a+") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0
        while True:
            processed = process_once(
                root,
                data_directory,
                directory / "request.json",
                directory / "result.json",
                env,
            )
            if args.once:
                return 0
            if processed and digest(script_path) != initial_digest:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
                argv = [
                    sys.executable,
                    str(script_path),
                    "--repo-root",
                    str(root),
                    "--data-directory",
                    str(data_directory),
                    "--poll-seconds",
                    str(args.poll_seconds),
                ]
                if project:
                    argv.extend(["--compose-project-name", project])
                os.execve(sys.executable, argv, env)
            if not processed:
                time.sleep(max(0.1, min(args.poll_seconds, 30.0)))


if __name__ == "__main__":
    raise SystemExit(main())
