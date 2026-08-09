#!/usr/bin/env python3
"""Host-owned exact-commit MSH update agent for Linux/POSIX launchers."""

from __future__ import annotations

import argparse
import fcntl
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
REQUEST_SCHEMA = "msh.host-update-request.v1"
RESULT_SCHEMA = "msh.host-update-result.v1"
MAX_BYTES = 8192
OID_RE = re.compile(r"^[0-9a-f]{40}$")
REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


def utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def run(argv: list[str], *, cwd: Path, check: bool = True) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        argv,
        cwd=cwd,
        shell=False,
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    if check and completed.returncode:
        raise RuntimeError(f"command_failed:{argv[0]}:{completed.returncode}")
    return completed


def git(root: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return run(["git", *args], cwd=root, check=check)


def approved_remote(value: str) -> bool:
    value = value.strip().removesuffix("/").removesuffix(".git")
    if value.startswith("git@github.com:"):
        return value[len("git@github.com:") :].casefold() == APPROVED_REPOSITORY.casefold()
    parsed = urlsplit(value)
    return (
        parsed.scheme in {"https", "ssh"}
        and parsed.hostname == "github.com"
        and parsed.username in {None, "git"}
        and parsed.password is None
        and not parsed.query
        and not parsed.fragment
        and parsed.path.strip("/").casefold() == APPROVED_REPOSITORY.casefold()
    )


def ancestor(root: Path, older: str, newer: str) -> bool:
    return git(root, "merge-base", "--is-ancestor", older, newer, check=False).returncode == 0


def running_commit(root: Path) -> str | None:
    try:
        result = run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "flask",
                "python",
                "-c",
                "import os; print(os.environ.get('MSH_BUILD_COMMIT',''))",
            ],
            cwd=root,
        )
        value = result.stdout.strip().splitlines()[-1].strip().lower()
        return value if OID_RE.fullmatch(value) else None
    except (RuntimeError, subprocess.SubprocessError, IndexError):
        return None


def inspect_checkout(root: Path, requested_target: str | None) -> dict[str, str | None]:
    top = Path(git(root, "rev-parse", "--show-toplevel").stdout.strip()).resolve()
    if top != root:
        raise RuntimeError("unsupported_checkout")
    if not approved_remote(git(root, "remote", "get-url", "origin").stdout):
        raise RuntimeError("unapproved_remote")
    branch = git(root, "symbolic-ref", "--quiet", "--short", "HEAD").stdout.strip()
    if branch != APPROVED_BRANCH:
        raise RuntimeError("detached_head")
    current = git(root, "rev-parse", "--verify", "HEAD^{commit}").stdout.strip().lower()
    if not OID_RE.fullmatch(current):
        raise RuntimeError("unsupported_checkout")
    status = git(root, "status", "--porcelain=v1", "--untracked-files=all").stdout
    if status:
        return {
            "state": "dirty",
            "current": current,
            "target": requested_target,
            "code": "dirty",
            "message": "Local changes must be reviewed before updating.",
        }
    git(root, "fetch", "--no-tags", "origin", APPROVED_BRANCH)
    approved_tip = git(root, "rev-parse", "--verify", "FETCH_HEAD^{commit}").stdout.strip().lower()
    target = requested_target.lower() if requested_target else approved_tip
    if not OID_RE.fullmatch(target):
        raise RuntimeError("target_unavailable")
    if git(root, "cat-file", "-e", f"{target}^{{commit}}", check=False).returncode or not ancestor(root, target, approved_tip):
        raise RuntimeError("target_unavailable")
    if current == target:
        state, code, message = "up_to_date", None, None
    elif ancestor(root, current, target):
        state, code, message = "update_available", None, None
    elif ancestor(root, target, current):
        state, code, message = "ahead", "ahead", "This checkout is ahead of approved main."
    else:
        state, code, message = "diverged", "diverged", "This checkout has diverged from approved main."
    return {"state": state, "current": current, "target": target, "code": code, "message": message}


def atomic_json(path: Path, value: dict[str, object]) -> None:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
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
    atomic_json(
        path,
        {
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
        },
    )


def wait_runtime(root: Path, target: str) -> str:
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            value = running_commit(root)
            if value == target:
                run(
                    [
                        "docker",
                        "compose",
                        "exec",
                        "-T",
                        "flask",
                        "python",
                        "-c",
                        "import urllib.request; r=urllib.request.urlopen('http://127.0.0.1:5000/federation',timeout=3); assert 200 <= r.status < 500",
                    ],
                    cwd=root,
                )
                services = set(
                    run(
                        ["docker", "compose", "ps", "--status", "running", "--services"],
                        cwd=root,
                    ).stdout.splitlines()
                )
                if {"relay", "recorder", "flask"}.issubset(services):
                    return value
        except (RuntimeError, subprocess.SubprocessError):
            pass
        time.sleep(2)
    raise RuntimeError("runtime_verification_timeout")


def validate_request(
    value: object,
) -> tuple[str, str, str | None, datetime | None]:
    if not isinstance(value, dict) or value.get("schema") != REQUEST_SCHEMA:
        raise ValueError("malformed_message")
    request_id = value.get("request_id")
    action = value.get("action")
    target = value.get("target_commit")
    if not isinstance(request_id, str) or not REQUEST_ID_RE.fullmatch(request_id):
        raise ValueError("malformed_request_id")
    if action not in {"check", "apply"}:
        raise ValueError("malformed_action")
    if value.get("repository") != APPROVED_REPOSITORY or value.get("branch") != APPROVED_BRANCH:
        raise ValueError("unapproved_source")
    if target is not None and (not isinstance(target, str) or not OID_RE.fullmatch(target)):
        raise ValueError("malformed_target")
    if action == "apply" and target is None:
        raise ValueError("malformed_target")
    created = utc(value.get("created_at")) if isinstance(value.get("created_at"), str) else None
    expires = utc(value.get("expires_at")) if isinstance(value.get("expires_at"), str) else None
    if created is None or expires is None:
        raise ValueError("malformed_timestamp")
    now = datetime.now(timezone.utc)
    if created.timestamp() > now.timestamp() + 60 or expires <= now or (expires - created).total_seconds() > 900:
        raise ValueError("expired_or_invalid_request")
    activate_after: datetime | None = None
    if action == "apply":
        raw_activate_after = value.get("activate_after")
        if not isinstance(raw_activate_after, str):
            raise ValueError("malformed_activation_grace")
        activate_after = utc(raw_activate_after)
        if (
            activate_after < created
            or activate_after > expires
            or (activate_after - created).total_seconds() > 30
        ):
            raise ValueError("invalid_activation_grace")
    return request_id, action, target, activate_after


def process_once(root: Path, request_file: Path, result_file: Path) -> bool:
    if not request_file.exists():
        return False
    if request_file.stat().st_size > MAX_BYTES:
        request_file.unlink(missing_ok=True)
        return True
    try:
        value = json.loads(request_file.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        request_file.unlink(missing_ok=True)
        return True
    processing = request_file.with_name(f"processing-{os.getpid()}-{time.time_ns()}.json")
    os.replace(request_file, processing)
    request_id, action, target = "invalid-request", "unknown", None
    try:
        request_id, action, target, activate_after = validate_request(value)
        if activate_after is not None:
            delay = (activate_after - datetime.now(timezone.utc)).total_seconds()
            if delay > 0:
                time.sleep(min(delay, 30.0))
        inspection = inspect_checkout(root, target)
        before = running_commit(root)
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
                message=str(inspection["message"] or "The checkout is not eligible for activation."),
            )
            return True
        if inspection["state"] == "update_available":
            git(root, "merge", "--ff-only", target)
        proven = git(root, "rev-parse", "--verify", "HEAD^{commit}").stdout.strip().lower()
        if proven != target:
            raise RuntimeError("source_verification_failed")
        env = os.environ.copy()
        env["MSH_BUILD_COMMIT"] = target
        for argv in (
            ["docker", "compose", "build", "relay", "flask", "recorder"],
            ["docker", "compose", "up", "-d", "relay", "ollama", "recorder"],
            ["docker", "compose", "stop", "flask"],
        ):
            subprocess.run(argv, cwd=root, env=env, shell=False, check=True, timeout=900)
        resume = subprocess.run(
            [
                "docker",
                "compose",
                "run",
                "--rm",
                "--no-deps",
                "--entrypoint",
                "python",
                "flask",
                "-m",
                "catalog.flask_app.services.existing_setup_resume",
            ],
            cwd=root,
            env=env,
            shell=False,
            check=False,
            timeout=300,
        )
        if resume.returncode not in {0, 4}:
            raise RuntimeError(f"resume_failed:{resume.returncode}")
        subprocess.run(
            ["docker", "compose", "up", "-d", "flask"],
            cwd=root,
            env=env,
            shell=False,
            check=True,
            timeout=300,
        )
        running = wait_runtime(root, target)
        write_result(
            result_file,
            request_id=request_id,
            action=action,
            state="runtime_verified",
            current=target,
            target=target,
            running=running,
            code="updated",
            message="MSH source, images, services, and running commit were updated and verified.",
        )
        return True
    except Exception as exc:  # noqa: BLE001 - host boundary emits only safe result text
        current = None
        try:
            current = git(root, "rev-parse", "--verify", "HEAD^{commit}").stdout.strip().lower()
        except Exception:  # noqa: BLE001
            pass
        write_result(
            result_file,
            request_id=request_id if REQUEST_ID_RE.fullmatch(request_id) else "invalid-request",
            action=action if action in {"check", "apply"} else "unknown",
            state="error",
            current=current,
            target=target,
            running=running_commit(root),
            code="host_update_failed",
            message="The host update agent stopped safely before it could verify the requested runtime.",
        )
        print(f"MSH update request failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return True
    finally:
        processing.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--data-directory", required=True)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    root = Path(args.repo_root).resolve()
    directory = Path(args.data_directory).resolve() / "federation" / "update-agent"
    directory.mkdir(parents=True, exist_ok=True)
    lock_path = directory / "agent.lock"
    with lock_path.open("a+") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0
        while True:
            processed = process_once(root, directory / "request.json", directory / "result.json")
            if args.once:
                return 0
            if not processed:
                time.sleep(max(0.1, min(args.poll_seconds, 30.0)))


if __name__ == "__main__":
    raise SystemExit(main())
