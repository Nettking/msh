#!/usr/bin/env python3
"""Bounded JSON codec for the Termux host-owned Federation update agent.

This helper runs inside the existing PRoot Python environment. It has no Git,
process-control, or Android host authority; the Termux shell agent owns those
operations and uses this module only to validate requests and atomically write
bounded results.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path

APPROVED_REPOSITORY = "Nettking/msh"
APPROVED_BRANCH = "main"
REQUEST_SCHEMA = "fcp.host-update-request.v1"
RESULT_SCHEMA = "fcp.host-update-result.v1"
MAX_BYTES = 8192
OID_RE = re.compile(r"^[0-9a-f]{40}$")
REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


def _utc(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("malformed_timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("malformed_timestamp")
    return parsed.astimezone(timezone.utc)


def parse_request(path: Path) -> tuple[str, str, str, int]:
    raw = path.read_bytes()
    if len(raw) > MAX_BYTES:
        raise ValueError("request_too_large")
    value = json.loads(raw.decode("utf-8"))
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
    if not isinstance(target, str) or not OID_RE.fullmatch(target):
        raise ValueError("malformed_target")

    created = _utc(value.get("created_at"))
    expires = _utc(value.get("expires_at"))
    now = datetime.now(timezone.utc)
    if (
        created.timestamp() > now.timestamp() + 60
        or expires <= now
        or (expires - created).total_seconds() > 900
    ):
        raise ValueError("expired_or_invalid_request")

    activate_epoch = 0
    if action == "apply":
        activate_after = _utc(value.get("activate_after"))
        if (
            activate_after < created
            or activate_after > expires
            or (activate_after - created).total_seconds() > 30
        ):
            raise ValueError("invalid_activation_grace")
        activate_epoch = int(activate_after.timestamp())

    return request_id, action, target, activate_epoch


def _atomic_json(path: Path, value: dict[str, object]) -> None:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    if len(encoded) > MAX_BYTES:
        raise ValueError("result_too_large")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
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
    if not REQUEST_ID_RE.fullmatch(request_id):
        raise ValueError("malformed_request_id")
    if action not in {"check", "apply"}:
        raise ValueError("malformed_action")
    for commit in (current, target, running):
        if commit is not None and not OID_RE.fullmatch(commit):
            raise ValueError("malformed_commit")
    if len(state) > 64 or len(code or "") > 128 or len(message) > 512:
        raise ValueError("result_field_too_large")

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
        "completed_at": datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
    }
    digest = hashlib.sha256(request_id.encode("utf-8")).hexdigest()
    _atomic_json(path.with_name(f"result-{digest}.json"), value)
    _atomic_json(path, value)


def _optional_commit(value: str) -> str | None:
    return value or None


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    parse = subparsers.add_parser("parse-request")
    parse.add_argument("path", type=Path)

    result = subparsers.add_parser("write-result")
    result.add_argument("path", type=Path)
    result.add_argument("--request-id", required=True)
    result.add_argument("--action", required=True)
    result.add_argument("--state", required=True)
    result.add_argument("--current", default="")
    result.add_argument("--target", default="")
    result.add_argument("--running", default="")
    result.add_argument("--code", default="")
    result.add_argument("--message", default="")

    args = parser.parse_args()
    if args.command == "parse-request":
        request_id, action, target, activate_epoch = parse_request(args.path)
        print(request_id)
        print(action)
        print(target)
        print(activate_epoch)
        return 0

    write_result(
        args.path,
        request_id=args.request_id,
        action=args.action,
        state=args.state,
        current=_optional_commit(args.current),
        target=_optional_commit(args.target),
        running=_optional_commit(args.running),
        code=args.code or None,
        message=args.message,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
