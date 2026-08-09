"""Bounded file handoff from the Flask container to the host update agent.

The Flask process never receives shell, Git, Docker, or host filesystem authority.
It may only write one declarative request into the existing bind-mounted MSH data
folder and read the agent's bounded result. The host agent independently
revalidates every security-sensitive field before mutating the checkout/runtime.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from catalog.federation.software_update import (
    APPROVED_BRANCH,
    APPROVED_REPOSITORY,
    OID_RE,
    UpdateInspection,
)

REQUEST_SCHEMA = "msh.host-update-request.v1"
RESULT_SCHEMA = "msh.host-update-result.v1"
MAX_HANDOFF_BYTES = 8192
ACTIVATION_GRACE_SECONDS = 2
HOST_REQUEST_TTL_SECONDS = 120


def _stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class HostUpdateHandoff:
    """Queue exact-commit update operations for a separately owned host agent."""

    def __init__(
        self,
        directory: Path | str,
        *,
        timeout: float = 20.0,
        poll_interval: float = 0.1,
    ) -> None:
        self.directory = Path(directory)
        self.request_file = self.directory / "request.json"
        self.result_file = self.directory / "result.json"
        self.timeout = min(max(float(timeout), 1.0), 60.0)
        self.poll_interval = min(max(float(poll_interval), 0.02), 1.0)

    @staticmethod
    def _inspection(value: dict[str, Any]) -> UpdateInspection:
        def optional_text(field: str) -> str | None:
            candidate = value.get(field)
            return candidate if isinstance(candidate, str) else None

        return UpdateInspection(
            state=str(value.get("state") or "error"),
            current_commit=optional_text("current_commit"),
            target_commit=optional_text("target_commit"),
            code=optional_text("code"),
            message=optional_text("message"),
            running_commit=optional_text("running_commit"),
            request_id=optional_text("request_id"),
        )

    @staticmethod
    def _bounded_json(value: object) -> bytes:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        if len(encoded) > MAX_HANDOFF_BYTES:
            raise ValueError("host_update_message_too_large")
        return encoded

    @staticmethod
    def _read(path: Path) -> dict[str, Any] | None:
        try:
            raw = path.read_bytes()
            if len(raw) > MAX_HANDOFF_BYTES:
                return None
            value = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return None
        return value if isinstance(value, dict) else None

    def _write_request(self, value: dict[str, object]) -> None:
        payload = self._bounded_json(value)
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".request-",
            suffix=".json",
            dir=self.directory,
        )
        temporary = Path(temporary_name)
        try:
            os.chmod(temporary, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                descriptor = -1
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.request_file)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            temporary.unlink(missing_ok=True)

    def _request(
        self,
        *,
        action: str,
        target: str | None,
        request_id: str | None = None,
        ttl_seconds: int = HOST_REQUEST_TTL_SECONDS,
    ) -> dict[str, object]:
        if action not in {"check", "apply"}:
            raise ValueError("unsupported_host_update_action")
        if target is not None and not OID_RE.fullmatch(target):
            raise ValueError("invalid_host_update_target")
        if not 1 <= ttl_seconds <= HOST_REQUEST_TTL_SECONDS:
            raise ValueError("invalid_host_update_ttl")
        now = datetime.now(timezone.utc)
        value: dict[str, object] = {
            "schema": REQUEST_SCHEMA,
            "request_id": request_id or f"host-update-{uuid.uuid4().hex}",
            "action": action,
            "repository": APPROVED_REPOSITORY,
            "branch": APPROVED_BRANCH,
            "target_commit": target,
            "created_at": _stamp(now),
            "expires_at": _stamp(now + timedelta(seconds=ttl_seconds)),
        }
        if action == "apply":
            value["activate_after"] = _stamp(
                now + timedelta(seconds=ACTIVATION_GRACE_SECONDS)
            )
        return value

    def latest_result(self) -> UpdateInspection | None:
        value = self._read(self.result_file)
        if value is None or value.get("schema") != RESULT_SCHEMA:
            return None
        return self._inspection(value)

    def inspect(
        self,
        *,
        target: str | None = None,
        fetch: bool = True,
    ) -> UpdateInspection:
        # ``fetch`` remains in the adapter shape; the host agent always performs
        # its own bounded fetch for check operations.
        del fetch
        request = self._request(action="check", target=target)
        request_id = str(request["request_id"])
        self._write_request(request)
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            result = self.latest_result()
            if result is not None and result.request_id == request_id:
                return result
            time.sleep(self.poll_interval)
        return UpdateInspection(
            "error",
            target_commit=target,
            code="host_update_agent_unavailable",
            message="The local host update agent did not complete the bounded check.",
            request_id=request_id,
        )

    def apply(
        self,
        target: str,
        *,
        request_id: str | None = None,
    ) -> UpdateInspection:
        if not OID_RE.fullmatch(target):
            return UpdateInspection(
                "error",
                target_commit=target,
                code="target_unavailable",
                message="The requested commit is not a full Git object ID.",
            )
        request = self._request(
            action="apply",
            target=target,
            request_id=request_id,
        )
        self._write_request(request)
        return UpdateInspection(
            "activation_queued",
            target_commit=target,
            code="host_activation_queued",
            message=(
                "The exact-commit update was queued for the local host agent. "
                "Success is reported only after rebuild, restart, and "
                "running-commit verification."
            ),
            request_id=str(request["request_id"]),
        )


__all__ = [
    "ACTIVATION_GRACE_SECONDS",
    "HOST_REQUEST_TTL_SECONDS",
    "MAX_HANDOFF_BYTES",
    "REQUEST_SCHEMA",
    "RESULT_SCHEMA",
    "HostUpdateHandoff",
]
