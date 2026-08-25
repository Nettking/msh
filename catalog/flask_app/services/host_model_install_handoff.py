"""Bounded local model-install request to the host-owned update agent.

The Flask container has no authority to measure or mutate Docker's host backing
resource directly. It can only enqueue a declarative model request in the same
host-owned handoff used by software updates. Model installation is accepted only
while that host mutation lane is idle, so a short-lived model request can never
sit behind a long update and expire before the agent reaches it.
"""

from __future__ import annotations

import os
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.federation.model_resource_pull import MODEL_RE, TARGETS

from .federation_update_handoff import HostUpdateBusyError, HostUpdateHandoff

MODEL_REQUEST_SCHEMA = "fcp.host-model-install-request.v1"
MODEL_REQUEST_TTL_SECONDS = 120
MODEL_REQUEST_ID = "host-model-install"
HOST_OPERATION_STALE_SECONDS = 3 * 60 * 60


def _stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class HostModelInstallHandoff(HostUpdateHandoff):
    """Queue one local model pull without giving Flask Docker/host authority."""

    def _active_processing_request(self) -> bool:
        """Return true while a bounded host mutation is still plausibly active.

        Update agents rename ``request.json`` to ``processing-*.json`` before
        doing host work. A hard-killed agent can leave that claim behind, so an
        ancient processing file must not permanently fence future model repair.
        Three hours exceeds the bounded update/model-operation envelope and is
        therefore treated as stale evidence rather than an active mutation.
        """

        now = time.time()
        try:
            entries = tuple(self.directory.glob("processing-*.json"))
        except OSError:
            return True
        for path in entries:
            try:
                age = now - path.stat().st_mtime
            except OSError:
                return True
            if age <= HOST_OPERATION_STALE_SECONDS:
                return True
        return False

    def _write_when_host_idle(self, value: dict[str, object]) -> None:
        """Atomically admit a model request only when no host operation owns the lane."""

        payload = self._bounded_json(value)
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        deadline = time.monotonic() + min(self.timeout, 5.0)
        lock_descriptor = self._acquire_writer_lock(deadline)
        try:
            # Check request_file first. If the agent claims it concurrently,
            # processing-* is then visible to the second check. If it is absent
            # here, there is no older pending request for the agent to claim.
            if self.request_file.exists() or self._active_processing_request():
                raise HostUpdateBusyError("host_model_install_lane_busy")
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
        finally:
            os.close(lock_descriptor)
            self.writer_lock.unlink(missing_ok=True)

    def queue(self, *, model: str, target: str = "ollama") -> tuple[bool, str]:
        selected = str(model or "").strip()
        if not MODEL_RE.fullmatch(selected):
            return False, "Invalid model identifier."
        if target not in TARGETS:
            return False, "Invalid model installation target."
        now = datetime.now(timezone.utc)
        request = {
            "schema": MODEL_REQUEST_SCHEMA,
            # Model installation is one serialized host-operation slot. Reusing
            # one request identity keeps the agent's per-request result filename
            # bounded instead of introducing an unbounded result history.
            "request_id": MODEL_REQUEST_ID,
            "action": "install",
            "model": selected,
            "target": target,
            "created_at": _stamp(now),
            "expires_at": _stamp(now + timedelta(seconds=MODEL_REQUEST_TTL_SECONDS)),
        }
        try:
            self._write_when_host_idle(request)
        except HostUpdateBusyError:
            return False, (
                "The host is already processing another update or model request. "
                "Retry after that bounded host operation has finished."
            )
        return True, (
            f"Model installation was queued on the local FCP host: {selected}. "
            "Core FCP remains available while the host enforces disk pressure."
        )


__all__ = [
    "HOST_OPERATION_STALE_SECONDS",
    "HostModelInstallHandoff",
    "MODEL_REQUEST_ID",
    "MODEL_REQUEST_SCHEMA",
    "MODEL_REQUEST_TTL_SECONDS",
]
