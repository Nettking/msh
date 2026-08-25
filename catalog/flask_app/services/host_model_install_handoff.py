"""Bounded local model-install request to the host-owned update agent.

The Flask container has no authority to measure or mutate Docker's host backing
resource directly. It can only enqueue a declarative model request in the same
single-producer handoff already serialized with host update requests. The host
agent revalidates the model and performs pressure-aware installation.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from catalog.federation.model_resource_pull import MODEL_RE, TARGETS

from .federation_update_handoff import HostUpdateBusyError, HostUpdateHandoff

MODEL_REQUEST_SCHEMA = "fcp.host-model-install-request.v1"
MODEL_REQUEST_TTL_SECONDS = 120


def _stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class HostModelInstallHandoff(HostUpdateHandoff):
    """Queue one local model pull without giving Flask Docker/host authority."""

    def queue(self, *, model: str, target: str = "ollama") -> tuple[bool, str]:
        selected = str(model or "").strip()
        if not MODEL_RE.fullmatch(selected):
            return False, "Invalid model identifier."
        if target not in TARGETS:
            return False, "Invalid model installation target."
        now = datetime.now(timezone.utc)
        request = {
            "schema": MODEL_REQUEST_SCHEMA,
            "request_id": f"host-model-{uuid.uuid4().hex}",
            "action": "install",
            "model": selected,
            "target": target,
            "created_at": _stamp(now),
            "expires_at": _stamp(now + timedelta(seconds=MODEL_REQUEST_TTL_SECONDS)),
        }
        try:
            self._write_request(request)
        except HostUpdateBusyError:
            return False, (
                "The host is already processing another update or model request. "
                "Retry after that bounded host operation has been claimed."
            )
        return True, (
            f"Model installation was queued on the local FCP host: {selected}. "
            "Core FCP remains available while the host enforces disk pressure."
        )


__all__ = ["HostModelInstallHandoff", "MODEL_REQUEST_SCHEMA", "MODEL_REQUEST_TTL_SECONDS"]
