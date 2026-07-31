"""Relay transport for signed storage control plans.

The relay intentionally rejects structured fields named ``signature`` as
credential-shaped metadata.  Control signatures are public verification
material, so the complete signed plan travels as one bounded JSON frame, matching
the established storage request/reply bridge.  Nodes still parse, hash, pin, and
cryptographically verify the frame before applying it.
"""

from __future__ import annotations

import json
import time
from typing import Any

from .control_sync import (
    STORAGE_CONTROL_RELAY_KIND,
    StorageControlPlan,
    StorageControlPublishResult,
)
from .errors import FederationValidationError


class FramedStorageControlRelayPublisher:
    """Deliver one signed plan and require a bound acknowledgement per target."""

    def __init__(self, client: Any, *, timeout: float = 15.0) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self.client = client
        self.timeout = float(timeout)

    async def publish(
        self,
        plan: StorageControlPlan,
        target_node_ids: tuple[str, ...],
    ) -> StorageControlPublishResult:
        if self.client.node_id != plan.authority_node_id:
            raise FederationValidationError(
                "storage-control-publisher-mismatch",
                "authority_node_id",
                "connected publisher does not own the signing identity",
            )
        targets = tuple(
            dict.fromkeys(self._target(item) for item in target_node_ids)
        )
        if not targets:
            raise FederationValidationError(
                "missing-storage-control-target",
                "target_node_ids",
                "at least one target is required",
            )
        frame = json.dumps(
            plan.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        for target in targets:
            delivery = await self.client.send_message(
                session_id=plan.session_id,
                target_node_id=target,
                request_id=f"control-{plan.publication_id}-{target}",
                payload={
                    "kind": STORAGE_CONTROL_RELAY_KIND,
                    "message": "plan",
                    "frame": frame,
                },
            )
            if not isinstance(delivery, dict) or delivery.get("delivered") is not True:
                raise FederationValidationError(
                    "storage-control-delivery-failed",
                    "target_node_id",
                    f"relay did not confirm delivery to {target}",
                )
        return await self._collect_acknowledgements(plan, targets)

    async def _collect_acknowledgements(
        self,
        plan: StorageControlPlan,
        targets: tuple[str, ...],
    ) -> StorageControlPublishResult:
        pending = set(targets)
        acknowledged: set[str] = set()
        deadline = time.monotonic() + self.timeout
        while pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    "storage control acknowledgement timeout for "
                    + ", ".join(sorted(pending))
                )
            message = await self.client.receive_message(timeout=remaining)
            payload = getattr(message, "payload", None)
            actor = getattr(message, "actor_node_id", None)
            if not self._matches(plan, pending, message, payload, actor):
                continue
            if payload.get("status") not in {"applied", "duplicate"}:
                raise FederationValidationError(
                    "storage-control-target-rejected",
                    "target_node_id",
                    f"{actor}: {payload.get('error')}",
                )
            pending.remove(actor)
            acknowledged.add(actor)
        return StorageControlPublishResult(
            plan.publication_id,
            plan.publication_revision,
            tuple(sorted(acknowledged)),
        )

    @staticmethod
    def _matches(
        plan: StorageControlPlan,
        pending: set[str],
        message: Any,
        payload: Any,
        actor: Any,
    ) -> bool:
        return bool(
            actor in pending
            and getattr(message, "session_id", None) == plan.session_id
            and isinstance(payload, dict)
            and payload.get("kind") == STORAGE_CONTROL_RELAY_KIND
            and payload.get("message") == "response"
            and payload.get("publication_id") == plan.publication_id
            and payload.get("publication_revision") == plan.publication_revision
            and payload.get("content_hash") == plan.content_hash
        )

    @staticmethod
    def _target(value: Any) -> str:
        if (
            not isinstance(value, str)
            or not value.strip()
            or any(ord(character) < 32 for character in value)
        ):
            raise FederationValidationError(
                "invalid-storage-control-target",
                "target_node_id",
                "must be non-empty text",
            )
        return value
