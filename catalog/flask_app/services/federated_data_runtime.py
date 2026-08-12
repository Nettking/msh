"""Public pairing-runtime seam for Federation JSONL publication and batch reads."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any

from catalog.federation.errors import AuthenticationError, FederationValidationError
from catalog.federation.recorder_storage_relay import RelayRecorderStorageClient
from catalog.federation.storage_catalog import CommittedBatchReference

from .federation_pairing_service import RemotePairingState
from .resilient_pairing_runtime import ResilientPairingRelayRuntime


class FederatedDataPairingRelayRuntime(ResilientPairingRelayRuntime):
    """Expose bounded generic storage operations over the shared paired client."""

    @staticmethod
    def _require_bound_session(state: RemotePairingState, session_id: str) -> None:
        if session_id != state.binding.internal_session_id:
            raise AuthenticationError(
                "pairing-membership-mismatch",
                "Federation data must use the paired Federation session",
                "session_id",
            )

    async def _publish_federated_batches(
        self,
        state: RemotePairingState,
        *,
        session_id: str,
        authority_node_id: str,
        batches: tuple[Mapping[str, Any], ...],
    ) -> tuple[object, ...]:
        self._require_bound_session(state, session_id)
        if not batches or len(batches) > 128:
            raise FederationValidationError(
                "invalid-federated-batch-count",
                "batches",
                "must contain between 1 and 128 bounded batches",
            )
        await self._ensure_connected(state)
        storage = RelayRecorderStorageClient(
            self._connected_client(),
            session_id=session_id,
            authority_node_id=authority_node_id,
            request_timeout=min(self.timeout_seconds, 60.0),
        )
        outcomes: list[object] = []
        try:
            for batch in batches:
                created_at = batch.get("created_at")
                if (
                    not isinstance(created_at, datetime)
                    or created_at.tzinfo is None
                    or created_at.utcoffset() is None
                ):
                    raise FederationValidationError(
                        "invalid-federated-batch-created-at",
                        "created_at",
                        "must be a timezone-aware datetime",
                    )
                version = batch.get("dataset_schema_version")
                if isinstance(version, bool) or not isinstance(version, int):
                    raise FederationValidationError(
                        "invalid-federated-batch-schema-version",
                        "dataset_schema_version",
                        "must be a positive integer",
                    )
                outcomes.append(
                    await storage.ingest_batch(
                        group_id=str(batch.get("group_id") or ""),
                        dataset_id=str(batch.get("dataset_id") or ""),
                        batch_id=str(batch.get("batch_id") or ""),
                        idempotency_key=str(batch.get("idempotency_key") or ""),
                        content=batch.get("content"),
                        created_at=created_at,
                        dataset_schema_name=str(
                            batch.get("dataset_schema_name") or ""
                        ),
                        dataset_schema_version=version,
                    )
                )
        finally:
            await storage.close()
        return tuple(outcomes)

    def publish_federated_batches(
        self,
        state: RemotePairingState,
        *,
        session_id: str,
        authority_node_id: str,
        batches: Iterable[Mapping[str, Any]],
    ) -> tuple[object, ...]:
        """Commit a bounded sequence of generic batches through logical storage."""

        batch_tuple = tuple(dict(batch) for batch in batches)
        return self._submit(
            self._publish_federated_batches(
                state,
                session_id=session_id,
                authority_node_id=authority_node_id,
                batches=batch_tuple,
            )
        )

    async def _read_federated_batches(
        self,
        state: RemotePairingState,
        *,
        session_id: str,
        authority_node_id: str,
        references: tuple[CommittedBatchReference, ...],
    ) -> tuple[object, ...]:
        self._require_bound_session(state, session_id)
        if not references or len(references) > 20:
            raise FederationValidationError(
                "invalid-federated-read-count",
                "references",
                "must contain between 1 and 20 committed references",
            )
        if any(reference.session_id != session_id for reference in references):
            raise AuthenticationError(
                "pairing-membership-mismatch",
                "committed references must belong to the paired Federation session",
                "session_id",
            )
        await self._ensure_connected(state)
        storage = RelayRecorderStorageClient(
            self._connected_client(),
            session_id=session_id,
            authority_node_id=authority_node_id,
            request_timeout=min(self.timeout_seconds, 60.0),
        )
        try:
            return tuple(
                [
                    await storage.read_committed_batch(reference)
                    for reference in references
                ]
            )
        finally:
            await storage.close()

    def read_federated_batches(
        self,
        state: RemotePairingState,
        *,
        session_id: str,
        authority_node_id: str,
        references: Iterable[CommittedBatchReference],
    ) -> tuple[object, ...]:
        """Read several manifest-pinned batches over one authenticated client."""

        reference_tuple = tuple(references)
        return self._submit(
            self._read_federated_batches(
                state,
                session_id=session_id,
                authority_node_id=authority_node_id,
                references=reference_tuple,
            )
        )


__all__ = ["FederatedDataPairingRelayRuntime"]
