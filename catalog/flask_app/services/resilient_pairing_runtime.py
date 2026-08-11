"""Recovery-aware relay pairing for devices with an existing identity.

A pairing code is primarily an initial enrollment mechanism. A device may,
however, have completed enrollment or membership before a browser/network
failure prevented the remote binding from being persisted. Recovery therefore
authenticates the existing identity and verifies existing membership rather than
requiring new authority.
"""

from __future__ import annotations

from typing import Any

from catalog.federation.errors import (
    AuthenticationError,
    FederationOperationError,
)
from catalog.federation.models import CapabilityAnnouncement, SessionEvent
from catalog.federation.recorder_storage_relay import RelayRecorderStorageClient
from catalog.federation.storage_catalog import (
    CommittedBatchDeltaPage,
    CommittedBatchPage,
    CommittedBatchReference,
)
from catalog.federation.onboarding_models import (
    FederationConnectionState,
    FederationSessionBinding,
)
from catalog.node.client import RelayRemoteError

from .federation_pairing_service import (
    PairingOffer,
    PairingRelayNodeClient,
    PairingRelayRuntime,
    RemotePairingState,
    _parse_stamp,
    _stamp,
)


class ResilientPairingRelayRuntime(PairingRelayRuntime):
    """Redeem and reuse one authenticated Federation relay connection."""

    @staticmethod
    def _existing_session(
        status: dict[str, Any],
        session_id: str,
    ) -> dict[str, Any] | None:
        sessions = status.get("sessions")
        if not isinstance(sessions, list):
            return None
        for value in sessions:
            if isinstance(value, dict) and value.get("session_id") == session_id:
                return value
        return None

    def _connected_client(self) -> PairingRelayNodeClient:
        client = self._client
        if client is None or not client.connected_event.is_set():
            raise FederationOperationError(
                "pairing-relay-disconnected",
                "the paired relay is not connected",
            )
        return client

    async def _session_events(
        self,
        state: RemotePairingState,
        *,
        session_id: str,
        after_revision: int,
    ) -> tuple[SessionEvent, ...]:
        if session_id != state.binding.internal_session_id:
            raise AuthenticationError(
                "pairing-membership-mismatch",
                "shared knowledge must use the paired Federation session",
                "session_id",
            )
        await self._ensure_connected(state)
        client = self._connected_client()
        await client.request_replay(session_id)
        return client.state.applied_events(
            session_id,
            after_revision=after_revision,
        )

    def session_events(
        self,
        state: RemotePairingState,
        *,
        session_id: str,
        after_revision: int = 0,
    ) -> tuple[SessionEvent, ...]:
        return self._submit(
            self._session_events(
                state,
                session_id=session_id,
                after_revision=after_revision,
            )
        )

    async def _append_session_event(
        self,
        state: RemotePairingState,
        *,
        session_id: str,
        event_type: str,
        payload: dict[str, Any],
        request_id: str,
    ) -> SessionEvent:
        if session_id != state.binding.internal_session_id:
            raise AuthenticationError(
                "pairing-membership-mismatch",
                "shared knowledge must use the paired Federation session",
                "session_id",
            )
        await self._ensure_connected(state)
        client = self._connected_client()
        return await client.append_event(
            session_id=session_id,
            event_type=event_type,
            payload=payload,
            request_id=request_id,
        )

    def append_session_event(
        self,
        state: RemotePairingState,
        *,
        session_id: str,
        event_type: str,
        payload: dict[str, Any],
        request_id: str,
    ) -> SessionEvent:
        return self._submit(
            self._append_session_event(
                state,
                session_id=session_id,
                event_type=event_type,
                payload=payload,
                request_id=request_id,
            )
        )

    async def _announce_capability(
        self,
        state: RemotePairingState,
        capability: CapabilityAnnouncement,
        *,
        request_id: str,
    ) -> CapabilityAnnouncement:
        """Publish metadata only for the authenticated bound device/session."""

        if (
            capability.session_id != state.binding.internal_session_id
            or capability.node_id != state.binding.device_id
        ):
            raise AuthenticationError(
                "pairing-capability-binding-mismatch",
                "capability metadata must use the paired device and Federation session",
                "binding",
            )
        await self._ensure_connected(state)
        client = self._connected_client()
        if client.node_id != capability.node_id:
            raise AuthenticationError(
                "pairing-actor-mismatch",
                "capability metadata must use the authenticated paired identity",
                "actor_node_id",
            )
        result = await client.request(
            "capability.announce",
            session_id=capability.session_id,
            payload={"announcement": capability.to_dict()},
            request_id=request_id,
        )
        value = result.get("announcement")
        if not isinstance(value, dict):
            raise FederationOperationError(
                "invalid-capability-announcement-response",
                "relay did not return the accepted capability metadata",
            )
        accepted = CapabilityAnnouncement.from_dict(value)
        if accepted != capability:
            raise FederationOperationError(
                "capability-announcement-response-mismatch",
                "relay returned different capability metadata than submitted",
            )
        save = getattr(client.state, "save_capability", None)
        if callable(save):
            save(accepted, now=self._clock())
        await client.request_replay(capability.session_id)
        return accepted

    def announce_capability(
        self,
        state: RemotePairingState,
        capability: CapabilityAnnouncement,
        *,
        request_id: str,
    ) -> CapabilityAnnouncement:
        return self._submit(
            self._announce_capability(
                state,
                capability,
                request_id=request_id,
            )
        )

    async def _list_committed_batches(
        self,
        state: RemotePairingState,
        *,
        authority_node_id: str,
        group_id: str,
        dataset_id: str | None,
        limit: int,
        cursor: str | None,
    ) -> CommittedBatchPage:
        await self._ensure_connected(state)
        storage = RelayRecorderStorageClient(
            self._connected_client(),
            session_id=state.binding.internal_session_id,
            authority_node_id=authority_node_id,
            request_timeout=min(self.timeout_seconds, 60.0),
        )
        try:
            return await storage.list_committed_batches(
                group_id=group_id,
                dataset_id=dataset_id,
                limit=limit,
                cursor=cursor,
            )
        finally:
            await storage.close()

    def list_committed_batches(
        self,
        state: RemotePairingState,
        *,
        authority_node_id: str,
        group_id: str,
        dataset_id: str | None = None,
        limit: int = 20,
        cursor: str | None = None,
    ) -> CommittedBatchPage:
        """Discover committed storage through the shared authenticated client."""

        return self._submit(
            self._list_committed_batches(
                state,
                authority_node_id=authority_node_id,
                group_id=group_id,
                dataset_id=dataset_id,
                limit=limit,
                cursor=cursor,
            )
        )

    async def _list_committed_batch_delta(
        self,
        state: RemotePairingState,
        *,
        authority_node_id: str,
        group_id: str,
        baseline_manifest_revision: int,
        baseline_manifest_hash: str,
        dataset_id: str | None,
        limit: int,
        cursor: str | None,
    ) -> CommittedBatchDeltaPage:
        await self._ensure_connected(state)
        storage = RelayRecorderStorageClient(
            self._connected_client(),
            session_id=state.binding.internal_session_id,
            authority_node_id=authority_node_id,
            request_timeout=min(self.timeout_seconds, 60.0),
        )
        try:
            return await storage.list_committed_batch_delta(
                group_id=group_id,
                baseline_manifest_revision=baseline_manifest_revision,
                baseline_manifest_hash=baseline_manifest_hash,
                dataset_id=dataset_id,
                limit=limit,
                cursor=cursor,
            )
        finally:
            await storage.close()

    def list_committed_batch_delta(
        self,
        state: RemotePairingState,
        *,
        authority_node_id: str,
        group_id: str,
        baseline_manifest_revision: int,
        baseline_manifest_hash: str,
        dataset_id: str | None = None,
        limit: int = 20,
        cursor: str | None = None,
    ) -> CommittedBatchDeltaPage:
        """Discover commits after one exact trusted manifest checkpoint."""

        return self._submit(
            self._list_committed_batch_delta(
                state,
                authority_node_id=authority_node_id,
                group_id=group_id,
                baseline_manifest_revision=baseline_manifest_revision,
                baseline_manifest_hash=baseline_manifest_hash,
                dataset_id=dataset_id,
                limit=limit,
                cursor=cursor,
            )
        )

    async def _read_committed_batch(
        self,
        state: RemotePairingState,
        *,
        authority_node_id: str,
        reference: CommittedBatchReference,
    ) -> object:
        await self._ensure_connected(state)
        storage = RelayRecorderStorageClient(
            self._connected_client(),
            session_id=state.binding.internal_session_id,
            authority_node_id=authority_node_id,
            request_timeout=min(self.timeout_seconds, 60.0),
        )
        try:
            return await storage.read_committed_batch(reference)
        finally:
            await storage.close()

    def read_committed_batch(
        self,
        state: RemotePairingState,
        *,
        authority_node_id: str,
        reference: CommittedBatchReference,
    ) -> object:
        """Fetch one manifest-pinned batch through the authenticated authority."""

        return self._submit(
            self._read_committed_batch(
                state,
                authority_node_id=authority_node_id,
                reference=reference,
            )
        )

    async def _redeem(self, offer: PairingOffer) -> FederationSessionBinding:
        await self._disconnect_current()
        client = PairingRelayNodeClient(
            state_directory=self.state_directory,
            relay_url=offer.relay_url,
            display_name=self.display_name,
            clock=self._clock,
            request_timeout=min(self.timeout_seconds, 60.0),
        )
        try:
            try:
                await client.connect(enrollment_token=offer.enrollment_token)
            except RelayRemoteError as exc:
                if exc.code != "identity-already-enrolled":
                    raise
                # The relay has already accepted this exact public identity.
                # Authenticate with the retained private key instead of trying
                # to consume another enrollment token.
                await client.connect()

            try:
                session = await client.join_session(offer.invitation_token)
            except RelayRemoteError as exc:
                if exc.code != "already-session-member":
                    raise
                # A previous attempt may have joined successfully before the
                # local remote-binding file was saved. Recover only after the
                # authenticated coordinator confirms the same session.
                session = self._existing_session(
                    await client.coordinator_status(),
                    offer.internal_session_id,
                )
                if session is None:
                    raise AuthenticationError(
                        "pairing-membership-missing",
                        "the authenticated device is not a member of the signed session",
                        "binding",
                    ) from exc
        except BaseException:
            if client.connected_event.is_set():
                await client.disconnect(error_code="pairing-failed")
            raise

        session_id = str(session.get("session_id") or "")
        if session_id != offer.internal_session_id:
            await client.disconnect(error_code="pairing-session-mismatch")
            raise AuthenticationError(
                "pairing-session-mismatch",
                "the relay joined a different session than the signed offer",
                "pairing_code",
            )
        revision = session.get("revision")
        if isinstance(revision, bool) or not isinstance(revision, int) or revision <= 0:
            revision = 1
        created_at = _parse_stamp(
            session.get("created_at") or _stamp(self._clock()),
            "created_at",
        )
        self._client = client
        self._relay_url = offer.relay_url
        return FederationSessionBinding(
            federation_id=offer.federation_id,
            internal_session_id=offer.internal_session_id,
            device_id=client.node_id,
            state=FederationConnectionState.CONNECTED,
            revision=revision,
            trusted=True,
            created_at=created_at,
            last_verified_at=self._clock(),
        )


__all__ = ["ResilientPairingRelayRuntime"]
