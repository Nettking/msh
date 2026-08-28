"""Headless Federation lifecycle for the standalone MTConnect recorder.

The recorder has one stable FCP device identity.  A short-lived ``FCP1-``
pairing code is consumed only on first join; subsequent starts reconnect from
public-safe pairing state.  Capture remains independent of Federation
availability.  When the Federation exposes exactly one ready logical-storage
group (or the operator selects one), locally committed recorder batches are
reconciled into a durable outbox and delivered through the authenticated
storage-control authority.
"""

from __future__ import annotations

import asyncio
import math
import sqlite3
import threading
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from catalog.federation.errors import (
    AuthenticationError,
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.outbox import SQLiteOutbox
from catalog.federation.recorder_delivery import (
    RECORDER_STORAGE_SCHEMA,
    DurableRecorderDeliveryQueue,
    RecorderDeliveryRunResult,
)
from catalog.federation.recorder_publication import (
    RecorderArchiveReconciler,
    RecorderFederationDeliveryWorker,
    RecorderPublicationTarget,
)
from catalog.federation.recorder_storage_relay import (
    RECORDER_RELAY_SAFE_CONTENT_BYTES,
    STORAGE_CONTROL_CAPABILITY_PROTOCOL,
    STORAGE_CONTROL_CAPABILITY_TYPE,
    RelayRecorderStorageClient,
)
from catalog.flask_app.services.federated_data_runtime import (
    FederatedDataPairingRelayRuntime,
)
from catalog.flask_app.services.federation_pairing_service import (
    PairingAwareCapabilityOnboardingService,
    RemotePairingState,
    RemotePairingStore,
)
from catalog.mtconnect_recorder.storage import DurableRecorderStore

MAX_SHARING_READY_SECONDS = 600.0
MAX_FEDERATION_REQUEST_SECONDS = 120.0

#: Every condition the publication driver retries rather than dies on.
#:
#: One tuple, used by the per-cycle boundary *and* by the failure handler's own
#: durable re-read. Two lists drift, and the drift is not symmetric: the handler
#: re-reads the same outbox the cycle just failed on, so anything the cycle
#: treats as retryable can arrive there a second time. A narrower handler guard
#: would let that second occurrence escape to the terminal boundary and convert
#: a retryable condition into a dead driver -- exactly the failure this boundary
#: exists to prevent. ``SQLiteOutbox.pending()`` raises ``sqlite3.Error`` from
#: the store and ``FederationValidationError`` (``malformed-outbox-row``) from
#: its own row decoding, so both paths are genuinely reachable.
PUBLICATION_RETRY_ERRORS: tuple[type[BaseException], ...] = (
    FederationValidationError,
    FederationOperationError,
    AuthenticationError,
    OSError,
    RuntimeError,
    TimeoutError,
    # SQLite reports a full disk, an I/O error, a lock held past its busy
    # timeout and a malformed image as sqlite3.Error, which is not an OSError.
    sqlite3.Error,
)


@dataclass(frozen=True)
class StorageAuthoritySelection:
    authority_node_id: str | None
    group_id: str | None
    state: str


@dataclass(frozen=True)
class RecorderFederationSnapshot:
    status: str
    node_id: str | None = None
    federation_id: str | None = None
    session_id: str | None = None
    storage_state: str = "not-started"
    storage_group: str | None = None
    storage_authority_node_id: str | None = None
    pending_batches: int = 0
    last_committed_count: int = 0
    jsonl_state: str = "not-started"
    jsonl_last_published_count: int = 0
    last_error_code: str | None = None


def _current_recorder_pending(
    pending_entries: tuple[object, ...],
    *,
    session_id: str,
    group_id: str,
) -> tuple[object, ...]:
    return tuple(
        entry
        for entry in pending_entries
        if getattr(entry, "session_id", None) == session_id
        and getattr(entry, "schema_id", None) == RECORDER_STORAGE_SCHEMA
        and getattr(entry, "destination_id", None) == group_id
    )


def _publication_cycle_status(
    *,
    pending_entries: tuple[object, ...],
    session_id: str,
    group_id: str,
    delivery: RecorderDeliveryRunResult,
    retired_total: int = 0,
) -> tuple[str, int, str | None]:
    """Classify only deliverable rows owned by this authenticated session.

    ``retired_total`` is durable truth re-read from the outbox this cycle, not
    a remembered count, and it outranks every other state. A recorder that has
    permanently withdrawn some evidence is not "up-to-date" even when nothing
    is pending: reporting the happy state there would be the exact silent
    disappearance the durable tombstone exists to prevent. It equally outranks
    "backlogged", because a retryable backlog resolves itself and a withdrawal
    does not -- it waits for a person.
    """

    current = _current_recorder_pending(
        pending_entries,
        session_id=session_id,
        group_id=group_id,
    )
    pending = len(current)
    if retired_total > 0:
        return "degraded", pending, "recorder-delivery-retired"
    failed = delivery.pending > 0 or any(
        getattr(entry, "last_error", None) for entry in current
    )
    if failed:
        return "backlogged", pending, "recorder-delivery-pending"
    if pending:
        return "publishing", pending, None
    return "up-to-date", 0, None


def select_storage_authority(
    status: dict[str, Any],
    *,
    session_id: str,
    requested_group: str | None,
) -> StorageAuthoritySelection:
    """Select only the session owner's advertised logical-storage authority."""

    sessions = status.get("sessions")
    capabilities = status.get("capabilities")
    if not isinstance(sessions, list) or not isinstance(capabilities, list):
        return StorageAuthoritySelection(None, None, "status-unavailable")
    owner_node_id = None
    for value in sessions:
        if isinstance(value, dict) and value.get("session_id") == session_id:
            candidate = value.get("created_by_node_id")
            if isinstance(candidate, str) and candidate:
                owner_node_id = candidate
            break
    if owner_node_id is None:
        return StorageAuthoritySelection(None, None, "owner-unavailable")

    groups: set[str] = set()
    authority_ready = False
    for value in capabilities:
        if not isinstance(value, dict):
            continue
        if (
            value.get("session_id") != session_id
            or value.get("node_id") != owner_node_id
            or value.get("type") != STORAGE_CONTROL_CAPABILITY_TYPE
            or value.get("protocol") != STORAGE_CONTROL_CAPABILITY_PROTOCOL
            or value.get("status") != CapabilityStatus.READY.value
        ):
            continue
        properties = value.get("properties")
        if not isinstance(properties, dict):
            continue
        advertised = properties.get("group_ids")
        if not isinstance(advertised, list):
            continue
        authority_ready = True
        groups.update(
            item for item in advertised if isinstance(item, str) and item
        )

    if not authority_ready:
        return StorageAuthoritySelection(None, None, "authority-unavailable")
    if requested_group:
        if requested_group not in groups:
            return StorageAuthoritySelection(
                owner_node_id,
                None,
                "storage-group-unavailable",
            )
        return StorageAuthoritySelection(
            owner_node_id,
            requested_group,
            "ready",
        )
    if len(groups) == 1:
        return StorageAuthoritySelection(
            owner_node_id,
            next(iter(groups)),
            "ready",
        )
    if not groups:
        return StorageAuthoritySelection(
            owner_node_id,
            None,
            "storage-group-unavailable",
        )
    return StorageAuthoritySelection(
        owner_node_id,
        None,
        "storage-group-required",
    )


#: What an operator can actually do about each unready sharing state.
#:
#: A headless recorder reports a machine-readable state, but the person standing
#: at the machine needs the next action. Two of these states are not recorder
#: faults at all: the logical-storage authority runs as its own process on the
#: leader, so a leader that was started normally advertises no authority and a
#: correctly fail-closed recorder looks broken until someone says why.
SHARING_STATE_REMEDIES: dict[str, str] = {
    "status-unavailable": (
        "the Federation leader did not return a usable status; confirm the "
        "leader is running and reachable on its relay address"
    ),
    "owner-unavailable": (
        "this Federation session has no reachable owner in the leader's status; "
        "confirm the leader is signed in to the same tailnet and connected"
    ),
    "authority-unavailable": (
        "the Federation leader advertises no ready logical-storage authority. "
        "It is off by default: set FCP_FEDERATION_STORAGE_AUTHORITY_ENABLED=1 "
        "and FCP_FEDERATION_STORAGE_AUTHORITY_RELAY on the device that created "
        "the Federation, then restart it (see docs/standalone_recorder.md)"
    ),
    "storage-group-unavailable": (
        "the leader's storage authority does not advertise the requested group; "
        "drop --storage-group to auto-select, or pass a group the leader lists"
    ),
    "storage-group-required": (
        "the leader advertises more than one storage group; pass "
        "--storage-group with one advertised group ID"
    ),
    "discovering": (
        "the recorder is still looking for the leader's storage authority; if "
        "this persists, confirm the leader is running that authority process"
    ),
    "publication-driver-failed": (
        "the recorder's Federation publication worker stopped on an "
        "unexpected fault and locally committed evidence is no longer being "
        "delivered; the capture archive is intact, so restart the recorder "
        "and report the recorded error code"
    ),
    "degraded": (
        "some recorder evidence was permanently withdrawn from publication "
        "because it could never be sent; newer evidence is still publishing. "
        "The withdrawn rows are retained as durable tombstones in the recorder "
        "publication outbox, and the primary archive was not touched"
    ),
}


def sharing_state_detail(state: str | None) -> str:
    """Return an operator-actionable description of an unready sharing state."""

    detail = state or "unknown"
    remedy = SHARING_STATE_REMEDIES.get(detail)
    return f"{detail}; {remedy}" if remedy else detail


class RecorderFederationNode:
    """Create/reuse identity, pair, advertise, and publish recorder data."""

    def __init__(
        self,
        *,
        data_directory: Path | str,
        display_name: str,
        source_names: tuple[str, ...],
        requested_storage_group: str | None = None,
        request_timeout: float = 15.0,
        publication_poll_seconds: float = 0.5,
        service: PairingAwareCapabilityOnboardingService | None = None,
        jsonl_publisher: object | None = None,
    ) -> None:
        if (
            not math.isfinite(request_timeout)
            or not 0 < request_timeout <= MAX_FEDERATION_REQUEST_SECONDS
        ):
            raise ValueError(
                "Federation request timeout must be finite, positive, and at "
                f"most {MAX_FEDERATION_REQUEST_SECONDS:g} seconds"
            )
        if (
            not math.isfinite(publication_poll_seconds)
            or not 0 < publication_poll_seconds <= 60.0
        ):
            raise ValueError(
                "Federation publication poll interval must be finite, positive, "
                "and at most 60 seconds"
            )
        self.data_directory = Path(data_directory).resolve()
        self.display_name = display_name.strip() or "FCP MTConnect recorder"
        self.source_names = tuple(sorted(dict.fromkeys(source_names)))
        self.requested_storage_group = (
            requested_storage_group.strip()
            if isinstance(requested_storage_group, str)
            and requested_storage_group.strip()
            else None
        )
        self.request_timeout = float(request_timeout)
        self.publication_poll_seconds = float(publication_poll_seconds)
        federation_root = self.data_directory / "federation"
        self.identity_directory = federation_root / "device"
        self.remote_store = RemotePairingStore(
            federation_root / "onboarding" / "remote_pairing.json"
        )
        self.runtime = FederatedDataPairingRelayRuntime(
            state_directory=self.identity_directory,
            display_name=self.display_name,
            timeout_seconds=self.request_timeout,
        )
        self.service = service or PairingAwareCapabilityOnboardingService(
            identity_directory=self.identity_directory,
            state_database=federation_root / "onboarding" / "onboarding.sqlite3",
            coordinator_database=federation_root / "relay" / "control.sqlite3",
            device_name=self.display_name,
            remote_store=self.remote_store,
            relay_runtime=self.runtime,
        )
        # Test-injected services may own their own runtime/store.
        self.runtime = getattr(self.service, "relay_runtime", self.runtime)
        self.remote_store = getattr(self.service, "remote_store", self.remote_store)
        self.jsonl_publisher = (
            jsonl_publisher
            if jsonl_publisher is not None
            else self._build_jsonl_publisher()
        )
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._publication_future = None
        self._snapshot = RecorderFederationSnapshot(status="not-started")

    def _build_jsonl_publisher(self) -> object:
        # Import lazily because the full workbench bridge imports the shared
        # storage-authority selector from this module. The headless recorder
        # uses only its local publisher entry point, never the mirror path.
        from flask import Flask

        from catalog.flask_app.services.federated_jsonl_product_bridge import (
            FederatedJsonlProductBridge,
        )

        federation_root = self.data_directory / "federation"
        app = Flask(
            "fcp-headless-recorder-jsonl-publisher",
            static_folder=None,
        )
        return FederatedJsonlProductBridge(
            app,
            self.service,
            data_root=self.data_directory,
            database=federation_root / "jsonl-sync.sqlite3",
            cache_root=federation_root / "jsonl-cache",
            mirror_root=federation_root / "shared" / "jsonl-files",
        )

    def _publish_jsonl_sync(
        self,
        state: RemotePairingState,
        *,
        authority_node_id: str,
        group_id: str,
    ) -> object:
        context = self.service.authorized_context()
        if context is None:
            raise FederationOperationError(
                "trusted-federation-context-incomplete",
                "headless JSONL publication requires the trusted paired context",
            )
        publish = getattr(self.jsonl_publisher, "publish_local_once", None)
        if not callable(publish):
            raise FederationOperationError(
                "federated-jsonl-publisher-unavailable",
                "headless JSONL publisher does not expose a local publication pass",
            )
        return publish(
            state,
            context,
            authority_node_id=authority_node_id,
            group_id=group_id,
        )

    async def _publish_jsonl_once(
        self,
        state: RemotePairingState,
        *,
        authority_node_id: str,
        group_id: str,
    ) -> object:
        # The shared publisher performs bounded filesystem/SQLite work and its
        # synchronous runtime seam submits storage calls back to this relay
        # loop. Running it in a worker thread prevents blocking or self-deadlock.
        return await asyncio.to_thread(
            self._publish_jsonl_sync,
            state,
            authority_node_id=authority_node_id,
            group_id=group_id,
        )

    def snapshot(self) -> RecorderFederationSnapshot:
        with self._lock:
            return self._snapshot

    def wait_until_sharing_ready(
        self,
        *,
        timeout_seconds: float,
    ) -> RecorderFederationSnapshot:
        """Wait until the authenticated recorder publication path is usable.

        Joining a Federation and having a writable logical-storage route are
        separate states.  Launchers that promise data sharing use this bounded
        gate so they cannot report success while publication is still waiting
        for an authority or an explicit group choice.
        """

        if (
            not math.isfinite(timeout_seconds)
            or not 0 < timeout_seconds <= MAX_SHARING_READY_SECONDS
        ):
            raise ValueError(
                "sharing readiness timeout must be finite, positive, and at "
                f"most {MAX_SHARING_READY_SECONDS:g} seconds"
            )
        deadline = time.monotonic() + float(timeout_seconds)
        while True:
            if self._stop.is_set():
                raise FederationOperationError(
                    "recorder-publication-stopped",
                    "recorder publication stopped before data sharing became ready",
                )
            future = self._publication_future
            if future is None:
                raise FederationOperationError(
                    "recorder-publication-not-started",
                    "recorder publication was not started",
                )
            if future.done():
                if future.cancelled():
                    suffix = " (cancelled)"
                else:
                    failure = future.exception()
                    suffix = (
                        "" if failure is None else f" ({type(failure).__name__})"
                    )
                raise FederationOperationError(
                    "recorder-publication-stopped",
                    "recorder publication stopped before data sharing became ready"
                    + suffix,
                )
            snapshot = self.snapshot()
            # "degraded" is ready. Publication is working; some older evidence
            # was permanently withdrawn and is reported as such. Refusing to
            # report ready here would let one ancient poisoned row block
            # recorder startup forever -- reintroducing, at the readiness gate,
            # exactly the starvation the durable tombstone removed from the
            # delivery loop. The degraded state and its error code stay on the
            # returned snapshot, so the caller still sees it.
            if snapshot.storage_state in ("up-to-date", "degraded") or (
                snapshot.storage_state == "publishing"
                and snapshot.last_committed_count > 0
            ):
                if snapshot.jsonl_state != "ready":
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise FederationOperationError(
                            "recorder-sharing-not-ready",
                            "Federation recorder storage is ready, but local JSONL "
                            "publication did not become ready before the timeout "
                            f"(state: {snapshot.jsonl_state or 'unknown'})",
                        )
                    self._stop.wait(min(0.1, remaining))
                    continue
                if (
                    self._stop.is_set()
                    or self._publication_future is not future
                    or future.done()
                ):
                    raise FederationOperationError(
                        "recorder-publication-stopped",
                        "recorder publication changed state while data sharing "
                        "readiness was being checked",
                    )
                return snapshot
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                detail = sharing_state_detail(snapshot.storage_state)
                raise FederationOperationError(
                    "recorder-sharing-not-ready",
                    "Federation membership connected, but recorder data sharing "
                    f"did not become ready before the timeout (state: {detail})",
                )
            self._stop.wait(min(0.1, remaining))

    def _set_snapshot(self, **changes: Any) -> None:
        with self._lock:
            current = self._snapshot
            values = {
                "status": current.status,
                "node_id": current.node_id,
                "federation_id": current.federation_id,
                "session_id": current.session_id,
                "storage_state": current.storage_state,
                "storage_group": current.storage_group,
                "storage_authority_node_id": current.storage_authority_node_id,
                "pending_batches": current.pending_batches,
                "last_committed_count": current.last_committed_count,
                "jsonl_state": current.jsonl_state,
                "jsonl_last_published_count": (
                    current.jsonl_last_published_count
                ),
                "last_error_code": current.last_error_code,
            }
            values.update(changes)
            self._snapshot = RecorderFederationSnapshot(**values)

    def has_saved_membership(self) -> bool:
        return self.remote_store.load() is not None

    def bootstrap(self, pairing_key: str | None = None) -> RecorderFederationSnapshot:
        """Create identity and join/reconnect without persisting the pairing key."""

        credentials = self.service.create_identity()
        saved = self.remote_store.load()
        if saved is None:
            if not isinstance(pairing_key, str) or not pairing_key.strip():
                raise FederationOperationError(
                    "recorder-pairing-required",
                    "first Federation startup requires a short-lived FCP pairing key",
                    "pairing_key",
                )
            binding = self.service.redeem_pairing_code(pairing_key)
            saved = self.remote_store.load()
            if saved is None:
                raise FederationOperationError(
                    "recorder-pairing-state-missing",
                    "pairing completed without durable public-safe reconnect state",
                )
        else:
            if isinstance(pairing_key, str) and pairing_key.strip():
                offer = self.service.pairing_codec.decode(pairing_key)
                if offer.internal_session_id != saved.binding.internal_session_id:
                    raise FederationValidationError(
                        "recorder-already-paired",
                        "pairing_key",
                        "this recorder already has a different saved Federation membership",
                    )
            binding = self.service.reconnect()
            saved = self.remote_store.load() or saved

        if binding.device_id != credentials.identity.node_id:
            raise AuthenticationError(
                "recorder-pairing-identity-mismatch",
                "saved Federation membership belongs to another device identity",
                "device_id",
            )
        self.runtime.ensure_connected(saved)
        self._announce(saved)
        self._set_snapshot(
            status="connected",
            node_id=binding.device_id,
            federation_id=binding.federation_id,
            session_id=binding.internal_session_id,
            storage_state="discovering",
            jsonl_state="discovering",
            last_error_code=None,
        )
        self._start_publication(saved)
        return self.snapshot()

    def _announcement(self, state: RemotePairingState) -> CapabilityAnnouncement:
        return CapabilityAnnouncement(
            capability_id="recorder-local",
            node_id=state.binding.device_id,
            session_id=state.binding.internal_session_id,
            type="recorder",
            protocol="mtconnect",
            protocol_version="1",
            status=CapabilityStatus.READY,
            properties={
                "kind": "standalone-recorder",
                "source_count": len(self.source_names),
                "source_names": list(self.source_names),
                "dataset_schema": "fcp.mtconnect.observations.v1",
                "logical_storage": True,
            },
            announced_at=datetime.now(timezone.utc),
        )

    def _announce(self, state: RemotePairingState) -> None:
        self.runtime.announce_capability(
            state,
            self._announcement(state),
            request_id=f"recorder-capability-{uuid.uuid4().hex}",
        )

    async def _announce_connected(self, state: RemotePairingState) -> None:
        await self.runtime._announce_capability(
            state,
            self._announcement(state),
            request_id=f"recorder-capability-{uuid.uuid4().hex}",
        )

    def _start_publication(self, state: RemotePairingState) -> None:
        if self._publication_future is not None and not self._publication_future.done():
            return
        self._stop.clear()
        loop = self.runtime._start_loop()
        self._publication_future = asyncio.run_coroutine_threadsafe(
            self._publication_loop(state),
            loop,
        )

    def _worker(
        self,
        *,
        state: RemotePairingState,
        storage_client: RelayRecorderStorageClient,
        group_id: str,
    ) -> tuple[RecorderFederationDeliveryWorker, SQLiteOutbox]:
        publication_root = self.data_directory / "federation" / "recorder_publication"
        outbox = SQLiteOutbox(publication_root / "outbox.sqlite3")
        queue = DurableRecorderDeliveryQueue(
            outbox=outbox,
            client=storage_client,
            session_id=state.binding.internal_session_id,
            destination_id=group_id,
        )
        reconciler = RecorderArchiveReconciler(
            store=DurableRecorderStore(self.data_directory),
            checkpoint_file=(
                self.data_directory
                / "source_state"
                / "mtconnect_recorder_state.json"
            ),
            queue=queue,
            target=RecorderPublicationTarget(
                session_id=state.binding.internal_session_id,
                group_id=group_id,
                recorder_node_id=state.binding.device_id,
            ),
            max_content_bytes=RECORDER_RELAY_SAFE_CONTENT_BYTES,
        )
        return (
            RecorderFederationDeliveryWorker(
                reconciler=reconciler,
                queue=queue,
                poll_interval_seconds=self.publication_poll_seconds,
                delivery_limit=100,
            ),
            outbox,
        )

    async def _publication_loop(self, state: RemotePairingState) -> None:
        active_client_id: int | None = None
        storage_client: RelayRecorderStorageClient | None = None
        worker: RecorderFederationDeliveryWorker | None = None
        outbox: SQLiteOutbox | None = None
        authority_node_id: str | None = None
        group_id: str | None = None
        failures = 0
        try:
            while not self._stop.is_set():
                try:
                    await self.runtime._ensure_connected(state)
                    client = self.runtime._connected_client()
                    if active_client_id != id(client):
                        if storage_client is not None:
                            await storage_client.close()
                        storage_client = None
                        worker = None
                        outbox = None
                        authority_node_id = None
                        group_id = None
                        active_client_id = id(client)
                        await self._announce_connected(state)

                    status = await client.coordinator_status()
                    selected = select_storage_authority(
                        status,
                        session_id=state.binding.internal_session_id,
                        requested_group=self.requested_storage_group,
                    )
                    if (
                        selected.authority_node_id is None
                        or selected.group_id is None
                    ):
                        self._set_snapshot(
                            status="connected",
                            storage_state=selected.state,
                            jsonl_state=selected.state,
                            storage_group=selected.group_id,
                            storage_authority_node_id=selected.authority_node_id,
                            last_error_code=None,
                        )
                        failures = 0
                        await asyncio.sleep(self.publication_poll_seconds)
                        continue

                    if (
                        storage_client is None
                        or authority_node_id != selected.authority_node_id
                        or group_id != selected.group_id
                    ):
                        if storage_client is not None:
                            await storage_client.close()
                        # Build the route whole, then publish it. Every part of
                        # this can fail on an ordinary bad day -- `_worker()`
                        # opens the durable outbox, so a locked, full, readonly
                        # or malformed SQLite file raises right here -- and the
                        # retry that failure now gets is only a recovery if the
                        # next cycle rebuilds. Recording the authority and the
                        # client before the worker exists made the next cycle
                        # believe the route was already built, skip rebuilding
                        # it, and die on the assertion below with the store
                        # fault replaced by an AssertionError.
                        storage_client = None
                        worker = None
                        outbox = None
                        authority_node_id = None
                        group_id = None
                        candidate = RelayRecorderStorageClient(
                            client,
                            session_id=state.binding.internal_session_id,
                            authority_node_id=selected.authority_node_id,
                            request_timeout=self.request_timeout,
                        )
                        try:
                            await candidate.start()
                            worker, outbox = self._worker(
                                state=state,
                                storage_client=candidate,
                                group_id=selected.group_id,
                            )
                        except BaseException:
                            # Nothing owns this client yet, so the loop's own
                            # cleanup will not close it. A failure to close
                            # must not replace the fault being reported.
                            with suppress(*PUBLICATION_RETRY_ERRORS):
                                await candidate.close()
                            raise
                        storage_client = candidate
                        authority_node_id = selected.authority_node_id
                        group_id = selected.group_id

                    assert worker is not None and outbox is not None
                    cycle = await worker.run_cycle()
                    jsonl_result = await self._publish_jsonl_once(
                        state,
                        authority_node_id=authority_node_id,
                        group_id=group_id,
                    )
                    pending_snapshot = await asyncio.to_thread(outbox.pending)
                    storage_state, pending, delivery_error = (
                        _publication_cycle_status(
                            pending_entries=pending_snapshot,
                            session_id=state.binding.internal_session_id,
                            group_id=group_id,
                            delivery=cycle.delivery,
                            retired_total=cycle.retirement.total,
                        )
                    )
                    self._set_snapshot(
                        status="connected",
                        storage_state=storage_state,
                        storage_group=group_id,
                        storage_authority_node_id=authority_node_id,
                        pending_batches=pending,
                        last_committed_count=cycle.delivery.committed,
                        jsonl_state="ready",
                        jsonl_last_published_count=int(
                            getattr(jsonl_result, "published_chunks", 0)
                        ),
                        last_error_code=delivery_error,
                    )
                    failures = 0
                    await asyncio.sleep(self.publication_poll_seconds)
                except PUBLICATION_RETRY_ERRORS as exc:
                    failures += 1
                    # Re-reading the backlog is how the count stays durable
                    # truth, but it reads the very store the cycle just failed
                    # on. Whatever the cycle retried, this read can raise
                    # again, so it is guarded by the same contract rather than
                    # a narrower one -- and an unreadable backlog keeps the
                    # last count this loop actually proved rather than being
                    # answered with an invented zero.
                    pending_batches = self.snapshot().pending_batches
                    if outbox is not None:
                        try:
                            pending_snapshot = await asyncio.to_thread(
                                outbox.pending
                            )
                        except PUBLICATION_RETRY_ERRORS:
                            pass
                        else:
                            pending_batches = len(
                                _current_recorder_pending(
                                    pending_snapshot,
                                    session_id=(
                                        state.binding.internal_session_id
                                    ),
                                    group_id=group_id or "",
                                )
                            )
                    self._set_snapshot(
                        status="retrying",
                        storage_state="backlogged",
                        jsonl_state="backlogged",
                        pending_batches=pending_batches,
                        last_error_code=str(
                            getattr(exc, "code", type(exc).__name__)
                        ),
                    )
                    await asyncio.sleep(min(10.0, float(2 ** min(failures - 1, 3))))
        except asyncio.CancelledError:
            # Operator stop and process shutdown keep their existing meaning.
            raise
        except Exception as exc:
            # Nothing above reads this coroutine's future once the recorder is
            # running, so an unclassified fault used to end publication in
            # complete silence: no log, no snapshot change, no restart, and a
            # Federation health report frozen on the last successful cycle.
            # The fault still ends the loop -- it is unclassified, and inventing
            # a retry for it would be guessing -- but it can no longer be
            # mistaken for a working publisher.
            self._set_snapshot(
                status="failed",
                storage_state="publication-driver-failed",
                jsonl_state="publication-driver-failed",
                last_error_code=str(getattr(exc, "code", type(exc).__name__)),
            )
            raise
        finally:
            if storage_client is not None:
                await storage_client.close()

    def stop(self, *, timeout: float = 3.0) -> None:
        self._stop.set()
        future = self._publication_future
        self._publication_future = None
        if future is None:
            return
        try:
            future.result(timeout=timeout)
        except Exception:  # noqa: BLE001 - process shutdown is best effort
            future.cancel()


__all__ = [
    "SHARING_STATE_REMEDIES",
    "RecorderFederationNode",
    "RecorderFederationSnapshot",
    "StorageAuthoritySelection",
    "select_storage_authority",
    "sharing_state_detail",
]
