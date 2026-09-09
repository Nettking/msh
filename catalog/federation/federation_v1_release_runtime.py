"""Release composition for the Federation v1 replicated runtime.

The implementation is kept separate from the reviewed consensus core. This
last composition point makes automatic legacy recovery use the same bounded
authenticated quorum acquisition as explicit Federation creation and places the
private credential-generation marker beside the replicated auth database so the
Flask process can observe an atomic restore across the container boundary.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Mapping
from typing import Any

from .control_plane_bootstrap_proposal import propose_bootstrap_command
from .control_plane_journal import (
    PRODUCT_JOURNAL_INITIALIZE,
    journal_prefix_digest,
    public_row_content_hash,
)
from .control_plane_journal_projection import (
    JournalSessionLeadershipService,
    project_product_journal,
)
from .control_plane_journal_runtime import ProductJournalController, public_rows
from .control_plane_journal_store import JournalCoordinatorStore
from .control_plane_legacy_migration import (
    LegacyMigrationError,
    _canonical,
    _read_event_journal,
)
from .control_plane_product import REPLICATED_COORDINATOR_ID
from .control_plane_readiness import authority_ready
from .control_plane_replication import AuthorityCommand, ControlPlaneError, ReplicaNode
from .control_plane_runtime import AUTH_GENERATION_FILE
from .coordinator import SessionCoordinator
from .federation_v1_runtime import FederationV1Runtime
from .persistence import _parse_time, _time


class FederationV1ReleaseRuntime(FederationV1Runtime):
    """Exact runtime used by the product relay and physical-readiness tests."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._canonical_product_journal = True
        self._journal_manifest = None
        self.local = SessionCoordinator(JournalCoordinatorStore(
            self.deployment.coordinator_database, coordinator_id=REPLICATED_COORDINATOR_ID,
        ), clock=self.clock)
        self.journal = ProductJournalController(self)
        self.local.leadership = JournalSessionLeadershipService(self.local.store, self)
        with self._lifecycle_lock:
            self.local.store.attach_controller(self.journal)
        if self.human_auth_database is not None:
            self._auth_generation_path = (
                self.human_auth_database.parent / AUTH_GENERATION_FILE
            )

    @property
    def ready(self) -> bool:
        state = self.node.state
        sessions = state.get("product_journal", {}).get("sessions", {})
        return authority_ready(state) and all(session in sessions for session in state["sessions"])

    def materialize(self) -> None:
        if not hasattr(self, "journal"):
            return super().materialize()
        with self._lifecycle_lock, self.local.store.projection() as database:
            state = self.node.state
            super().materialize(state=state)
            project_product_journal(self, database, state)
            self.journal.private.apply(
                database, state.get("product_journal", {}).get("private_rows", {}),
                complete=bool(state.get("product_journal", {}).get("sessions")),
            )

    def propose(self, command: AuthorityCommand) -> None:
        if self.journal.active:
            self.journal.queue(command)
        else:
            with self._lifecycle_lock:
                super().propose(command)

    def _drive_lifecycle_round(self) -> None:
        if not self.ready and self._fresh_genesis() is not None:
            if self.node.role == ReplicaNode.LEADER or (
                time.monotonic() - self.node.last_leader_contact > self.election_timeout_seconds
                and time.monotonic() >= self._next_election_at
            ):
                self._resume_fresh_bootstrap()
            return
        if authority_ready(self.node.state) and not self.ready:
            self._resume_witnessed_bootstrap()
            return
        super()._drive_lifecycle_round()

    def _fresh_genesis(self):
        return next((entry.command for entry in self.node.store.entries()
                     if entry.log_index <= self.node.store.commit_index
                     and entry.command.command_type == "FEDERATION_GENESIS"
                     and entry.command.command_id.startswith("genesis-")), None)

    def _propose_bootstrap_command(self, command):
        return propose_bootstrap_command(self.node, self.transport, command)

    def _bootstrap_new_federation(self, **kwargs) -> None:
        if self.node.state.get("federation_id") is None:
            return super()._bootstrap_new_federation(**kwargs)
        genesis = self._fresh_genesis()
        if genesis is None or any(genesis.payload.get(key) != value for key, value in kwargs.items()):
            raise ControlPlaneError("bootstrap retry conflicts with committed fresh Federation identity")
        self._resume_fresh_bootstrap()

    def _resume_fresh_bootstrap(self) -> None:
        genesis = self._fresh_genesis()
        if genesis is None:
            raise ControlPlaneError("fresh bootstrap recovery lacks committed genesis provenance")
        if self.ready:
            self.materialize()
            return
        self._acquire_bootstrap_leadership()
        self._propose_bootstrap_command(genesis)
        source = genesis.payload
        self.materialize()
        self._seal_authority(
            federation_id=source["federation_id"], session_id=source["session_id"],
            creator_node_id=source["creator_node_id"], occurred_at=source["occurred_at"],
        )
        self.node.synchronize(self.transport)
        self.materialize()
        if not self.ready:
            raise ControlPlaneError("fresh bootstrap recovery is not fully committed")

    def _initialize_public_journal(self, session_id: str, rows: list[dict[str, Any]], *, provenance=None) -> None:
        existing = self.node.state.get("product_journal", {}).get("sessions", {}).get(session_id)
        if existing is not None:
            if existing["rows"][:len(rows)] != rows:
                raise LegacyMigrationError("committed public journal conflicts with bootstrap evidence")
            return
        digest = journal_prefix_digest(rows)
        chunks: list[list[dict[str, Any]]] = [[]]
        size = 0
        for row in rows:
            encoded_size = len(_canonical(row))
            if chunks[-1] and (size + encoded_size > 64 * 1024 or len(chunks[-1]) >= 128):
                chunks.append([])
                size = 0
            chunks[-1].append(row)
            size += encoded_size
        for index, chunk in enumerate(chunks):
            payload = {"session_id": session_id, "expected_revision": len(rows),
                       "prefix_digest": digest, "public_rows": chunk, "final": index == len(chunks) - 1}
            if provenance is not None:
                payload["provenance"] = provenance
            command = AuthorityCommand(
                command_id=f"journal-initialize-{hashlib.sha256(session_id.encode()).hexdigest()}-{digest[7:]}-{index}",
                command_type=PRODUCT_JOURNAL_INITIALIZE,
                cluster_id=self.node.configuration.cluster_id, issued_by=self.node.voter_id,
                payload=payload,
            )
            self._propose_bootstrap_command(command)
        self.materialize()

    def _bootstrap_from_witness_manifest(self, manifest):
        self._journal_manifest = manifest
        try:
            return super()._bootstrap_from_witness_manifest(manifest)
        finally:
            self._journal_manifest = None

    def _initialize_witnessed_journal(self, manifest: Mapping[str, Any]) -> None:
        session_id = str(manifest["session_id"])
        events = _read_event_journal(self.legacy_node_state_database, session_id)
        if ("sha256:" + hashlib.sha256(_canonical([event.to_dict() for event in events])).hexdigest()
                != manifest.get("history_digest") or events[-1].revision != manifest.get("last_revision")):
            raise LegacyMigrationError("legacy public journal changed after quorum witnessing")
        rows = []
        for event in events:
            payload_json = _canonical(event.payload).decode("utf-8")
            row = {
                "session_id": session_id, "revision": event.revision, "event_id": event.event_id,
                "event_type": event.event_type, "occurred_at": _time(event.occurred_at),
                "actor_node_id": event.actor_node_id, "payload_json": payload_json,
                # A member's historical wire journal has no private request ID.
                # Every voter must derive the same disjoint import identity.
                "request_id": "sha256:" + hashlib.sha256(("witnessed-event:" + event.event_id).encode()).hexdigest(),
                "content_hash": public_row_content_hash(event.event_type, payload_json),
            }
            rows.append(row)
        # Witnesses prove the complete legacy prefix. Any later migration
        # leadership change is separately proved by its actual committed C03
        # command; it is not reconstructed from the narrow authority event list.
        legacy_term = int(manifest["legacy_leadership_term"])
        transitions = sorted((entry for entry in self.node.store.entries()
                              if entry.log_index <= self.node.store.commit_index
                              and entry.command.command_type == "LEADER_TRANSITION"
                              and entry.command.payload["session_id"] == session_id
                              and entry.command.payload["term"] > legacy_term),
                             key=lambda entry: entry.command.payload["term"])
        for entry in transitions:
            command = entry.command
            payload = {key: command.payload[key] for key in (
                "session_id", "previous_leader_node_id", "leader_node_id", "term", "reason",
            )}
            payload["schema"] = "fcp.session-leadership.v1"
            payload_json = _canonical(payload).decode("utf-8")
            digest = hashlib.sha256((command.command_id + "\0" + command.content_hash).encode()).hexdigest()
            rows.append({
                "session_id": session_id, "revision": len(rows) + 1,
                "event_id": f"c03-migration-{digest[:32]}", "request_id": f"sha256:{digest}",
                "event_type": "session.leader.changed", "actor_node_id": REPLICATED_COORDINATOR_ID,
                "occurred_at": _time(_parse_time(command.payload["occurred_at"])),
                "payload_json": payload_json, "content_hash": public_row_content_hash("session.leader.changed", payload_json),
            })
        self._initialize_public_journal(session_id, rows, provenance={
            "kind": "witnessed", "source_revision": len(events), "history_digest": manifest["history_digest"],
        })

    def _before_witnessed_migration_promotion(self, manifest: Mapping[str, Any]) -> None:
        session_id = str(manifest["session_id"])
        if session_id in self.node.state.get("product_journal", {}).get("initializing", {}):
            # Source and prior authority commands have been revalidated. Finish
            # their exact inactive prefix before a new leader transition can
            # append to it; never change the core initialization fence.
            self._initialize_witnessed_journal(manifest)

    def _seal_authority(self, **kwargs) -> None:
        session_id = kwargs["session_id"]
        if session_id not in self.node.state.get("product_journal", {}).get("sessions", {}):
            if self._journal_manifest is not None:
                self._initialize_witnessed_journal(self._journal_manifest)
                self._promote_bootstrap_session(session_id, kwargs["occurred_at"])
                return self._commit_readiness_seal(**kwargs)
            with self.local.store.read_transaction() as database:
                rows = [row for row in public_rows(database) if row["session_id"] == session_id]
            if rows:
                raise LegacyMigrationError("existing public history requires quorum-witnessed initialization")
            genesis = next((entry for entry in self.node.store.entries()
                            if entry.command.command_type == "FEDERATION_GENESIS"
                            and entry.command.payload["session_id"] == session_id), None)
            if genesis is None or genesis.log_index > self.node.store.commit_index:
                raise ControlPlaneError("fresh public journal requires the committed genesis command")
            source = genesis.command.payload
            occurred_at = _time(_parse_time(source["occurred_at"]))
            events = [(source["creator_node_id"], "session.created", {
                "session_id": session_id, "display_name": source["display_name"],
            })]
            events.extend((node_id, "node.joined", {"node_id": node_id})
                          for node_id in sorted(source["members"]))
            for revision, (actor, event_type, payload) in enumerate(events, 1):
                digest = hashlib.sha256(f"{genesis.command.content_hash}:{revision}".encode()).hexdigest()
                payload_json = _canonical(payload).decode("utf-8")
                rows.append({
                    "session_id": session_id, "revision": revision, "event_id": f"c03-genesis-{digest[:32]}",
                    "request_id": f"sha256:{digest}", "event_type": event_type,
                    "occurred_at": occurred_at, "actor_node_id": actor, "payload_json": payload_json,
                    "content_hash": public_row_content_hash(event_type, payload_json),
                })
            self._initialize_public_journal(session_id, rows)
        # The seal requires the operational voter leader. On a resumed fresh
        # bootstrap, first commit the surviving quorum's leader transition with
        # its canonical public row, retaining the immutable genesis creator.
        self._promote_bootstrap_session(session_id, kwargs["occurred_at"])
        self._commit_readiness_seal(**kwargs)

    def _promote_bootstrap_session(self, session_id: str, occurred_at: str) -> None:
        # Normal lifecycle promotions deliberately require an existing seal.
        # This narrower path runs only after validating fresh/witnessed source
        # and committing its complete public prefix. It must itself prove a
        # live quorum before the surviving voter can seal readiness.
        state = self.node.state
        if session_id not in state.get("product_journal", {}).get("sessions", {}):
            raise ControlPlaneError("bootstrap promotion requires the complete committed public journal")
        leadership = state["leaders"][session_id]
        if leadership["leader_node_id"] == self.node.voter_id:
            return
        command_id = f"bootstrap-recover-leader-{session_id}-{int(leadership['term']) + 1}-{self.node.voter_id}"
        pending = self.node.store.entry_for_command(command_id)
        command = AuthorityCommand(
            command_id=command_id, command_type="LEADER_TRANSITION",
            cluster_id=self.node.configuration.cluster_id, issued_by=self.node.voter_id,
            payload={"session_id": session_id,
                     "previous_leader_node_id": leadership["leader_node_id"],
                     "leader_node_id": self.node.voter_id, "term": int(leadership["term"]) + 1,
                     "occurred_at": pending.command.payload["occurred_at"] if pending else occurred_at,
                     "reason": "replicated-quorum-bootstrap-recovery"},
        )
        self._propose_bootstrap_command(command)
        if self.node.state["leaders"][session_id]["leader_node_id"] != self.node.voter_id:
            raise ControlPlaneError("bootstrap session leadership did not commit")
        self.materialize()

    def _commit_readiness_seal(self, *, federation_id, session_id, creator_node_id, occurred_at):
        del creator_node_id  # The seal proves readiness, never historical authorship.
        command_id = "bootstrap-seal-" + hashlib.sha256(f"{federation_id}:{session_id}".encode()).hexdigest()
        existing = self.node.store.entry_for_command(command_id)
        if existing is not None:
            command = existing.command
            if (command.command_type != "CAPABILITY_DECLARE"
                    or command.cluster_id != self.node.configuration.cluster_id
                    or command.payload.get("session_id") != session_id
                    or command.payload.get("capability_id") != "fcp.control-plane.bootstrap-complete"
                    or command.payload.get("capability_type") != "fcp-control-plane-bootstrap-seal"
                    or command.issued_by != command.payload.get("owner_node_id")
                    or command.issued_by not in self.node.configuration.voter_ids):
                raise ControlPlaneError("retained bootstrap seal conflicts with readiness provenance")
        else:
            command = AuthorityCommand(
                command_id=command_id, command_type="CAPABILITY_DECLARE",
                cluster_id=self.node.configuration.cluster_id, issued_by=self.node.voter_id,
                payload={"session_id": session_id, "capability_id": "fcp.control-plane.bootstrap-complete",
                         "capability_type": "fcp-control-plane-bootstrap-seal", "owner_node_id": self.node.voter_id,
                         "occurred_at": occurred_at},
            )
        self._propose_bootstrap_command(command)

    def _attempt_existing_federation_bootstrap(self) -> None:
        with self._lifecycle_lock:
            self._resume_witnessed_bootstrap()

    def _resume_witnessed_bootstrap(self) -> None:
        if self.bootstrap_federation_id is None or self.bootstrap_session_id is None:
            return
        existing_id = self.node.state.get("federation_id")
        if existing_id is not None and existing_id != self.bootstrap_federation_id:
            raise LegacyMigrationError(
                "configured migration Federation ID conflicts with replicated authority"
            )
        if self.ready:
            return

        # A transient voter-socket startup race is not authority. Every retry
        # is a normal authenticated election and must independently prove 2/3.
        self._acquire_bootstrap_leadership()
        manifest = self._matching_witness_quorum(
            self.bootstrap_federation_id,
            self.bootstrap_session_id,
        )
        self._bootstrap_from_witness_manifest(manifest)
        matched = self.node.synchronize(self.transport)
        if matched + 1 < self.node.quorum:
            raise ControlPlaneError(
                "witnessed migration lost voter quorum before materialization"
            )
        self.materialize()
        self._restore_human_credentials_if_available()
        self._sync_human_credentials_if_due(force=True)


__all__ = ["FederationV1ReleaseRuntime"]
