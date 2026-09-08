"""Minimal native C03 voter for the protected MSH Recorder host.

The Recorder contributes quorum durability without becoming an operational
Federation coordinator.  This process accepts authenticated/encrypted vote,
AppendEntries, snapshot and private credential-replica RPCs, but it never starts
an election, never opens the product relay, and never materializes coordinator
state.  All writable state is required to live outside the operator-declared
protected recorder data directory.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
from pathlib import Path
from typing import Any

from catalog.node.identity import IdentityStore

from .control_plane_credentials import (
    CredentialReplicaServer,
    CredentialSnapshotStore,
)
from .control_plane_product import (
    ReplicatedControlPlaneDeployment,
    _secret_file,
)
from .control_plane_replication import (
    ControlPlaneError,
    PersistentReplicaStore,
    ReplicaNode,
)
from .control_plane_transport import (
    PersistentReplayGuard,
    SecureEnvelopeCodec,
    SecureReplicationServer,
    VoterIdentityRegistry,
)

STATUS_SCHEMA = "fcp.control-plane.recorder-voter-status.v1"
STATUS_FILE = "recorder-voter-status.json"


def _resolved(path: Path | str) -> Path:
    return Path(path).expanduser().resolve(strict=False)


def _inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def validate_recorder_voter_isolation(
    deployment: ReplicatedControlPlaneDeployment,
    protected_record_data: Path | str,
) -> None:
    """Fail closed if any C03 path overlaps protected recorder data."""

    protected = _resolved(protected_record_data)
    candidates = {
        "identity_directory": _resolved(deployment.identity_directory),
        "replica_database": _resolved(deployment.replica_database),
        "replay_database": _resolved(deployment.replay_database),
        "coordinator_database": _resolved(deployment.coordinator_database),
        "transport_secret_file": _resolved(deployment.transport_secret_file),
        "credential_store": _resolved(
            deployment.replica_database.with_name("human_credentials_replica.sqlite3")
        ),
        "status_file": _resolved(deployment.replica_database.parent / STATUS_FILE),
    }
    for name, candidate in candidates.items():
        # Reject either direction: a writable control-plane directory must not
        # contain protected recorder data, and protected data must not contain
        # any control-plane file/directory.
        if _inside(candidate, protected) or _inside(protected, candidate):
            raise ControlPlaneError(
                f"Recorder C03 {name} overlaps protected record data"
            )


class RecorderControlPlaneVoter:
    """Voter-only authenticated C03 endpoint for a native Recorder host."""

    def __init__(
        self,
        deployment: ReplicatedControlPlaneDeployment,
        *,
        protected_record_data: Path | str,
        status_interval_seconds: float = 1.0,
    ) -> None:
        if status_interval_seconds <= 0:
            raise ControlPlaneError("Recorder voter status interval must be positive")
        validate_recorder_voter_isolation(deployment, protected_record_data)
        self.deployment = deployment
        self.protected_record_data = _resolved(protected_record_data)
        self.status_interval_seconds = float(status_interval_seconds)

        identity_store = IdentityStore(
            deployment.identity_directory,
            display_name=deployment.local_display_name,
        )
        if (
            not identity_store.private_key_path.exists()
            or not identity_store.public_identity_path.exists()
        ):
            raise ControlPlaneError(
                "Recorder voter identity must already be provisioned"
            )
        credentials = identity_store.load_or_create()
        if credentials.identity.node_id != deployment.local_voter_id:
            raise ControlPlaneError(
                "Recorder voter identity does not match configured voter ID"
            )

        configuration = deployment.configuration
        registry = VoterIdentityRegistry(
            configuration,
            {peer.voter_id: peer.public_key for peer in deployment.peers},
        )
        secret = _secret_file(deployment.transport_secret_file)
        self.node = ReplicaNode(
            deployment.local_voter_id,
            PersistentReplicaStore(deployment.replica_database, configuration),
        )
        self.codec = SecureEnvelopeCodec(
            credentials,
            registry,
            secret,
            PersistentReplayGuard(deployment.replay_database),
        )
        self.server = SecureReplicationServer(
            self.node,
            self.codec,
            host=deployment.listen_host,
            port=deployment.listen_port,
        )
        if deployment.listen_port >= 65535:
            raise ControlPlaneError(
                "Recorder voter listen port leaves no credential replica port"
            )
        self.credential_store = CredentialSnapshotStore(
            deployment.replica_database.with_name("human_credentials_replica.sqlite3")
        )
        self.credential_server = CredentialReplicaServer(
            self.node,
            self.codec,
            self.credential_store,
            host=deployment.listen_host,
            port=deployment.listen_port + 1,
        )
        self.status_path = deployment.replica_database.parent / STATUS_FILE
        self._stop = threading.Event()
        self._status_thread: threading.Thread | None = None

    def _status(self) -> dict[str, Any]:
        state = self.node.state
        return {
            "schema": STATUS_SCHEMA,
            "cluster_id": self.node.configuration.cluster_id,
            "voter_id": self.node.voter_id,
            "role": self.node.role,
            "leader_id": self.node.leader_id,
            "term": self.node.store.current_term,
            "commit_index": self.node.store.commit_index,
            "last_applied": self.node.store.last_applied,
            "federation_id": state.get("federation_id"),
            "voter_only": True,
        }

    def _write_status(self) -> None:
        self.status_path.parent.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(
            self._status(), sort_keys=True, separators=(",", ":")
        )
        temporary = self.status_path.with_suffix(".tmp")
        temporary.write_text(encoded + "\n", encoding="utf-8")
        os.replace(temporary, self.status_path)

    def _status_loop(self) -> None:
        while not self._stop.wait(self.status_interval_seconds):
            try:
                self.node.apply_committed()
                self._write_status()
            except Exception as exc:  # noqa: BLE001 - status cannot widen voter authority
                logging.getLogger(__name__).warning(
                    "Recorder voter status publication failed (%s)", type(exc).__name__
                )

    def start(self) -> None:
        self.server.start()
        self.credential_server.start()
        self.node.apply_committed()
        self._write_status()
        if self._status_thread is not None and self._status_thread.is_alive():
            return
        self._stop.clear()
        self._status_thread = threading.Thread(
            target=self._status_loop,
            name="fcp-recorder-control-plane-voter",
            daemon=True,
        )
        self._status_thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._status_thread is not None:
            self._status_thread.join(timeout=max(3.0, self.status_interval_seconds * 3))
            self._status_thread = None
        self.credential_server.close()
        self.server.close()

    def serve_forever(self) -> None:
        self.start()
        try:
            while not self._stop.wait(1.0):
                pass
        finally:
            self.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m catalog.federation.recorder_control_plane_voter",
        description="Native voter-only C03 service for the MSH Recorder host",
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--protected-record-data", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        deployment = ReplicatedControlPlaneDeployment.from_file(args.config)
        voter = RecorderControlPlaneVoter(
            deployment,
            protected_record_data=args.protected_record_data,
        )
    except (ControlPlaneError, OSError, ValueError) as exc:
        print(
            f"Recorder control-plane voter configuration failed ({type(exc).__name__})",
            file=sys.stderr,
        )
        return 2

    stop = threading.Event()

    def request_stop(_signum: int, _frame: object) -> None:
        stop.set()

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(signum, request_stop)
        except (ValueError, OSError):
            pass

    voter.start()
    try:
        while not stop.wait(1.0):
            pass
    finally:
        voter.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "RecorderControlPlaneVoter",
    "validate_recorder_voter_isolation",
]
