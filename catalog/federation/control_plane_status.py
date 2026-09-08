"""Public-safe status projection for Federation v1 replicated authority.

This file contains no endpoint, credential, key or local-path material.  It is
intended for physical acceptance and operator diagnostics: a host can prove the
same Federation survived leader loss, show the monotonic consensus term and
fencing epoch, and distinguish immutable creator provenance from the current
operational leader.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .control_plane_replication import ReplicaNode

CONTROL_PLANE_STATUS_SCHEMA = "fcp.control-plane.status.v1"
CONTROL_PLANE_STATUS_FILE = "control-plane-status.json"


def status_document(node: ReplicaNode, *, ready: bool, voter_only: bool) -> dict[str, Any]:
    state = node.state
    sessions: list[dict[str, Any]] = []
    for session_id, session in sorted(state.get("sessions", {}).items()):
        leadership = state.get("leaders", {}).get(session_id)
        if not isinstance(session, dict) or not isinstance(leadership, dict):
            continue
        sessions.append(
            {
                "session_id": session_id,
                "creator_node_id": session.get("creator_node_id"),
                "leader_node_id": leadership.get("leader_node_id"),
                "leadership_term": leadership.get("term"),
            }
        )
    return {
        "schema": CONTROL_PLANE_STATUS_SCHEMA,
        "cluster_id": node.configuration.cluster_id,
        "voter_id": node.voter_id,
        "role": node.role,
        "consensus_leader_id": node.leader_id,
        "consensus_term": node.store.current_term,
        "fencing_epoch": node.store.fencing_epoch,
        "commit_index": node.store.commit_index,
        "last_applied": node.store.last_applied,
        "federation_id": state.get("federation_id"),
        "ready": bool(ready),
        "voter_only": bool(voter_only),
        "sessions": sessions,
    }


def write_status(
    path: Path | str,
    node: ReplicaNode,
    *,
    ready: bool,
    voter_only: bool,
) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(
        status_document(node, ready=ready, voter_only=voter_only),
        sort_keys=True,
        separators=(",", ":"),
    )
    temporary = target.with_name(target.name + ".tmp")
    temporary.write_text(encoded + "\n", encoding="utf-8")
    os.replace(temporary, target)


__all__ = [
    "CONTROL_PLANE_STATUS_FILE",
    "CONTROL_PLANE_STATUS_SCHEMA",
    "status_document",
    "write_status",
]
