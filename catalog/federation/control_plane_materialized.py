"""Non-destructive materialized coordinator view for replicated C03 authority.

The replicated log remains authoritative. This SQLite view exists solely for
compatibility with established relay/product readers. Materialization updates
only C03-owned durable authority and never truncates unrelated session history.
The internal bootstrap readiness seal remains private to the control-plane state
machine and is deliberately excluded from normal product capability surfaces.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .control_plane_product import (
    REPLICATED_COORDINATOR_ID,
    ReplicatedFederationRuntime,
    _event_time,
    _stamp,
)
from .control_plane_readiness import BOOTSTRAP_SEAL_CAPABILITY_ID
from .control_plane_replication import ControlPlaneError
from .session_leadership import LEADERSHIP_SCHEMA


def _canonical(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _event_digest(session_id: str, term: int, leader_id: str) -> str:
    return hashlib.sha256(
        f"c03-leadership:{session_id}:{term}:{leader_id}".encode()
    ).hexdigest()


class MaterializedReplicatedFederationRuntime(ReplicatedFederationRuntime):
    """Replicated runtime whose local coordinator projection is non-destructive."""

    def materialize(self) -> None:
        state = self.node.state
        if state.get("federation_id") is None:
            return
        now_text = _stamp(self.clock())
        with self.local.store.transaction() as database:
            # Identity and revocation -------------------------------------------------
            for node_id, node in sorted(state["nodes"].items()):
                existing = database.execute(
                    "SELECT public_key FROM nodes WHERE node_id=?", (node_id,)
                ).fetchone()
                if existing is not None and existing["public_key"] != node["public_key"]:
                    raise ControlPlaneError(
                        "local coordinator node identity conflicts with replicated authority"
                    )
                database.execute(
                    """
                    INSERT OR IGNORE INTO nodes(
                        node_id,display_name,public_key,created_at,
                        identity_version,enrolled_at
                    ) VALUES(?,?,?,?,1,?)
                    """,
                    (node_id, node["display_name"], node["public_key"], now_text, now_text),
                )
                database.execute(
                    "UPDATE nodes SET display_name=?,public_key=? WHERE node_id=?",
                    (node["display_name"], node["public_key"], node_id),
                )
                database.execute(
                    "INSERT OR IGNORE INTO node_connectivity(node_id,state) VALUES(?,'disconnected')",
                    (node_id,),
                )
            for node_id, revocation in sorted(state["revocations"].items()):
                database.execute(
                    """
                    UPDATE nodes SET revoked_at=COALESCE(revoked_at,?),
                        revoked_by=?,revocation_reason=? WHERE node_id=?
                    """,
                    (
                        _event_time(revocation.get("occurred_at"), now_text),
                        REPLICATED_COORDINATOR_ID,
                        str(revocation.get("reason") or "replicated-revocation")[:512],
                        node_id,
                    ),
                )
                database.execute(
                    "UPDATE node_connectivity SET state='revoked' WHERE node_id=?",
                    (node_id,),
                )

            # Sessions and membership -----------------------------------------------
            for session_id, session in sorted(state["sessions"].items()):
                database.execute(
                    """
                    INSERT OR IGNORE INTO sessions(
                        session_id,display_name,state,revision,created_at,
                        created_by_node_id,coordinator_id
                    ) VALUES(?,?,'active',0,?,?,?)
                    """,
                    (
                        session_id,
                        session["display_name"],
                        now_text,
                        session["creator_node_id"],
                        REPLICATED_COORDINATOR_ID,
                    ),
                )
                # Never rewrite revision here. It is the revision of the complete
                # product event stream, not the smaller C03 authority event list.
                database.execute(
                    """
                    UPDATE sessions SET display_name=?,state='active',
                        created_by_node_id=?,coordinator_id=?
                    WHERE session_id=?
                    """,
                    (
                        session["display_name"],
                        session["creator_node_id"],
                        REPLICATED_COORDINATOR_ID,
                        session_id,
                    ),
                )
                members = state["memberships"].get(session_id, {})
                for node_id, active in sorted(members.items()):
                    database.execute(
                        """
                        INSERT OR IGNORE INTO session_memberships(
                            session_id,node_id,joined_at
                        ) VALUES(?,?,?)
                        """,
                        (session_id, node_id, now_text),
                    )
                    if active:
                        database.execute(
                            """
                            UPDATE session_memberships SET removed_at=NULL,
                                removed_by=NULL,removal_reason=NULL
                            WHERE session_id=? AND node_id=?
                            """,
                            (session_id, node_id),
                        )
                    else:
                        database.execute(
                            """
                            UPDATE session_memberships
                            SET removed_at=COALESCE(removed_at,?),removed_by=?,
                                removal_reason=COALESCE(removal_reason,'replicated-removal')
                            WHERE session_id=? AND node_id=?
                            """,
                            (
                                now_text,
                                REPLICATED_COORDINATOR_ID,
                                session_id,
                                node_id,
                            ),
                        )

                # Leadership is the one C03 public event existing product code
                # consumes as an authorization proof. Append exactly one event
                # for every replicated monotonic term, leaving unrelated event
                # types and revisions untouched.
                authority = state["leaders"].get(session_id)
                if authority is not None:
                    target_term = int(authority["term"])
                    target_leader = str(authority["leader_node_id"])
                    creator = str(authority["creator_node_id"])
                    c03_events = state["session_events"].get(session_id, [])
                    c03_transitions: dict[int, dict[str, Any]] = {}
                    for event in c03_events:
                        if event.get("event_type") != "session.leader.changed":
                            continue
                        payload = event.get("payload")
                        if not isinstance(payload, dict):
                            raise ControlPlaneError(
                                "replicated leadership event payload is malformed"
                            )
                        term = payload.get("term")
                        if isinstance(term, bool) or not isinstance(term, int) or term < 2:
                            raise ControlPlaneError(
                                "replicated leadership term is malformed"
                            )
                        if term in c03_transitions:
                            raise ControlPlaneError(
                                "replicated leadership term appears more than once"
                            )
                        c03_transitions[term] = {
                            "event": event,
                            "payload": payload,
                        }

                    expected_leader = creator
                    for term in range(2, target_term + 1):
                        transition = c03_transitions.get(term)
                        if transition is None:
                            raise ControlPlaneError(
                                "replicated leadership history has a term gap"
                            )
                        source = transition["payload"]
                        previous = str(source.get("previous_leader_node_id", ""))
                        leader_id = str(source.get("leader_node_id", ""))
                        if previous != expected_leader or not leader_id:
                            raise ControlPlaneError(
                                "replicated leadership history is not contiguous"
                            )
                        expected_leader = leader_id

                        digest = _event_digest(session_id, term, leader_id)
                        existing = database.execute(
                            "SELECT 1 FROM session_events WHERE event_id=?",
                            (f"c03-{digest[:32]}",),
                        ).fetchone()
                        if existing is not None:
                            continue
                        revision = int(
                            database.execute(
                                "SELECT revision FROM sessions WHERE session_id=?",
                                (session_id,),
                            ).fetchone()[0]
                        ) + 1
                        payload = {
                            "schema": LEADERSHIP_SCHEMA,
                            "session_id": session_id,
                            "previous_leader_node_id": previous,
                            "leader_node_id": leader_id,
                            "term": term,
                            "reason": str(
                                source.get("reason")
                                or "replicated-authority-materialization"
                            )[:128],
                        }
                        payload_json = _canonical(payload)
                        event_value = transition["event"]
                        database.execute(
                            """
                            INSERT INTO session_events(
                                session_id,revision,event_id,request_id,event_type,
                                occurred_at,actor_node_id,payload_json,content_hash
                            ) VALUES(?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                session_id,
                                revision,
                                f"c03-{digest[:32]}",
                                f"sha256:{digest}",
                                "session.leader.changed",
                                _event_time(event_value.get("occurred_at"), now_text),
                                REPLICATED_COORDINATOR_ID,
                                payload_json,
                                f"sha256:{hashlib.sha256(payload_json.encode('utf-8')).hexdigest()}",
                            ),
                        )
                        database.execute(
                            "UPDATE sessions SET revision=? WHERE session_id=?",
                            (revision, session_id),
                        )
                    if expected_leader != target_leader:
                        raise ControlPlaneError(
                            "replicated leadership projection disagrees with current leader"
                        )

                # Capability authority ------------------------------------------------
                # The readiness seal is an internal durability marker, not an
                # end-user/product capability. Never surface or persist it in the
                # ordinary capability table.
                authoritative_caps = {
                    capability_id: capability
                    for capability_id, capability in state["capabilities"].get(
                        session_id, {}
                    ).items()
                    if capability_id != BOOTSTRAP_SEAL_CAPABILITY_ID
                }
                rows = database.execute(
                    "SELECT * FROM capabilities WHERE session_id=?", (session_id,)
                ).fetchall()
                existing_caps: dict[str, Any] = {
                    str(row["capability_id"]): row for row in rows
                }
                # Remove a seal persisted by an earlier development revision.
                database.execute(
                    "DELETE FROM capabilities WHERE session_id=? AND capability_id=?",
                    (session_id, BOOTSTRAP_SEAL_CAPABILITY_ID),
                )
                for capability_id, capability in sorted(authoritative_caps.items()):
                    row = existing_caps.get(capability_id)
                    database.execute(
                        """
                        INSERT INTO capabilities(
                            session_id,capability_id,node_id,type,protocol,
                            protocol_version,status,properties_json,
                            announced_at,last_heartbeat_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(session_id,capability_id) DO UPDATE SET
                            node_id=excluded.node_id,type=excluded.type
                        """,
                        (
                            session_id,
                            capability_id,
                            capability["owner_node_id"],
                            capability["capability_type"],
                            str(row["protocol"]) if row is not None else "replicated-authority",
                            str(row["protocol_version"]) if row is not None else "1",
                            str(row["status"]) if row is not None else "ready",
                            str(row["properties_json"]) if row is not None else "{}",
                            str(row["announced_at"]) if row is not None else now_text,
                            now_text,
                        ),
                    )
                # Do not delete pre-existing rows on first C03 materialization.
                # A migration may contain valid capability rows announced before
                # replicated authority was enabled.


__all__ = ["MaterializedReplicatedFederationRuntime"]