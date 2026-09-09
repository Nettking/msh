"""Preserve committed public history and project complete product metadata.

A certified witnessed prefix may replace its two local-only request/hash index
columns with canonical values. Its seven public wire fields never change.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from .control_plane_journal import (
    LEGACY_COORDINATOR_ID,
    REPLICATED_COORDINATOR_ID,
    journal_session,
    public_row_content_hash,
    validate_product_journal_state,
)
from .control_plane_readiness import BOOTSTRAP_SEAL_CAPABILITY_ID
from .control_plane_replication import ControlPlaneError
from .errors import (
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
)
from .models import CapabilityAnnouncement, CapabilityStatus
from .persistence import CoordinatorStore, _parse_time, _request_key, _time
from .session_leadership import (
    INITIAL_TERM,
    LEADER_CHANGED_EVENT,
    LEADERSHIP_SCHEMA,
    SessionLeadership,
    SessionLeadershipService,
)

ROW_COLUMNS = (
    "session_id",
    "revision",
    "event_id",
    "request_id",
    "event_type",
    "occurred_at",
    "actor_node_id",
    "payload_json",
    "content_hash",
)
_COLUMNS_SQL = ",".join(ROW_COLUMNS)
_COORDINATORS = frozenset({REPLICATED_COORDINATOR_ID, LEGACY_COORDINATOR_ID})
_ANNOUNCEMENTS = frozenset({"capability.registered", "capability.status.changed"})
_PUBLIC_COLUMNS = tuple(
    column for column in ROW_COLUMNS if column not in {"request_id", "content_hash"}
)


def _rows(database: Any, session_id: str) -> list[dict[str, Any]]:
    return [
        {column: row[column] for column in ROW_COLUMNS}
        for row in database.execute(
            f"SELECT {_COLUMNS_SQL} FROM session_events WHERE session_id=? ORDER BY revision",
            (session_id,),
        ).fetchall()
    ]


def _verify_prefix(
    existing: list[dict[str, Any]], canonical: list[dict[str, Any]],
    *, witnessed_revision: int = 0,
) -> list[dict[str, Any]]:
    if len(existing) > len(canonical):
        raise ControlPlaneError(
            "materialized public journal conflicts with canonical prefix"
        )
    index_updates = []
    for old, current in zip(existing, canonical, strict=False):
        if old == current:
            continue
        if current["revision"] <= witnessed_revision and all(
            old[column] == current[column] for column in _PUBLIC_COLUMNS
        ) and (
            current["request_id"] == _request_key("witnessed-event:" + current["event_id"])
            and current["content_hash"] == public_row_content_hash(
                current["event_type"], current["payload_json"]
            )
        ):
            index_updates.append(current)
            continue
        raise ControlPlaneError(
            "materialized public journal conflicts with canonical prefix"
        )
    return index_updates


def _witnessed_revision(journal: Mapping[str, Any]) -> int:
    # The caller first validates the full state, including the closed marker.
    provenance = journal.get("provenance")
    if isinstance(provenance, dict) and provenance.get("kind") == "witnessed":
        return provenance["source_revision"]
    return 0


def _legacy_capability(
    row: Mapping[str, Any], payload: dict[str, Any], session_id: str,
    prior: CapabilityAnnouncement | None,
) -> CapabilityAnnouncement:
    """Keep incomplete witnessed identity without inventing executable metadata."""

    capability_id = payload.get("capability_id")
    owner = payload.get("node_id") or payload.get("owner_node_id")
    capability_type = payload.get("type") or payload.get("capability_type")
    if prior is not None and row["event_type"] == "capability.status.changed":
        owner = owner or prior.node_id
        capability_type = capability_type or prior.type
    if (
        not all(isinstance(value, str) and value for value in (
            capability_id, owner, capability_type
        ))
        or payload.get("session_id", session_id) != session_id
        or (row["actor_node_id"] != owner and not (
            row["event_type"] == "capability.status.changed"
            and prior is not None and row["actor_node_id"] in _COORDINATORS
        ))
        or (prior is not None and (prior.node_id, prior.type) != (owner, capability_type))
        or ("node_id" in payload and payload["node_id"] != owner)
        or ("owner_node_id" in payload and payload["owner_node_id"] != owner)
        or ("type" in payload and payload["type"] != capability_type)
        or ("capability_type" in payload and payload["capability_type"] != capability_type)
    ):
        raise ControlPlaneError("witnessed capability identity is malformed")
    status = (
        CapabilityStatus.REVOKED if payload.get("status") == "revoked"
        else CapabilityStatus.UNAVAILABLE
    )
    if prior is not None and row["event_type"] == "capability.status.changed":
        return replace(prior, status=status)
    try:
        return CapabilityAnnouncement(
            capability_id=capability_id, node_id=owner, session_id=session_id,
            type=capability_type, protocol="metadata-incomplete", protocol_version="0",
            status=status, properties={"metadata_complete": False},
            announced_at=_parse_time(row["occurred_at"]),
        )
    except (FederationValidationError, ValueError) as exc:
        raise ControlPlaneError("witnessed capability identity is malformed") from exc


def _capabilities(
    state: Mapping[str, Any], session_id: str, rows: list[dict[str, Any]],
    *, witnessed_revision: int = 0,
) -> dict[str, CapabilityAnnouncement]:
    """Fold full owner metadata; local liveness never changes public status."""

    capabilities: dict[str, CapabilityAnnouncement] = {}
    incomplete: set[str] = set()
    authority = state["capabilities"].get(session_id, {})
    leader = state["sessions"][session_id]["creator_node_id"]
    for row in rows:
        payload = json.loads(row["payload_json"])
        event_type, actor = row["event_type"], row["actor_node_id"]
        if event_type == LEADER_CHANGED_EVENT and actor in _COORDINATORS:
            # The complete canonical leadership chain was validated above.
            leader = payload["leader_node_id"]
        elif event_type in _ANNOUNCEMENTS:
            capability_id = payload.get("capability_id")
            prior = capabilities.get(capability_id) if isinstance(capability_id, str) else None
            if "schema" not in payload and row["revision"] <= witnessed_revision:
                announcement = _legacy_capability(row, payload, session_id, prior)
                # A partial status can preserve preceding complete metadata,
                # but a partial declaration never proves executable metadata.
                if prior is None or event_type == "capability.registered":
                    incomplete.add(announcement.capability_id)
            else:
                try:
                    announcement = CapabilityAnnouncement.from_dict(payload)
                except FederationValidationError as exc:
                    raise ControlPlaneError(
                        "canonical capability metadata is malformed"
                    ) from exc
                incomplete.discard(announcement.capability_id)
            if announcement.capability_id == BOOTSTRAP_SEAL_CAPABILITY_ID:
                raise ControlPlaneError(
                    "private bootstrap seal appears in public journal"
                )
            legacy_status = (
                "schema" not in payload and row["revision"] <= witnessed_revision
                and event_type == "capability.status.changed" and actor in _COORDINATORS
            )
            if announcement.session_id != session_id or (
                actor != announcement.node_id and not legacy_status
            ):
                raise ControlPlaneError(
                    "canonical capability metadata has wrong owner or session"
                )
            prior = capabilities.get(announcement.capability_id)
            declared = authority.get(announcement.capability_id)
            if (
                prior is not None
                and (prior.node_id, prior.type)
                != (announcement.node_id, announcement.type)
            ) or (
                declared is not None
                and (declared["owner_node_id"], declared["capability_type"])
                != (announcement.node_id, announcement.type)
            ):
                raise ControlPlaneError(
                    "canonical capability identity conflicts with authority"
                )
            capabilities[announcement.capability_id] = announcement
        elif event_type == "capability.health.changed" and actor in _COORDINATORS:
            capability_id = payload.get("capability_id")
            prior = capabilities.get(capability_id)
            if prior is None or payload.get("node_id") != prior.node_id:
                raise ControlPlaneError(
                    "canonical capability health has no matching declaration"
                )
            try:
                status = CapabilityStatus(payload.get("status"))
            except (TypeError, ValueError) as exc:
                raise ControlPlaneError(
                    "canonical capability health status is malformed"
                ) from exc
            if capability_id in incomplete and status != CapabilityStatus.REVOKED:
                status = CapabilityStatus.UNAVAILABLE
            capabilities[capability_id] = replace(prior, status=status)
        elif (
            event_type == "node.left" and actor in {payload.get("node_id"), leader}
        ) or (event_type == "node.revoked" and actor in _COORDINATORS):
            for capability_id, prior in tuple(capabilities.items()):
                if prior.node_id == payload.get("node_id"):
                    capabilities[capability_id] = replace(
                        prior, status=CapabilityStatus.REVOKED
                    )

    for capability_id, declared in authority.items():
        if capability_id == BOOTSTRAP_SEAL_CAPABILITY_ID:
            continue
        if capability_id not in capabilities:
            raise ControlPlaneError(
                "canonical capability authority lacks full public metadata"
            )
        announcement = capabilities[capability_id]
        if (announcement.node_id, announcement.type) != (
            declared["owner_node_id"],
            declared["capability_type"],
        ):
            raise ControlPlaneError(
                "canonical capability identity conflicts with authority"
            )
    for capability_id, announcement in tuple(capabilities.items()):
        if (
            state["memberships"].get(session_id, {}).get(announcement.node_id)
            is not True
            or announcement.node_id in state["revocations"]
        ):
            capabilities[capability_id] = replace(
                announcement, status=CapabilityStatus.REVOKED
            )
    return capabilities


def project_product_journal(
    runtime: Any, database: Any, state: Mapping[str, Any]
) -> None:
    """Project inside the caller's owned transaction, preserving public history.

    Base authority materialization must already have created nodes and sessions.
    PrivateJournalRows.apply runs after this helper in the same transaction.
    """

    if not database.in_transaction:
        raise ControlPlaneError(
            "public journal projection requires an owned transaction"
        )
    validate_product_journal_state(state)
    journals = state.get("product_journal", {}).get("sessions", {})
    plans = []
    for session_id, journal in sorted(journals.items()):
        session = database.execute(
            "SELECT revision,created_by_node_id FROM sessions WHERE session_id=?",
            (session_id,),
        ).fetchone()
        if session is None:
            raise ControlPlaneError("canonical journal session is not materialized")
        if (
            session["created_by_node_id"]
            != state["sessions"][session_id]["creator_node_id"]
        ):
            raise ControlPlaneError(
                "materialized session creator conflicts with canonical journal"
            )
        if session["revision"] > journal["revision"]:
            raise ControlPlaneError(
                "materialized session revision is ahead of canonical journal"
            )
        existing = _rows(database, session_id)
        canonical = journal["rows"]
        witnessed_revision = _witnessed_revision(journal)
        index_updates = _verify_prefix(
            existing, canonical, witnessed_revision=witnessed_revision
        )
        full_capabilities = _capabilities(
            state, session_id, canonical, witnessed_revision=witnessed_revision
        )
        plans.append((session_id, journal, len(existing), full_capabilities, index_updates))

    for session_id, journal, old_count, capabilities, index_updates in plans:
        for row in index_updates:
            database.execute(
                """UPDATE session_events SET request_id=?,content_hash=?
                WHERE session_id=? AND revision=? AND event_id=?""",
                (
                    row["request_id"], row["content_hash"], session_id,
                    row["revision"], row["event_id"],
                ),
            )
        for row in journal["rows"][old_count:]:
            database.execute(
                f"INSERT INTO session_events({_COLUMNS_SQL}) VALUES(?,?,?,?,?,?,?,?,?)",
                tuple(row[column] for column in ROW_COLUMNS),
            )
        database.execute(
            "UPDATE sessions SET revision=? WHERE session_id=?",
            (journal["revision"], session_id),
        )
        if journal["rows"] and journal["rows"][0]["event_type"] == "session.created":
            database.execute(
                "UPDATE sessions SET created_at=? WHERE session_id=?",
                (journal["rows"][0]["occurred_at"], session_id),
            )
        for capability_id, announcement in capabilities.items():
            # The conflict clause deliberately omits last_heartbeat_at. That is
            # a local observation and must survive replays of the public view.
            database.execute(
                """
                INSERT INTO capabilities(
                    session_id,capability_id,node_id,type,protocol,protocol_version,
                    status,properties_json,announced_at,last_heartbeat_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(session_id,capability_id) DO UPDATE SET
                    node_id=excluded.node_id,type=excluded.type,
                    protocol=excluded.protocol,protocol_version=excluded.protocol_version,
                    status=excluded.status,properties_json=excluded.properties_json,
                    announced_at=excluded.announced_at
                """,
                (
                    session_id,
                    capability_id,
                    announcement.node_id,
                    announcement.type,
                    announcement.protocol,
                    announcement.protocol_version,
                    announcement.status.value,
                    json.dumps(
                        announcement.properties,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                        allow_nan=False,
                    ),
                    _time(announcement.announced_at),
                    _time(announcement.announced_at),
                ),
            )


class JournalSessionLeadershipService(SessionLeadershipService):
    """Trust legacy coordinator rows only as exact committed canonical rows."""

    def __init__(self, store: CoordinatorStore, runtime: Any) -> None:
        super().__init__(store)
        self.runtime = runtime

    def _snapshot_tx(self, database: Any, session_id: str) -> SessionLeadership:
        state = self.runtime.node.state
        journal = journal_session(state, session_id)
        if journal is None:
            return super()._snapshot_tx(database, session_id)
        session = database.execute(
            "SELECT * FROM sessions WHERE session_id=?", (session_id,)
        ).fetchone()
        if session is None:
            raise AuthorizationError(
                "unknown-session", "target session does not exist", "session_id"
            )
        creator = str(session["created_by_node_id"])
        if creator != state["sessions"][session_id]["creator_node_id"]:
            raise ControlPlaneError(
                "materialized leadership creator conflicts with canonical journal"
            )
        canonical = journal["rows"]
        local = _rows(database, session_id)
        if len(local) < len(canonical) or local[: len(canonical)] != canonical:
            raise ControlPlaneError(
                "materialized leadership lacks exact canonical journal prefix"
            )
        staged = bool(getattr(getattr(self.runtime, "journal", None), "active", False))
        if len(local) > len(canonical) and not staged:
            raise ControlPlaneError(
                "materialized leadership has an uncommitted public suffix"
            )
        leader, term = creator, INITIAL_TERM
        for index, row in enumerate(local):
            if row["event_type"] != LEADER_CHANGED_EVENT:
                continue
            actor = row["actor_node_id"]
            witnessed = index < len(canonical) and actor == LEGACY_COORDINATOR_ID
            current_coordinator = actor == REPLICATED_COORDINATOR_ID and (
                index < len(canonical) or actor == self.store.coordinator_id
            )
            if not current_coordinator and not witnessed:
                continue
            payload = json.loads(row["payload_json"])
            next_leader, next_term = payload.get("leader_node_id"), payload.get("term")
            if (
                payload.get("schema") != LEADERSHIP_SCHEMA
                or payload.get("session_id") != session_id
                or payload.get("previous_leader_node_id") != leader
                or not isinstance(next_leader, str)
                or not next_leader
                or isinstance(next_term, bool)
                or not isinstance(next_term, int)
                or next_term != term + 1
            ):
                raise FederationOperationError(
                    "malformed-leadership-history",
                    "coordinator leadership history is not a contiguous monotonic chain",
                )
            leader, term = next_leader, next_term
        connectivity = database.execute(
            """
            SELECT membership.removed_at,node.revoked_at,connectivity.state
            FROM session_memberships AS membership
            JOIN nodes AS node ON node.node_id=membership.node_id
            LEFT JOIN node_connectivity AS connectivity ON connectivity.node_id=membership.node_id
            WHERE membership.session_id=? AND membership.node_id=?
            """,
            (session_id, leader),
        ).fetchone()
        return SessionLeadership(
            session_id=session_id,
            creator_node_id=creator,
            leader_node_id=leader,
            term=term,
            leader_connected=bool(
                connectivity is not None
                and connectivity["removed_at"] is None
                and connectivity["revoked_at"] is None
                and str(connectivity["state"] or "").casefold() == "connected"
            ),
        )


__all__ = ["ROW_COLUMNS", "JournalSessionLeadershipService", "project_product_journal"]
