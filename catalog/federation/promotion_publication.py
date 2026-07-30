"""Atomic coordinator publication for Phase E storage promotion."""

from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from .errors import FederationValidationError
from .models import SessionEvent
from .promotion_transaction import PromotionTransactionRecord
from .storage_control_plane import (
    STORAGE_ASSIGNMENT_CHANGED,
    STORAGE_LEADER_GRANTED,
    STORAGE_LEADER_REVOKED,
)
from .storage_protocol import StorageOperation

if TYPE_CHECKING:
    from .phase_d_control import PhaseDControlPlane

PROMOTION_PUBLICATION_SCHEMA = "msh.storage_promotion_publication.v1"
_COORDINATOR_ACTOR = "phase-e-promotion-coordinator"


def _utc(value: datetime | None = None) -> datetime:
    result = value or datetime.now(timezone.utc)
    if result.tzinfo is None or result.utcoffset() is None:
        raise FederationValidationError(
            "invalid-timestamp",
            "published_at",
            "must be timezone-aware",
        )
    return result.astimezone(timezone.utc)


def _event_id(
    session_id: str,
    group_id: str,
    promotion_id: str,
    event_type: str,
) -> str:
    payload = (
        f"{session_id}\0{group_id}\0{promotion_id}\0{event_type}"
    ).encode()
    return "promotion-" + hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class PromotionPublicationRecord:
    schema: str
    promotion_id: str
    session_id: str
    group_id: str
    selected_provider_id: str
    previous_provider_id: str
    grant_id: str
    term: int
    fencing_token: int
    replica_provider_ids: tuple[str, ...]
    lease_expires_at: datetime
    final_authority_identity: str
    control_revision: int
    published_at: datetime

    def __post_init__(self) -> None:
        if self.schema != PROMOTION_PUBLICATION_SCHEMA:
            raise FederationValidationError(
                "unsupported-schema",
                "schema",
                f"expected {PROMOTION_PUBLICATION_SCHEMA}",
            )
        for field in (
            "promotion_id",
            "session_id",
            "group_id",
            "selected_provider_id",
            "previous_provider_id",
            "grant_id",
            "final_authority_identity",
        ):
            value = getattr(self, field)
            if (
                not isinstance(value, str)
                or not value.strip()
                or any(ord(char) < 32 for char in value)
            ):
                raise FederationValidationError(
                    "invalid-id",
                    field,
                    "must be non-empty opaque text",
                )
        for field in ("term", "fencing_token", "control_revision"):
            value = getattr(self, field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise FederationValidationError(
                    "invalid-non-negative-integer",
                    field,
                    "must be a non-negative integer",
                )
        if (
            not isinstance(self.replica_provider_ids, tuple)
            or any(
                not isinstance(provider_id, str) or not provider_id
                for provider_id in self.replica_provider_ids
            )
            or len(self.replica_provider_ids) != len(set(self.replica_provider_ids))
            or self.selected_provider_id in self.replica_provider_ids
        ):
            raise FederationValidationError(
                "invalid-assignment",
                "replica_provider_ids",
                "must contain unique non-primary providers",
            )
        object.__setattr__(
            self,
            "lease_expires_at",
            _utc(self.lease_expires_at),
        )
        object.__setattr__(self, "published_at", _utc(self.published_at))
        if self.lease_expires_at <= self.published_at:
            raise FederationValidationError(
                "invalid-lease",
                "lease_expires_at",
                "must be later than publication",
            )


def _ensure_schema(control: PhaseDControlPlane) -> None:
    with control._connect() as connection:
        connection.execute(
            """CREATE TABLE IF NOT EXISTS storage_promotion_publications (
                   session_id TEXT NOT NULL,
                   group_id TEXT NOT NULL,
                   promotion_id TEXT NOT NULL,
                   selected_provider_id TEXT NOT NULL,
                   previous_provider_id TEXT NOT NULL,
                   grant_id TEXT NOT NULL,
                   term INTEGER NOT NULL CHECK(term >= 0),
                   fencing_token INTEGER NOT NULL CHECK(fencing_token >= 0),
                   replica_provider_ids_json TEXT NOT NULL
                       CHECK(json_valid(replica_provider_ids_json)),
                   lease_expires_at TEXT NOT NULL,
                   final_authority_identity TEXT NOT NULL,
                   control_revision INTEGER NOT NULL CHECK(control_revision >= 0),
                   published_at TEXT NOT NULL,
                   PRIMARY KEY(session_id, group_id, promotion_id),
                   UNIQUE(session_id, group_id, grant_id),
                   UNIQUE(session_id, group_id, control_revision)
               )"""
        )


def promotion_publication(
    control: PhaseDControlPlane,
    session_id: str,
    group_id: str,
    promotion_id: str,
) -> PromotionPublicationRecord | None:
    _ensure_schema(control)
    with control._connect() as connection:
        row = connection.execute(
            """SELECT * FROM storage_promotion_publications
               WHERE session_id=? AND group_id=? AND promotion_id=?""",
            (session_id, group_id, promotion_id),
        ).fetchone()
    if row is None:
        return None
    try:
        replicas = json.loads(row["replica_provider_ids_json"])
        if not isinstance(replicas, list):
            raise TypeError("replica_provider_ids_json must decode to a list")
        return PromotionPublicationRecord(
            schema=PROMOTION_PUBLICATION_SCHEMA,
            promotion_id=row["promotion_id"],
            session_id=row["session_id"],
            group_id=row["group_id"],
            selected_provider_id=row["selected_provider_id"],
            previous_provider_id=row["previous_provider_id"],
            grant_id=row["grant_id"],
            term=int(row["term"]),
            fencing_token=int(row["fencing_token"]),
            replica_provider_ids=tuple(replicas),
            lease_expires_at=datetime.fromisoformat(
                str(row["lease_expires_at"]).replace("Z", "+00:00")
            ),
            final_authority_identity=row["final_authority_identity"],
            control_revision=int(row["control_revision"]),
            published_at=datetime.fromisoformat(
                str(row["published_at"]).replace("Z", "+00:00")
            ),
        )
    except (
        json.JSONDecodeError,
        KeyError,
        TypeError,
        ValueError,
        FederationValidationError,
    ) as exc:
        raise FederationValidationError(
            "corrupt-promotion-publication",
            "storage_promotion_publications",
            "durable coordinator publication is malformed",
        ) from exc


def _validate_publication_snapshot(
    control: PhaseDControlPlane,
    publication: PromotionPublicationRecord,
) -> None:
    snapshot = control.snapshot(publication.session_id)
    assignment = snapshot.groups.get(publication.group_id)
    grant = snapshot.leader_grants.get(publication.group_id)
    if (
        snapshot.revision < publication.control_revision
        or assignment is None
        or assignment.primary_provider_id != publication.selected_provider_id
        or assignment.replica_provider_ids != publication.replica_provider_ids
        or grant is None
        or grant.get("provider_id") != publication.selected_provider_id
        or grant.get("grant_id") != publication.grant_id
        or int(grant.get("term", -1)) != publication.term
        or int(grant.get("fencing_token", -1)) != publication.fencing_token
        or datetime.fromisoformat(
            str(grant.get("lease_expires_at")).replace("Z", "+00:00")
        )
        != publication.lease_expires_at
    ):
        raise FederationValidationError(
            "promotion-publication-mismatch",
            "storage_control_events",
            "coordinator assignment or grant does not match durable publication",
        )


def _validate_existing(
    control: PhaseDControlPlane,
    publication: PromotionPublicationRecord,
    record: PromotionTransactionRecord,
    final_authority_identity: str,
) -> None:
    if (
        publication.selected_provider_id != record.selected_provider_id
        or publication.previous_provider_id != record.previous_provider_id
        or publication.grant_id != record.grant_id
        or publication.term != record.reserved_term
        or publication.final_authority_identity != final_authority_identity
    ):
        raise FederationValidationError(
            "conflicting-promotion-publication",
            "promotion_id",
            "durable coordinator publication conflicts with finalization",
        )
    _validate_publication_snapshot(control, publication)


def _historical_maxima(
    connection: sqlite3.Connection,
    session_id: str,
    group_id: str,
) -> tuple[int, int]:
    term = 0
    token = 0
    rows = connection.execute(
        """SELECT payload_json FROM storage_control_events
           WHERE session_id=? AND event_type=?""",
        (session_id, STORAGE_LEADER_GRANTED),
    ).fetchall()
    for row in rows:
        payload = json.loads(row["payload_json"])
        if payload.get("group_id") != group_id:
            continue
        term = max(term, int(payload["term"]))
        token = max(token, int(payload["fencing_token"]))
    counter = connection.execute(
        """SELECT last_term, last_fencing_token
           FROM storage_fencing_counters
           WHERE session_id=? AND group_id=?""",
        (session_id, group_id),
    ).fetchone()
    if counter is not None:
        term = max(term, int(counter["last_term"]))
        token = max(token, int(counter["last_fencing_token"]))
    return term, token


def publish_storage_promotion(
    control: PhaseDControlPlane,
    session_id: str,
    group_id: str,
    promotion_id: str,
    *,
    now: datetime | None = None,
    actor_node_id: str = _COORDINATOR_ACTOR,
) -> tuple[PromotionPublicationRecord, bool]:
    """Publish the finalized provider grant into coordinator routing exactly once."""

    _ensure_schema(control)
    record = control.promotion_transaction(session_id, group_id, promotion_id)
    finalization = control.promotion_finalization(session_id, group_id, promotion_id)
    if (
        record is None
        or finalization is None
        or record.state != "finalized"
        or record.previous_provider_id is None
        or record.previous_term is None
        or record.reserved_term is None
        or record.grant_id is None
    ):
        raise FederationValidationError(
            "promotion-not-finalized",
            "promotion_id",
            "coordinator publication requires a complete durable finalization",
        )
    existing = promotion_publication(control, session_id, group_id, promotion_id)
    if existing is not None:
        _validate_existing(
            control,
            existing,
            record,
            finalization.final_authority_identity,
        )
        return existing, False

    issued_at = _utc(now)
    expiry = issued_at + timedelta(minutes=5)
    snapshot = control.snapshot(session_id)
    assignment = snapshot.groups.get(group_id)
    active = snapshot.leader_grants.get(group_id)
    target = snapshot.providers.get(record.selected_provider_id)
    if (
        assignment is None
        or assignment.primary_provider_id != record.previous_provider_id
        or record.selected_provider_id not in assignment.replica_provider_ids
        or target is None
        or not target.assignable
        or active is None
        or active.get("provider_id") != record.previous_provider_id
        or int(active.get("term", -1)) != record.previous_term
        or record.reserved_term <= record.previous_term
    ):
        raise FederationValidationError(
            "promotion-control-state-mismatch",
            "storage_control_events",
            "coordinator state no longer matches the validated promotion source",
        )

    new_replicas = tuple(
        dict.fromkeys(
            provider_id
            for provider_id in (
                record.previous_provider_id,
                *assignment.replica_provider_ids,
            )
            if provider_id != record.selected_provider_id
        )
    )
    start_revision = snapshot.revision

    with control._connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        try:
            row = connection.execute(
                """SELECT * FROM storage_promotion_publications
                   WHERE session_id=? AND group_id=? AND promotion_id=?""",
                (session_id, group_id, promotion_id),
            ).fetchone()
            if row is not None:
                connection.commit()
                published = promotion_publication(
                    control,
                    session_id,
                    group_id,
                    promotion_id,
                )
                if published is None:
                    raise FederationValidationError(
                        "corrupt-promotion-publication",
                        "promotion_id",
                        "publication disappeared after commit",
                    )
                _validate_existing(
                    control,
                    published,
                    record,
                    finalization.final_authority_identity,
                )
                return published, False

            current_revision = int(
                connection.execute(
                    """SELECT COALESCE(MAX(revision), 0)
                       FROM storage_control_events WHERE session_id=?""",
                    (session_id,),
                ).fetchone()[0]
            )
            if current_revision != start_revision:
                connection.rollback()
                existing = promotion_publication(
                    control,
                    session_id,
                    group_id,
                    promotion_id,
                )
                if existing is not None:
                    _validate_existing(
                        control,
                        existing,
                        record,
                        finalization.final_authority_identity,
                    )
                    return existing, False
                raise FederationValidationError(
                    "concurrent-control-change",
                    "revision",
                    "storage control state changed before promotion publication",
                )

            historical_term, historical_token = _historical_maxima(
                connection,
                session_id,
                group_id,
            )
            if historical_term > record.reserved_term:
                raise FederationValidationError(
                    "stale-term",
                    "reserved_term",
                    "a later coordinator term already exists",
                )
            event_term = 0
            rows = connection.execute(
                """SELECT payload_json FROM storage_control_events
                   WHERE session_id=? AND event_type=?""",
                (session_id, STORAGE_LEADER_GRANTED),
            ).fetchall()
            for event_row in rows:
                payload = json.loads(event_row["payload_json"])
                if payload.get("group_id") == group_id:
                    event_term = max(event_term, int(payload["term"]))
            if event_term >= record.reserved_term:
                raise FederationValidationError(
                    "conflicting-promotion-publication",
                    "reserved_term",
                    "reserved term is already present without its publication record",
                )
            fencing_token = historical_token + 1
            event_specs: tuple[tuple[str, dict[str, Any]], ...] = (
                (
                    STORAGE_LEADER_REVOKED,
                    {"group_id": group_id, "grant_id": active["grant_id"]},
                ),
                (
                    STORAGE_ASSIGNMENT_CHANGED,
                    {
                        "group_id": group_id,
                        "primary_provider_id": record.selected_provider_id,
                        "replica_provider_ids": list(new_replicas),
                    },
                ),
                (
                    STORAGE_LEADER_GRANTED,
                    {
                        "group_id": group_id,
                        "provider_id": record.selected_provider_id,
                        "grant_id": record.grant_id,
                        "term": record.reserved_term,
                        "fencing_token": fencing_token,
                        "lease_expires_at": expiry.isoformat(),
                        "scopes": [StorageOperation.BATCH_INGEST.value],
                    },
                ),
            )
            candidate = copy.deepcopy(snapshot)
            events: list[SessionEvent] = []
            for offset, (event_type, payload) in enumerate(event_specs, start=1):
                event = SessionEvent(
                    session_id=session_id,
                    revision=start_revision + offset,
                    event_id=_event_id(
                        session_id,
                        group_id,
                        promotion_id,
                        event_type,
                    ),
                    event_type=event_type,
                    occurred_at=issued_at,
                    actor_node_id=actor_node_id,
                    payload=payload,
                )
                candidate.apply(event)
                events.append(event)

            for event in events:
                connection.execute(
                    "INSERT INTO storage_control_events VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        event.session_id,
                        event.revision,
                        event.event_id,
                        event.event_type,
                        event.occurred_at.isoformat(),
                        event.actor_node_id,
                        json.dumps(
                            event.payload,
                            sort_keys=True,
                            separators=(",", ":"),
                            allow_nan=False,
                        ),
                    ),
                )
            connection.execute(
                """INSERT INTO storage_fencing_counters
                       (session_id, group_id, last_term, last_fencing_token)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(session_id, group_id) DO UPDATE SET
                       last_term=MAX(last_term, excluded.last_term),
                       last_fencing_token=MAX(
                           last_fencing_token,
                           excluded.last_fencing_token
                       )""",
                (
                    session_id,
                    group_id,
                    record.reserved_term,
                    fencing_token,
                ),
            )
            connection.execute(
                """INSERT INTO storage_promotion_publications
                   (session_id, group_id, promotion_id, selected_provider_id,
                    previous_provider_id, grant_id, term, fencing_token,
                    replica_provider_ids_json, lease_expires_at,
                    final_authority_identity, control_revision, published_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    session_id,
                    group_id,
                    promotion_id,
                    record.selected_provider_id,
                    record.previous_provider_id,
                    record.grant_id,
                    record.reserved_term,
                    fencing_token,
                    json.dumps(
                        list(new_replicas),
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    ),
                    expiry.isoformat().replace("+00:00", "Z"),
                    finalization.final_authority_identity,
                    events[-1].revision,
                    issued_at.isoformat().replace("+00:00", "Z"),
                ),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    published = promotion_publication(control, session_id, group_id, promotion_id)
    if published is None:
        raise FederationValidationError(
            "corrupt-promotion-publication",
            "promotion_id",
            "publication commit did not produce durable state",
        )
    _validate_existing(
        control,
        published,
        record,
        finalization.final_authority_identity,
    )
    return published, True
