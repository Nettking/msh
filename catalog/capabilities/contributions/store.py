"""Restart-safe local contribution-intent persistence."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Self

from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionCandidate,
    ContributionDesiredState,
    ContributionIntent,
    ContributionPolicyState,
)

#: Retained superseded intent revisions per contribution candidate.
#
# ``contribution_intents`` already holds the authoritative current intent for
# each candidate; this table is the superseded revisions behind it, appended on
# every enable/disable/suspend/reconcile. Nothing in the product reads it -- not
# one query selects from it -- so no replay, idempotency, authority or
# recovery path can observe a retired row. That is what makes a bound here
# semantics-preserving rather than a retention policy: the frontier follows from
# the table having no consumer, not from someone choosing how much history is
# worth keeping.
#
# Recent revisions are retained rather than none, because their only remaining
# value is operator forensics and that value is entirely in the recent ones.
# Retirement is by the candidate's own monotonic ``revision``, so no clock
# change retires a row early and progress survives restart.
MAX_INTENT_HISTORY_REVISIONS = 200

#: Superseded revisions one write may retire while catching up on legacy history.
#
# A storage bound is not a work bound: on a device whose history predates this
# ring, retiring "everything past the window" would happen inside the same
# transaction as an ordinary intent change. This mirrors the batch bound the
# provider-health, provider-enrollment and coordinator audit rings already use.
INTENT_HISTORY_MAINTENANCE_BATCH_ROWS = 500


def _stamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class SQLiteContributionIntentStore:
    """Store only local user intent and its deterministic transition history."""

    def __init__(self, path: str | Path) -> None:
        self._path = str(path)
        self._lock = RLock()
        self._connection = sqlite3.connect(
            self._path,
            check_same_thread=False,
            isolation_level=None,
        )
        self._connection.row_factory = sqlite3.Row
        with self._lock:
            if self._path != ":memory:":
                self._connection.execute("PRAGMA journal_mode = WAL")
            self._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS contribution_intents (
                    candidate_id TEXT PRIMARY KEY,
                    device_id TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    intent_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS contribution_intent_history (
                    candidate_id TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    intent_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (candidate_id, revision)
                );
                """
            )

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @staticmethod
    def _decode(payload: str) -> ContributionIntent:
        return ContributionIntent.from_dict(json.loads(payload))

    def get(self, candidate_id: str) -> ContributionIntent | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT intent_json FROM contribution_intents WHERE candidate_id = ?",
                (candidate_id,),
            ).fetchone()
        return None if row is None else self._decode(row["intent_json"])

    def list_intents(self) -> tuple[ContributionIntent, ...]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT intent_json FROM contribution_intents "
                "ORDER BY candidate_id"
            ).fetchall()
        return tuple(self._decode(row["intent_json"]) for row in rows)

    def transition(
        self,
        *,
        candidate: ContributionCandidate,
        desired_state: ContributionDesiredState,
        policy_state: ContributionPolicyState,
        activation_state: ContributionActivationState,
        reason: str | None,
        decided_at: datetime,
    ) -> ContributionIntent:
        desired_state = ContributionDesiredState(desired_state)
        policy_state = ContributionPolicyState(policy_state)
        activation_state = ContributionActivationState(activation_state)
        with self._lock:
            connection = self._connection
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT device_id, revision, intent_json "
                    "FROM contribution_intents WHERE candidate_id = ?",
                    (candidate.candidate_id,),
                ).fetchone()
                previous = None if row is None else self._decode(row["intent_json"])
                if row is not None and row["device_id"] != candidate.device_id:
                    raise ValueError("candidate ID is already bound to another device")
                if previous is not None and (
                    previous.desired_state == desired_state
                    and previous.policy_state == policy_state
                    and previous.activation_state == activation_state
                    and previous.reason == reason
                ):
                    connection.execute("COMMIT")
                    return previous
                revision = 1 if row is None else int(row["revision"]) + 1
                intent = ContributionIntent(
                    candidate_id=candidate.candidate_id,
                    device_id=candidate.device_id,
                    desired_state=desired_state,
                    decision_revision=revision,
                    policy_state=policy_state,
                    activation_state=activation_state,
                    reason=reason,
                    decided_at=decided_at,
                )
                payload = json.dumps(
                    intent.to_dict(),
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                )
                updated_at = _stamp(decided_at)
                connection.execute(
                    "INSERT INTO contribution_intent_history "
                    "(candidate_id, revision, intent_json, updated_at) "
                    "VALUES (?, ?, ?, ?)",
                    (candidate.candidate_id, revision, payload, updated_at),
                )
                # Retire superseded revisions for this candidate in the same
                # transaction that appended one, so the history cannot outgrow
                # its bound between maintenance passes and no separate sweep is
                # needed. One insert may retire up to a batch, so while any
                # overflow remains it strictly shrinks.
                connection.execute(
                    """
                    DELETE FROM contribution_intent_history
                    WHERE candidate_id = ?
                      AND revision IN (
                          SELECT revision FROM contribution_intent_history
                          WHERE candidate_id = ?
                            AND revision <= COALESCE(
                                (
                                    SELECT revision
                                    FROM contribution_intent_history
                                    WHERE candidate_id = ?
                                    ORDER BY revision DESC
                                    LIMIT 1 OFFSET ?
                                ),
                                -1
                            )
                          ORDER BY revision ASC
                          LIMIT ?
                      )
                    """,
                    (
                        candidate.candidate_id,
                        candidate.candidate_id,
                        candidate.candidate_id,
                        MAX_INTENT_HISTORY_REVISIONS,
                        INTENT_HISTORY_MAINTENANCE_BATCH_ROWS,
                    ),
                )
                connection.execute(
                    "INSERT INTO contribution_intents "
                    "(candidate_id, device_id, revision, intent_json, updated_at) "
                    "VALUES (?, ?, ?, ?, ?) "
                    "ON CONFLICT(candidate_id) DO UPDATE SET "
                    "device_id = excluded.device_id, revision = excluded.revision, "
                    "intent_json = excluded.intent_json, updated_at = excluded.updated_at",
                    (
                        candidate.candidate_id,
                        candidate.device_id,
                        revision,
                        payload,
                        updated_at,
                    ),
                )
                connection.execute("COMMIT")
                return intent
            except Exception:
                connection.execute("ROLLBACK")
                raise
