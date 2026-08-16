"""Durable SQLite projection for S2-S5 operational segmentation.

The database in this module is derived state. Canonical MTConnect observations
remain the input authority and immutable recorder batches remain the evidence
authority. Deleting this database must therefore be safe: S6 can rebuild it
from canonical observations without reparsing raw XML.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from catalog.federation.errors import FederationValidationError
from catalog.federation.sqlite_schema import SQLiteMigration, ensure_sqlite_schema

from ..canonical.observation import CanonicalObservation, epoch_micros, parse_timestamp
from .cycles import ProductionCycle
from .episodes import EpisodeBoundary, OperationalEpisode
from .model import OperationalBoundary, canonical_sha256
from .runs import MachineRun
from .timeline import ContextTransition, DeviceTimelineReport

SEGMENTATION_SCHEMA_NAME = "mtconnect_recorder.operational_segmentation"
SEGMENTATION_SCHEMA_VERSION = 1

SEGMENT_TYPE_RUN = "run"
SEGMENT_TYPE_CYCLE = "cycle"
SEGMENT_TYPE_EPISODE = "episode"

PROJECTION_STATUS_READY = "READY"
PROJECTION_STATUS_PARTIAL = "PARTIAL"
PROJECTION_STATUS_BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class SegmentationRunRecord:
    segment_id: str
    fingerprint: str
    segmentation_policy: str
    source_key: str
    agent_instance_id: int
    device_key: str
    machine_id: str | None
    device_uuid: str | None
    start_sequence: int
    end_sequence: int
    start_at: str
    start_at_us: int
    end_at: str
    end_at_us: int
    start_reason: str
    end_reason: str
    start_confidence: str
    end_confidence: str
    partial_start: bool
    partial_end: bool
    wall_duration_us: int
    active_duration_us: int
    program_stopped_duration_us: int
    feed_hold_duration_us: int
    other_execution_duration_us: int
    unknown_duration_us: int


@dataclass(frozen=True)
class SegmentationCycleRecord:
    segment_id: str
    fingerprint: str
    segmentation_policy: str
    machine_run_id: str
    source_key: str
    agent_instance_id: int
    device_key: str
    machine_id: str | None
    device_uuid: str | None
    start_sequence: int
    end_sequence: int
    start_at: str
    start_at_us: int
    end_at: str
    end_at_us: int
    start_reason: str
    end_reason: str
    start_confidence: str
    end_confidence: str
    partial_start: bool
    partial_end: bool
    classification: str
    evidence_flags: tuple[str, ...]
    reason_codes: tuple[str, ...]
    wall_duration_us: int


@dataclass(frozen=True)
class SegmentationEpisodeRecord:
    segment_id: str
    fingerprint: str
    segmentation_policy: str
    production_cycle_id: str
    machine_run_id: str
    source_key: str
    agent_instance_id: int
    device_key: str
    machine_id: str | None
    device_uuid: str | None
    start_sequence: int
    end_sequence: int
    first_sequence: int
    last_sequence: int
    start_at: str
    start_at_us: int
    end_at: str
    end_at_us: int
    start_reason: str
    end_reason: str
    start_confidence: str
    end_confidence: str
    partial_start: bool
    partial_end: bool
    entry_context: dict[str, Any]
    tool_identity_role: str | None
    tool_context_partial: bool
    wall_duration_us: int
    active_duration_us: int
    program_stopped_duration_us: int
    feed_hold_duration_us: int
    other_execution_duration_us: int
    unknown_duration_us: int


@dataclass(frozen=True)
class SegmentationBoundaryRecord:
    boundary_id: str
    segmentation_policy: str
    segment_type: str
    segment_id: str
    edge: str
    source_key: str
    agent_instance_id: int
    device_key: str
    reason: str
    confidence: str
    inherited_reason: str | None
    trigger_sequence: int
    evidence_sequence: int
    observed_at: str
    observed_at_us: int
    raw_batch_sha256: str
    evidence_data_item_id: str | None

    @property
    def canonical_identity(self) -> tuple[str, int, int]:
        return (self.source_key, self.agent_instance_id, self.evidence_sequence)


@dataclass(frozen=True)
class SegmentationContextTransitionRecord:
    transition_id: str
    episode_id: str
    segmentation_policy: str
    source_key: str
    agent_instance_id: int
    device_key: str
    sequence: int
    observed_at: str
    observed_at_us: int
    role: str
    previous_value: Any
    new_value: Any
    data_item_id: str | None
    establishes_context: bool
    raw_batch_sha256: str

    @property
    def canonical_identity(self) -> tuple[str, int, int]:
        return (self.source_key, self.agent_instance_id, self.sequence)


@dataclass(frozen=True)
class SegmentationProjectionRecord:
    projection_id: str
    segmentation_policy: str
    source_key: str
    agent_instance_id: int
    input_fingerprint: str
    output_fingerprint: str
    status: str
    first_sequence: int
    last_sequence: int
    observation_count: int
    device_count: int
    run_count: int
    cycle_count: int
    episode_count: int


@dataclass(frozen=True)
class SegmentationDeviceStatusRecord:
    segmentation_policy: str
    source_key: str
    agent_instance_id: int
    device_key: str
    machine_id: str | None
    device_uuid: str | None
    timeline_status: str
    context_continuity_lost_at_end: bool
    sequence_gap_count: int
    timestamp_regression_count: int


_RUN_COLUMNS = (
    "segment_id",
    "fingerprint",
    "segmentation_policy",
    "source_key",
    "agent_instance_id",
    "device_key",
    "machine_id",
    "device_uuid",
    "start_sequence",
    "end_sequence",
    "start_at",
    "start_at_us",
    "end_at",
    "end_at_us",
    "start_reason",
    "end_reason",
    "start_confidence",
    "end_confidence",
    "partial_start",
    "partial_end",
    "wall_duration_us",
    "active_duration_us",
    "program_stopped_duration_us",
    "feed_hold_duration_us",
    "other_execution_duration_us",
    "unknown_duration_us",
)

_CYCLE_COLUMNS = (
    "segment_id",
    "fingerprint",
    "segmentation_policy",
    "machine_run_id",
    "source_key",
    "agent_instance_id",
    "device_key",
    "machine_id",
    "device_uuid",
    "start_sequence",
    "end_sequence",
    "start_at",
    "start_at_us",
    "end_at",
    "end_at_us",
    "start_reason",
    "end_reason",
    "start_confidence",
    "end_confidence",
    "partial_start",
    "partial_end",
    "classification",
    "evidence_flags",
    "reason_codes",
    "wall_duration_us",
)

_EPISODE_COLUMNS = (
    "segment_id",
    "fingerprint",
    "segmentation_policy",
    "production_cycle_id",
    "machine_run_id",
    "source_key",
    "agent_instance_id",
    "device_key",
    "machine_id",
    "device_uuid",
    "start_sequence",
    "end_sequence",
    "first_sequence",
    "last_sequence",
    "start_at",
    "start_at_us",
    "end_at",
    "end_at_us",
    "start_reason",
    "end_reason",
    "start_confidence",
    "end_confidence",
    "partial_start",
    "partial_end",
    "entry_context",
    "entry_tool_number_key",
    "entry_tool_group_key",
    "tool_identity_role",
    "tool_context_partial",
    "wall_duration_us",
    "active_duration_us",
    "program_stopped_duration_us",
    "feed_hold_duration_us",
    "other_execution_duration_us",
    "unknown_duration_us",
)

_BOUNDARY_COLUMNS = (
    "boundary_id",
    "segmentation_policy",
    "segment_type",
    "segment_id",
    "edge",
    "source_key",
    "agent_instance_id",
    "device_key",
    "reason",
    "confidence",
    "inherited_reason",
    "trigger_sequence",
    "evidence_sequence",
    "observed_at",
    "observed_at_us",
    "raw_batch_sha256",
    "evidence_data_item_id",
)

_TRANSITION_COLUMNS = (
    "transition_id",
    "episode_id",
    "segmentation_policy",
    "source_key",
    "agent_instance_id",
    "device_key",
    "sequence",
    "observed_at",
    "observed_at_us",
    "role",
    "previous_value",
    "new_value",
    "new_value_key",
    "data_item_id",
    "establishes_context",
    "raw_batch_sha256",
)

_PROJECTION_COLUMNS = (
    "projection_id",
    "segmentation_policy",
    "source_key",
    "agent_instance_id",
    "input_fingerprint",
    "output_fingerprint",
    "status",
    "first_sequence",
    "last_sequence",
    "observation_count",
    "device_count",
    "run_count",
    "cycle_count",
    "episode_count",
)

_DEVICE_STATUS_COLUMNS = (
    "segmentation_policy",
    "source_key",
    "agent_instance_id",
    "device_key",
    "machine_id",
    "device_uuid",
    "timeline_status",
    "context_continuity_lost_at_end",
    "sequence_gap_count",
    "timestamp_regression_count",
)


def _create_v1(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE segmentation_projection_runs (
            projection_id TEXT PRIMARY KEY,
            segmentation_policy TEXT NOT NULL,
            source_key TEXT NOT NULL,
            agent_instance_id INTEGER NOT NULL,
            input_fingerprint TEXT NOT NULL,
            output_fingerprint TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('READY', 'PARTIAL', 'BLOCKED')),
            first_sequence INTEGER NOT NULL,
            last_sequence INTEGER NOT NULL,
            observation_count INTEGER NOT NULL CHECK(observation_count >= 0),
            device_count INTEGER NOT NULL CHECK(device_count >= 0),
            run_count INTEGER NOT NULL CHECK(run_count >= 0),
            cycle_count INTEGER NOT NULL CHECK(cycle_count >= 0),
            episode_count INTEGER NOT NULL CHECK(episode_count >= 0),
            UNIQUE(segmentation_policy, source_key, agent_instance_id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE segmentation_device_status (
            segmentation_policy TEXT NOT NULL,
            source_key TEXT NOT NULL,
            agent_instance_id INTEGER NOT NULL,
            device_key TEXT NOT NULL,
            machine_id TEXT,
            device_uuid TEXT,
            timeline_status TEXT NOT NULL,
            context_continuity_lost_at_end INTEGER NOT NULL CHECK(context_continuity_lost_at_end IN (0, 1)),
            sequence_gap_count INTEGER NOT NULL CHECK(sequence_gap_count >= 0),
            timestamp_regression_count INTEGER NOT NULL CHECK(timestamp_regression_count >= 0),
            PRIMARY KEY(segmentation_policy, source_key, agent_instance_id, device_key),
            FOREIGN KEY(segmentation_policy, source_key, agent_instance_id)
                REFERENCES segmentation_projection_runs(segmentation_policy, source_key, agent_instance_id)
                ON DELETE CASCADE
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE segmentation_runs (
            segment_id TEXT PRIMARY KEY,
            fingerprint TEXT NOT NULL,
            segmentation_policy TEXT NOT NULL,
            source_key TEXT NOT NULL,
            agent_instance_id INTEGER NOT NULL,
            device_key TEXT NOT NULL,
            machine_id TEXT,
            device_uuid TEXT,
            start_sequence INTEGER NOT NULL,
            end_sequence INTEGER NOT NULL,
            start_at TEXT NOT NULL,
            start_at_us INTEGER NOT NULL,
            end_at TEXT NOT NULL,
            end_at_us INTEGER NOT NULL,
            start_reason TEXT NOT NULL,
            end_reason TEXT NOT NULL,
            start_confidence TEXT NOT NULL,
            end_confidence TEXT NOT NULL,
            partial_start INTEGER NOT NULL CHECK(partial_start IN (0, 1)),
            partial_end INTEGER NOT NULL CHECK(partial_end IN (0, 1)),
            wall_duration_us INTEGER NOT NULL CHECK(wall_duration_us >= 0),
            active_duration_us INTEGER NOT NULL CHECK(active_duration_us >= 0),
            program_stopped_duration_us INTEGER NOT NULL CHECK(program_stopped_duration_us >= 0),
            feed_hold_duration_us INTEGER NOT NULL CHECK(feed_hold_duration_us >= 0),
            other_execution_duration_us INTEGER NOT NULL CHECK(other_execution_duration_us >= 0),
            unknown_duration_us INTEGER NOT NULL CHECK(unknown_duration_us >= 0),
            CHECK(end_sequence >= start_sequence),
            CHECK(end_at_us >= start_at_us),
            CHECK(active_duration_us + program_stopped_duration_us + feed_hold_duration_us + other_execution_duration_us + unknown_duration_us = wall_duration_us),
            UNIQUE(segment_id, source_key, agent_instance_id, device_key),
            FOREIGN KEY(segmentation_policy, source_key, agent_instance_id)
                REFERENCES segmentation_projection_runs(segmentation_policy, source_key, agent_instance_id)
                ON DELETE CASCADE
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE segmentation_cycles (
            segment_id TEXT PRIMARY KEY,
            fingerprint TEXT NOT NULL,
            segmentation_policy TEXT NOT NULL,
            machine_run_id TEXT NOT NULL,
            source_key TEXT NOT NULL,
            agent_instance_id INTEGER NOT NULL,
            device_key TEXT NOT NULL,
            machine_id TEXT,
            device_uuid TEXT,
            start_sequence INTEGER NOT NULL,
            end_sequence INTEGER NOT NULL,
            start_at TEXT NOT NULL,
            start_at_us INTEGER NOT NULL,
            end_at TEXT NOT NULL,
            end_at_us INTEGER NOT NULL,
            start_reason TEXT NOT NULL,
            end_reason TEXT NOT NULL,
            start_confidence TEXT NOT NULL,
            end_confidence TEXT NOT NULL,
            partial_start INTEGER NOT NULL CHECK(partial_start IN (0, 1)),
            partial_end INTEGER NOT NULL CHECK(partial_end IN (0, 1)),
            classification TEXT NOT NULL,
            evidence_flags TEXT NOT NULL,
            reason_codes TEXT NOT NULL,
            wall_duration_us INTEGER NOT NULL CHECK(wall_duration_us >= 0),
            CHECK(end_sequence >= start_sequence),
            CHECK(end_at_us >= start_at_us),
            UNIQUE(segment_id, source_key, agent_instance_id, device_key),
            FOREIGN KEY(machine_run_id, source_key, agent_instance_id, device_key)
                REFERENCES segmentation_runs(segment_id, source_key, agent_instance_id, device_key)
                ON DELETE CASCADE,
            FOREIGN KEY(segmentation_policy, source_key, agent_instance_id)
                REFERENCES segmentation_projection_runs(segmentation_policy, source_key, agent_instance_id)
                ON DELETE CASCADE
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE segmentation_episodes (
            segment_id TEXT PRIMARY KEY,
            fingerprint TEXT NOT NULL,
            segmentation_policy TEXT NOT NULL,
            production_cycle_id TEXT NOT NULL,
            machine_run_id TEXT NOT NULL,
            source_key TEXT NOT NULL,
            agent_instance_id INTEGER NOT NULL,
            device_key TEXT NOT NULL,
            machine_id TEXT,
            device_uuid TEXT,
            start_sequence INTEGER NOT NULL,
            end_sequence INTEGER NOT NULL,
            first_sequence INTEGER NOT NULL,
            last_sequence INTEGER NOT NULL,
            start_at TEXT NOT NULL,
            start_at_us INTEGER NOT NULL,
            end_at TEXT NOT NULL,
            end_at_us INTEGER NOT NULL,
            start_reason TEXT NOT NULL,
            end_reason TEXT NOT NULL,
            start_confidence TEXT NOT NULL,
            end_confidence TEXT NOT NULL,
            partial_start INTEGER NOT NULL CHECK(partial_start IN (0, 1)),
            partial_end INTEGER NOT NULL CHECK(partial_end IN (0, 1)),
            entry_context TEXT NOT NULL,
            entry_tool_number_key TEXT,
            entry_tool_group_key TEXT,
            tool_identity_role TEXT,
            tool_context_partial INTEGER NOT NULL CHECK(tool_context_partial IN (0, 1)),
            wall_duration_us INTEGER NOT NULL CHECK(wall_duration_us >= 0),
            active_duration_us INTEGER NOT NULL CHECK(active_duration_us >= 0),
            program_stopped_duration_us INTEGER NOT NULL CHECK(program_stopped_duration_us >= 0),
            feed_hold_duration_us INTEGER NOT NULL CHECK(feed_hold_duration_us >= 0),
            other_execution_duration_us INTEGER NOT NULL CHECK(other_execution_duration_us >= 0),
            unknown_duration_us INTEGER NOT NULL CHECK(unknown_duration_us >= 0),
            CHECK(end_sequence >= start_sequence),
            CHECK(last_sequence >= first_sequence),
            CHECK(end_at_us >= start_at_us),
            CHECK(active_duration_us + program_stopped_duration_us + feed_hold_duration_us + other_execution_duration_us + unknown_duration_us = wall_duration_us),
            UNIQUE(segment_id, source_key, agent_instance_id, device_key),
            FOREIGN KEY(production_cycle_id, source_key, agent_instance_id, device_key)
                REFERENCES segmentation_cycles(segment_id, source_key, agent_instance_id, device_key)
                ON DELETE CASCADE,
            FOREIGN KEY(machine_run_id, source_key, agent_instance_id, device_key)
                REFERENCES segmentation_runs(segment_id, source_key, agent_instance_id, device_key)
                ON DELETE CASCADE,
            FOREIGN KEY(segmentation_policy, source_key, agent_instance_id)
                REFERENCES segmentation_projection_runs(segmentation_policy, source_key, agent_instance_id)
                ON DELETE CASCADE
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE segmentation_boundaries (
            boundary_id TEXT PRIMARY KEY,
            segmentation_policy TEXT NOT NULL,
            segment_type TEXT NOT NULL CHECK(segment_type IN ('run', 'cycle', 'episode')),
            segment_id TEXT NOT NULL,
            edge TEXT NOT NULL CHECK(edge IN ('start', 'end')),
            source_key TEXT NOT NULL,
            agent_instance_id INTEGER NOT NULL,
            device_key TEXT NOT NULL,
            reason TEXT NOT NULL,
            confidence TEXT NOT NULL,
            inherited_reason TEXT,
            trigger_sequence INTEGER NOT NULL,
            evidence_sequence INTEGER NOT NULL,
            observed_at TEXT NOT NULL,
            observed_at_us INTEGER NOT NULL,
            raw_batch_sha256 TEXT NOT NULL,
            evidence_data_item_id TEXT,
            UNIQUE(segment_type, segment_id, edge),
            FOREIGN KEY(segmentation_policy, source_key, agent_instance_id)
                REFERENCES segmentation_projection_runs(segmentation_policy, source_key, agent_instance_id)
                ON DELETE CASCADE
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE segmentation_context_transitions (
            transition_id TEXT PRIMARY KEY,
            episode_id TEXT NOT NULL,
            segmentation_policy TEXT NOT NULL,
            source_key TEXT NOT NULL,
            agent_instance_id INTEGER NOT NULL,
            device_key TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            observed_at TEXT NOT NULL,
            observed_at_us INTEGER NOT NULL,
            role TEXT NOT NULL,
            previous_value TEXT,
            new_value TEXT,
            new_value_key TEXT,
            data_item_id TEXT,
            establishes_context INTEGER NOT NULL CHECK(establishes_context IN (0, 1)),
            raw_batch_sha256 TEXT NOT NULL,
            UNIQUE(episode_id, sequence, role),
            FOREIGN KEY(episode_id, source_key, agent_instance_id, device_key)
                REFERENCES segmentation_episodes(segment_id, source_key, agent_instance_id, device_key)
                ON DELETE CASCADE
        )
        """
    )

    connection.execute(
        "CREATE INDEX idx_segmentation_runs_scope_time ON segmentation_runs(source_key, agent_instance_id, device_key, start_at_us, end_at_us, start_sequence)"
    )
    connection.execute(
        "CREATE INDEX idx_segmentation_cycles_scope_time ON segmentation_cycles(source_key, agent_instance_id, device_key, start_at_us, end_at_us, start_sequence)"
    )
    connection.execute(
        "CREATE INDEX idx_segmentation_episodes_scope_time ON segmentation_episodes(source_key, agent_instance_id, device_key, start_at_us, end_at_us, first_sequence)"
    )
    connection.execute(
        "CREATE INDEX idx_segmentation_episodes_tool_number ON segmentation_episodes(entry_tool_number_key, source_key, device_key, start_at_us)"
    )
    connection.execute(
        "CREATE INDEX idx_segmentation_episodes_tool_group ON segmentation_episodes(entry_tool_group_key, source_key, device_key, start_at_us)"
    )
    connection.execute(
        "CREATE INDEX idx_segmentation_boundaries_segment ON segmentation_boundaries(segment_type, segment_id, edge)"
    )
    connection.execute(
        "CREATE INDEX idx_segmentation_transitions_episode ON segmentation_context_transitions(episode_id, sequence, role)"
    )
    connection.execute(
        "CREATE INDEX idx_segmentation_transitions_tool ON segmentation_context_transitions(role, new_value_key, source_key, device_key, observed_at_us)"
    )

    connection.execute(
        """
        CREATE TRIGGER segmentation_cycle_parent_containment
        BEFORE INSERT ON segmentation_cycles
        BEGIN
            SELECT CASE WHEN NOT EXISTS (
                SELECT 1 FROM segmentation_runs AS parent
                WHERE parent.segment_id = NEW.machine_run_id
                  AND parent.source_key = NEW.source_key
                  AND parent.agent_instance_id = NEW.agent_instance_id
                  AND parent.device_key = NEW.device_key
                  AND NEW.start_sequence >= parent.start_sequence
                  AND NEW.end_sequence <= parent.end_sequence
                  AND NEW.start_at_us >= parent.start_at_us
                  AND NEW.end_at_us <= parent.end_at_us
            ) THEN RAISE(ABORT, 'cycle outside parent run') END;
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER segmentation_episode_parent_containment
        BEFORE INSERT ON segmentation_episodes
        BEGIN
            SELECT CASE WHEN NOT EXISTS (
                SELECT 1 FROM segmentation_cycles AS parent
                WHERE parent.segment_id = NEW.production_cycle_id
                  AND parent.source_key = NEW.source_key
                  AND parent.agent_instance_id = NEW.agent_instance_id
                  AND parent.device_key = NEW.device_key
                  AND NEW.start_sequence >= parent.start_sequence
                  AND NEW.end_sequence <= parent.end_sequence
                  AND NEW.first_sequence >= parent.start_sequence
                  AND NEW.last_sequence <= parent.end_sequence
                  AND NEW.start_at_us >= parent.start_at_us
                  AND NEW.end_at_us <= parent.end_at_us
            ) THEN RAISE(ABORT, 'episode outside parent cycle') END;
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER segmentation_episode_no_overlap
        BEFORE INSERT ON segmentation_episodes
        BEGIN
            SELECT CASE WHEN EXISTS (
                SELECT 1 FROM segmentation_episodes AS existing
                WHERE existing.production_cycle_id = NEW.production_cycle_id
                  AND NOT (
                      NEW.last_sequence < existing.first_sequence
                      OR NEW.first_sequence > existing.last_sequence
                  )
            ) THEN RAISE(ABORT, 'episodes overlap inside cycle') END;
        END
        """
    )


def _table_exists(connection: sqlite3.Connection, name: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone()
        is not None
    )


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}


def _detect_legacy_version(connection: sqlite3.Connection) -> int:
    if not _table_exists(connection, "segmentation_runs"):
        return 0
    return SEGMENTATION_SCHEMA_VERSION


def _validate(connection: sqlite3.Connection) -> None:
    expected = (
        ("segmentation_runs", _RUN_COLUMNS),
        ("segmentation_cycles", _CYCLE_COLUMNS),
        ("segmentation_episodes", _EPISODE_COLUMNS),
        ("segmentation_boundaries", _BOUNDARY_COLUMNS),
        ("segmentation_context_transitions", _TRANSITION_COLUMNS),
        ("segmentation_projection_runs", _PROJECTION_COLUMNS),
        ("segmentation_device_status", _DEVICE_STATUS_COLUMNS),
    )
    for table, columns in expected:
        if not _table_exists(connection, table):
            raise FederationValidationError(
                "unsupported-operational-segmentation-schema",
                "schema",
                f"{table} table is missing",
            )
        missing = set(columns).difference(_columns(connection, table))
        if missing:
            raise FederationValidationError(
                "unsupported-operational-segmentation-schema",
                "schema",
                f"{table} is missing required columns: {sorted(missing)!r}",
            )


def _as_micros(value: datetime | str | int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError("time bound must not be a bool")
    if isinstance(value, int):
        return value
    if isinstance(value, datetime):
        moment = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return epoch_micros(moment)
    return epoch_micros(parse_timestamp(str(value)))


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json_value(value: Any) -> str | None:
    return None if value is None else _json(value)


def _decode_json(value: str | None) -> Any:
    return None if value is None else json.loads(value)


def semantic_value_key(value: Any) -> str | None:
    """Return a stable lookup key without changing stored semantic values."""

    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return format(value, ".15g")
    return str(value).strip()


def _single_provenance(
    observations: Sequence[CanonicalObservation],
    attribute: str,
) -> str | None:
    values = {
        str(value).strip()
        for item in observations
        if (value := getattr(item, attribute)) is not None and str(value).strip()
    }
    if len(values) > 1:
        raise ValueError(f"device scope has conflicting {attribute}: {sorted(values)!r}")
    return next(iter(values), None)


def _entry_context_payload(episode: OperationalEpisode) -> dict[str, Any]:
    context = episode.entry_context
    return {
        "main_program": context.main_program,
        "subprogram": context.subprogram,
        "program_comment": context.program_comment,
        "line": context.line,
        "controller_mode": context.controller_mode,
        "tool_number": context.tool_number,
        "tool_group": context.tool_group,
        "pallet": context.pallet,
        "partial_fields": list(context.partial_fields),
    }


def _row_to_run(row: sqlite3.Row) -> SegmentationRunRecord:
    return SegmentationRunRecord(
        segment_id=row["segment_id"],
        fingerprint=row["fingerprint"],
        segmentation_policy=row["segmentation_policy"],
        source_key=row["source_key"],
        agent_instance_id=int(row["agent_instance_id"]),
        device_key=row["device_key"],
        machine_id=row["machine_id"],
        device_uuid=row["device_uuid"],
        start_sequence=int(row["start_sequence"]),
        end_sequence=int(row["end_sequence"]),
        start_at=row["start_at"],
        start_at_us=int(row["start_at_us"]),
        end_at=row["end_at"],
        end_at_us=int(row["end_at_us"]),
        start_reason=row["start_reason"],
        end_reason=row["end_reason"],
        start_confidence=row["start_confidence"],
        end_confidence=row["end_confidence"],
        partial_start=bool(row["partial_start"]),
        partial_end=bool(row["partial_end"]),
        wall_duration_us=int(row["wall_duration_us"]),
        active_duration_us=int(row["active_duration_us"]),
        program_stopped_duration_us=int(row["program_stopped_duration_us"]),
        feed_hold_duration_us=int(row["feed_hold_duration_us"]),
        other_execution_duration_us=int(row["other_execution_duration_us"]),
        unknown_duration_us=int(row["unknown_duration_us"]),
    )


def _row_to_cycle(row: sqlite3.Row) -> SegmentationCycleRecord:
    return SegmentationCycleRecord(
        segment_id=row["segment_id"],
        fingerprint=row["fingerprint"],
        segmentation_policy=row["segmentation_policy"],
        machine_run_id=row["machine_run_id"],
        source_key=row["source_key"],
        agent_instance_id=int(row["agent_instance_id"]),
        device_key=row["device_key"],
        machine_id=row["machine_id"],
        device_uuid=row["device_uuid"],
        start_sequence=int(row["start_sequence"]),
        end_sequence=int(row["end_sequence"]),
        start_at=row["start_at"],
        start_at_us=int(row["start_at_us"]),
        end_at=row["end_at"],
        end_at_us=int(row["end_at_us"]),
        start_reason=row["start_reason"],
        end_reason=row["end_reason"],
        start_confidence=row["start_confidence"],
        end_confidence=row["end_confidence"],
        partial_start=bool(row["partial_start"]),
        partial_end=bool(row["partial_end"]),
        classification=row["classification"],
        evidence_flags=tuple(json.loads(row["evidence_flags"])),
        reason_codes=tuple(json.loads(row["reason_codes"])),
        wall_duration_us=int(row["wall_duration_us"]),
    )


def _row_to_episode(row: sqlite3.Row) -> SegmentationEpisodeRecord:
    return SegmentationEpisodeRecord(
        segment_id=row["segment_id"],
        fingerprint=row["fingerprint"],
        segmentation_policy=row["segmentation_policy"],
        production_cycle_id=row["production_cycle_id"],
        machine_run_id=row["machine_run_id"],
        source_key=row["source_key"],
        agent_instance_id=int(row["agent_instance_id"]),
        device_key=row["device_key"],
        machine_id=row["machine_id"],
        device_uuid=row["device_uuid"],
        start_sequence=int(row["start_sequence"]),
        end_sequence=int(row["end_sequence"]),
        first_sequence=int(row["first_sequence"]),
        last_sequence=int(row["last_sequence"]),
        start_at=row["start_at"],
        start_at_us=int(row["start_at_us"]),
        end_at=row["end_at"],
        end_at_us=int(row["end_at_us"]),
        start_reason=row["start_reason"],
        end_reason=row["end_reason"],
        start_confidence=row["start_confidence"],
        end_confidence=row["end_confidence"],
        partial_start=bool(row["partial_start"]),
        partial_end=bool(row["partial_end"]),
        entry_context=dict(json.loads(row["entry_context"])),
        tool_identity_role=row["tool_identity_role"],
        tool_context_partial=bool(row["tool_context_partial"]),
        wall_duration_us=int(row["wall_duration_us"]),
        active_duration_us=int(row["active_duration_us"]),
        program_stopped_duration_us=int(row["program_stopped_duration_us"]),
        feed_hold_duration_us=int(row["feed_hold_duration_us"]),
        other_execution_duration_us=int(row["other_execution_duration_us"]),
        unknown_duration_us=int(row["unknown_duration_us"]),
    )


def _row_to_boundary(row: sqlite3.Row) -> SegmentationBoundaryRecord:
    return SegmentationBoundaryRecord(
        boundary_id=row["boundary_id"],
        segmentation_policy=row["segmentation_policy"],
        segment_type=row["segment_type"],
        segment_id=row["segment_id"],
        edge=row["edge"],
        source_key=row["source_key"],
        agent_instance_id=int(row["agent_instance_id"]),
        device_key=row["device_key"],
        reason=row["reason"],
        confidence=row["confidence"],
        inherited_reason=row["inherited_reason"],
        trigger_sequence=int(row["trigger_sequence"]),
        evidence_sequence=int(row["evidence_sequence"]),
        observed_at=row["observed_at"],
        observed_at_us=int(row["observed_at_us"]),
        raw_batch_sha256=row["raw_batch_sha256"],
        evidence_data_item_id=row["evidence_data_item_id"],
    )


def _row_to_transition(row: sqlite3.Row) -> SegmentationContextTransitionRecord:
    return SegmentationContextTransitionRecord(
        transition_id=row["transition_id"],
        episode_id=row["episode_id"],
        segmentation_policy=row["segmentation_policy"],
        source_key=row["source_key"],
        agent_instance_id=int(row["agent_instance_id"]),
        device_key=row["device_key"],
        sequence=int(row["sequence"]),
        observed_at=row["observed_at"],
        observed_at_us=int(row["observed_at_us"]),
        role=row["role"],
        previous_value=_decode_json(row["previous_value"]),
        new_value=_decode_json(row["new_value"]),
        data_item_id=row["data_item_id"],
        establishes_context=bool(row["establishes_context"]),
        raw_batch_sha256=row["raw_batch_sha256"],
    )


def _row_to_projection(row: sqlite3.Row) -> SegmentationProjectionRecord:
    return SegmentationProjectionRecord(
        projection_id=row["projection_id"],
        segmentation_policy=row["segmentation_policy"],
        source_key=row["source_key"],
        agent_instance_id=int(row["agent_instance_id"]),
        input_fingerprint=row["input_fingerprint"],
        output_fingerprint=row["output_fingerprint"],
        status=row["status"],
        first_sequence=int(row["first_sequence"]),
        last_sequence=int(row["last_sequence"]),
        observation_count=int(row["observation_count"]),
        device_count=int(row["device_count"]),
        run_count=int(row["run_count"]),
        cycle_count=int(row["cycle_count"]),
        episode_count=int(row["episode_count"]),
    )


def _row_to_device_status(row: sqlite3.Row) -> SegmentationDeviceStatusRecord:
    return SegmentationDeviceStatusRecord(
        segmentation_policy=row["segmentation_policy"],
        source_key=row["source_key"],
        agent_instance_id=int(row["agent_instance_id"]),
        device_key=row["device_key"],
        machine_id=row["machine_id"],
        device_uuid=row["device_uuid"],
        timeline_status=row["timeline_status"],
        context_continuity_lost_at_end=bool(row["context_continuity_lost_at_end"]),
        sequence_gap_count=int(row["sequence_gap_count"]),
        timestamp_regression_count=int(row["timestamp_regression_count"]),
    )


class OperationalSegmentationStore:
    """Disposable, rebuildable S6 projection with narrow deterministic queries."""

    def __init__(self, database_path: Path | str) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            ensure_sqlite_schema(
                connection,
                schema_name=SEGMENTATION_SCHEMA_NAME,
                target_version=SEGMENTATION_SCHEMA_VERSION,
                migrations=(SQLiteMigration(1, _create_v1),),
                detect_legacy_version=_detect_legacy_version,
                validate=_validate,
            )

    def clear(self) -> None:
        """Delete all derived operational state without touching canonical evidence."""

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("DELETE FROM segmentation_boundaries")
            connection.execute("DELETE FROM segmentation_projection_runs")

    def delete_agent_instance(self, *, source_key: str, agent_instance_id: int) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "DELETE FROM segmentation_boundaries WHERE source_key = ? AND agent_instance_id = ?",
                (source_key, int(agent_instance_id)),
            )
            connection.execute(
                "DELETE FROM segmentation_projection_runs WHERE source_key = ? AND agent_instance_id = ?",
                (source_key, int(agent_instance_id)),
            )

    def replace_agent_instance(
        self,
        *,
        projection_id: str,
        segmentation_policy: str,
        input_fingerprint: str,
        output_fingerprint: str,
        status: str,
        observations: Sequence[CanonicalObservation],
        reports: Sequence[DeviceTimelineReport],
        runs: Sequence[MachineRun],
        cycles: Sequence[ProductionCycle],
        episodes: Sequence[OperationalEpisode],
    ) -> SegmentationProjectionRecord:
        """Atomically replace one complete Agent-instance projection.

        S6 intentionally rebuilds every device sharing the affected Agent
        instance. That preserves Agent-wide gap semantics while remaining a
        bounded incremental unit.
        """

        if not observations:
            raise ValueError("cannot persist an empty Agent-instance projection")
        source_keys = {item.source_key for item in observations}
        instances = {item.agent_instance_id for item in observations}
        if len(source_keys) != 1 or len(instances) != 1:
            raise ValueError("replace_agent_instance requires exactly one Agent scope")
        source_key = next(iter(source_keys))
        agent_instance_id = next(iter(instances))
        ordered_observations = tuple(sorted(observations, key=lambda item: item.sequence))
        observation_by_sequence = {item.sequence: item for item in ordered_observations}
        if len(observation_by_sequence) != len(ordered_observations):
            raise ValueError("Agent projection contains duplicate canonical sequence values")

        rows_by_device: dict[str, list[CanonicalObservation]] = {}
        for item in ordered_observations:
            from .policy import device_key

            rows_by_device.setdefault(device_key(item), []).append(item)

        provenance: dict[str, tuple[str | None, str | None]] = {}
        for key, scoped_rows in rows_by_device.items():
            provenance[key] = (
                _single_provenance(scoped_rows, "machine_id"),
                _single_provenance(scoped_rows, "device_uuid"),
            )

        report_scopes = {
            (item.source_key, item.agent_instance_id, item.device_key) for item in reports
        }
        expected_report_scopes = {
            (source_key, agent_instance_id, key) for key in rows_by_device
        }
        if report_scopes != expected_report_scopes:
            raise ValueError("timeline reports do not cover the complete Agent device set")

        for collection, label in ((runs, "run"), (cycles, "cycle"), (episodes, "episode")):
            for item in collection:
                if (
                    item.source_key != source_key
                    or item.agent_instance_id != agent_instance_id
                    or item.device_key not in rows_by_device
                ):
                    raise ValueError(f"{label} crosses the Agent-instance projection scope")

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "DELETE FROM segmentation_boundaries WHERE source_key = ? AND agent_instance_id = ?",
                (source_key, agent_instance_id),
            )
            connection.execute(
                "DELETE FROM segmentation_projection_runs WHERE source_key = ? AND agent_instance_id = ?",
                (source_key, agent_instance_id),
            )
            connection.execute(
                """
                INSERT INTO segmentation_projection_runs (
                    projection_id, segmentation_policy, source_key, agent_instance_id,
                    input_fingerprint, output_fingerprint, status,
                    first_sequence, last_sequence, observation_count,
                    device_count, run_count, cycle_count, episode_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    projection_id,
                    segmentation_policy,
                    source_key,
                    agent_instance_id,
                    input_fingerprint,
                    output_fingerprint,
                    status,
                    ordered_observations[0].sequence,
                    ordered_observations[-1].sequence,
                    len(ordered_observations),
                    len(reports),
                    len(runs),
                    len(cycles),
                    len(episodes),
                ),
            )

            for report in sorted(reports, key=lambda item: item.device_key):
                machine_id, device_uuid = provenance[report.device_key]
                connection.execute(
                    """
                    INSERT INTO segmentation_device_status (
                        segmentation_policy, source_key, agent_instance_id, device_key,
                        machine_id, device_uuid, timeline_status,
                        context_continuity_lost_at_end, sequence_gap_count,
                        timestamp_regression_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        segmentation_policy,
                        source_key,
                        agent_instance_id,
                        report.device_key,
                        machine_id,
                        device_uuid,
                        report.status.value,
                        int(report.context_continuity_lost_at_end),
                        len(report.agent_sequence_discontinuities),
                        len(report.timestamp_discontinuities),
                    ),
                )

            for run in sorted(runs, key=lambda item: (item.device_key, item.start_sequence, item.run_id)):
                machine_id, device_uuid = provenance[run.device_key]
                connection.execute(
                    """
                    INSERT INTO segmentation_runs VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?
                    )
                    """,
                    (
                        run.run_id,
                        run.fingerprint,
                        run.segmentation_policy,
                        run.source_key,
                        run.agent_instance_id,
                        run.device_key,
                        machine_id,
                        device_uuid,
                        run.start_sequence,
                        run.end_sequence,
                        run.start_boundary.observed_at,
                        run.start_boundary.observed_at_us,
                        run.end_boundary.observed_at,
                        run.end_boundary.observed_at_us,
                        run.start_boundary.reason.value,
                        run.end_boundary.reason.value,
                        run.start_boundary.confidence.value,
                        run.end_boundary.confidence.value,
                        int(run.partial_start),
                        int(run.partial_end),
                        run.wall_duration_us,
                        run.active_duration_us,
                        run.program_stopped_duration_us,
                        run.feed_hold_duration_us,
                        run.other_execution_duration_us,
                        run.unknown_duration_us,
                    ),
                )
                self._insert_boundary(
                    connection,
                    segmentation_policy=segmentation_policy,
                    segment_type=SEGMENT_TYPE_RUN,
                    segment_id=run.run_id,
                    edge="start",
                    boundary=run.start_boundary,
                    observation_by_sequence=observation_by_sequence,
                )
                self._insert_boundary(
                    connection,
                    segmentation_policy=segmentation_policy,
                    segment_type=SEGMENT_TYPE_RUN,
                    segment_id=run.run_id,
                    edge="end",
                    boundary=run.end_boundary,
                    observation_by_sequence=observation_by_sequence,
                )

            for cycle in sorted(cycles, key=lambda item: (item.device_key, item.start_sequence, item.cycle_id)):
                machine_id, device_uuid = provenance[cycle.device_key]
                connection.execute(
                    """
                    INSERT INTO segmentation_cycles VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?
                    )
                    """,
                    (
                        cycle.cycle_id,
                        cycle.fingerprint,
                        cycle.segmentation_policy,
                        cycle.machine_run_id,
                        cycle.source_key,
                        cycle.agent_instance_id,
                        cycle.device_key,
                        machine_id,
                        device_uuid,
                        cycle.start_sequence,
                        cycle.end_sequence,
                        cycle.start_boundary.observed_at,
                        cycle.start_boundary.observed_at_us,
                        cycle.end_boundary.observed_at,
                        cycle.end_boundary.observed_at_us,
                        cycle.start_boundary.reason.value,
                        cycle.end_boundary.reason.value,
                        cycle.start_boundary.confidence.value,
                        cycle.end_boundary.confidence.value,
                        int(cycle.start_boundary.confidence.value != "OBSERVED"),
                        int(cycle.end_boundary.confidence.value != "OBSERVED"),
                        cycle.classification.value,
                        _json([item.value for item in cycle.evidence_flags]),
                        _json([item.value for item in cycle.reason_codes]),
                        cycle.wall_duration_us,
                    ),
                )
                self._insert_boundary(
                    connection,
                    segmentation_policy=segmentation_policy,
                    segment_type=SEGMENT_TYPE_CYCLE,
                    segment_id=cycle.cycle_id,
                    edge="start",
                    boundary=cycle.start_boundary,
                    observation_by_sequence=observation_by_sequence,
                )
                self._insert_boundary(
                    connection,
                    segmentation_policy=segmentation_policy,
                    segment_type=SEGMENT_TYPE_CYCLE,
                    segment_id=cycle.cycle_id,
                    edge="end",
                    boundary=cycle.end_boundary,
                    observation_by_sequence=observation_by_sequence,
                )

            for episode in sorted(
                episodes,
                key=lambda item: (item.device_key, item.first_sequence, item.episode_id),
            ):
                machine_id, device_uuid = provenance[episode.device_key]
                context = _entry_context_payload(episode)
                connection.execute(
                    """
                    INSERT INTO segmentation_episodes VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    (
                        episode.episode_id,
                        episode.fingerprint,
                        episode.segmentation_policy,
                        episode.production_cycle_id,
                        episode.machine_run_id,
                        episode.source_key,
                        episode.agent_instance_id,
                        episode.device_key,
                        machine_id,
                        device_uuid,
                        episode.start_boundary.trigger_sequence,
                        episode.end_boundary.trigger_sequence,
                        episode.first_sequence,
                        episode.last_sequence,
                        episode.start_boundary.observed_at,
                        episode.start_boundary.observed_at_us,
                        episode.end_boundary.observed_at,
                        episode.end_boundary.observed_at_us,
                        episode.start_boundary.reason.value,
                        episode.end_boundary.reason.value,
                        episode.start_boundary.confidence.value,
                        episode.end_boundary.confidence.value,
                        int(episode.start_boundary.confidence.value != "OBSERVED"),
                        int(episode.end_boundary.confidence.value != "OBSERVED"),
                        _json(context),
                        semantic_value_key(episode.entry_context.tool_number),
                        semantic_value_key(episode.entry_context.tool_group),
                        episode.tool_identity_role.value if episode.tool_identity_role is not None else None,
                        int(episode.tool_context_partial),
                        episode.wall_duration_us,
                        episode.active_duration_us,
                        episode.program_stopped_duration_us,
                        episode.feed_hold_duration_us,
                        episode.other_execution_duration_us,
                        episode.unknown_duration_us,
                    ),
                )
                self._insert_boundary(
                    connection,
                    segmentation_policy=segmentation_policy,
                    segment_type=SEGMENT_TYPE_EPISODE,
                    segment_id=episode.episode_id,
                    edge="start",
                    boundary=episode.start_boundary,
                    observation_by_sequence=observation_by_sequence,
                )
                self._insert_boundary(
                    connection,
                    segmentation_policy=segmentation_policy,
                    segment_type=SEGMENT_TYPE_EPISODE,
                    segment_id=episode.episode_id,
                    edge="end",
                    boundary=episode.end_boundary,
                    observation_by_sequence=observation_by_sequence,
                )
                for transition in episode.context_transitions:
                    self._insert_transition(
                        connection,
                        segmentation_policy=segmentation_policy,
                        episode=episode,
                        transition=transition,
                        observation_by_sequence=observation_by_sequence,
                    )

        record = self.projection_for_agent(
            source_key=source_key,
            agent_instance_id=agent_instance_id,
            segmentation_policy=segmentation_policy,
        )
        if record is None:
            raise RuntimeError("Agent-instance projection disappeared after commit")
        return record

    @staticmethod
    def _evidence_observation(
        observation_by_sequence: dict[int, CanonicalObservation],
        *,
        source_key: str,
        agent_instance_id: int,
        sequence: int,
    ) -> CanonicalObservation:
        item = observation_by_sequence.get(sequence)
        if item is None:
            raise ValueError(f"boundary/context evidence sequence {sequence} is absent")
        if item.source_key != source_key or item.agent_instance_id != agent_instance_id:
            raise ValueError("boundary/context evidence resolves outside Agent scope")
        return item

    @classmethod
    def _insert_boundary(
        cls,
        connection: sqlite3.Connection,
        *,
        segmentation_policy: str,
        segment_type: str,
        segment_id: str,
        edge: str,
        boundary: OperationalBoundary | EpisodeBoundary,
        observation_by_sequence: dict[int, CanonicalObservation],
    ) -> None:
        evidence = cls._evidence_observation(
            observation_by_sequence,
            source_key=boundary.source_key,
            agent_instance_id=boundary.agent_instance_id,
            sequence=boundary.evidence_sequence,
        )
        inherited = getattr(boundary, "inherited_reason", None)
        inherited_reason = inherited.value if inherited is not None else None
        boundary_id = f"segmentation-boundary:{canonical_sha256({'segment_type': segment_type, 'segment_id': segment_id, 'edge': edge, 'reason': boundary.reason.value, 'evidence_sequence': boundary.evidence_sequence})}"
        connection.execute(
            """
            INSERT INTO segmentation_boundaries VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                boundary_id,
                segmentation_policy,
                segment_type,
                segment_id,
                edge,
                boundary.source_key,
                boundary.agent_instance_id,
                boundary.device_key,
                boundary.reason.value,
                boundary.confidence.value,
                inherited_reason,
                boundary.trigger_sequence,
                boundary.evidence_sequence,
                boundary.observed_at,
                boundary.observed_at_us,
                evidence.raw_batch_sha256,
                evidence.data_item_id,
            ),
        )

    @classmethod
    def _insert_transition(
        cls,
        connection: sqlite3.Connection,
        *,
        segmentation_policy: str,
        episode: OperationalEpisode,
        transition: ContextTransition,
        observation_by_sequence: dict[int, CanonicalObservation],
    ) -> None:
        evidence = cls._evidence_observation(
            observation_by_sequence,
            source_key=transition.source_key,
            agent_instance_id=transition.agent_instance_id,
            sequence=transition.sequence,
        )
        transition_id = f"segmentation-context:{canonical_sha256({'episode_id': episode.episode_id, 'sequence': transition.sequence, 'role': transition.role.value, 'previous_value': transition.previous_value, 'new_value': transition.new_value})}"
        connection.execute(
            """
            INSERT INTO segmentation_context_transitions VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                transition_id,
                episode.episode_id,
                segmentation_policy,
                transition.source_key,
                transition.agent_instance_id,
                transition.device_key,
                transition.sequence,
                transition.observed_at,
                transition.observed_at_us,
                transition.role.value,
                _json_value(transition.previous_value),
                _json_value(transition.new_value),
                semantic_value_key(transition.new_value),
                transition.data_item_id,
                int(transition.establishes_context),
                evidence.raw_batch_sha256,
            ),
        )

    def projection_for_agent(
        self,
        *,
        source_key: str,
        agent_instance_id: int,
        segmentation_policy: str | None = None,
    ) -> SegmentationProjectionRecord | None:
        clauses = ["source_key = ?", "agent_instance_id = ?"]
        parameters: list[Any] = [source_key, int(agent_instance_id)]
        if segmentation_policy is not None:
            clauses.append("segmentation_policy = ?")
            parameters.append(segmentation_policy)
        with self._connect() as connection:
            row = connection.execute(
                f"SELECT * FROM segmentation_projection_runs WHERE {' AND '.join(clauses)} ORDER BY segmentation_policy LIMIT 1",
                parameters,
            ).fetchone()
        return _row_to_projection(row) if row is not None else None

    def projection_runs(self) -> tuple[SegmentationProjectionRecord, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM segmentation_projection_runs ORDER BY source_key, agent_instance_id, segmentation_policy"
            ).fetchall()
        return tuple(_row_to_projection(row) for row in rows)

    def device_statuses(
        self,
        *,
        source_key: str | None = None,
        agent_instance_id: int | None = None,
        device_key: str | None = None,
    ) -> tuple[SegmentationDeviceStatusRecord, ...]:
        clauses: list[str] = []
        parameters: list[Any] = []
        for column, value in (("source_key", source_key), ("device_key", device_key)):
            if value is not None:
                clauses.append(f"{column} = ?")
                parameters.append(value)
        if agent_instance_id is not None:
            clauses.append("agent_instance_id = ?")
            parameters.append(int(agent_instance_id))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM segmentation_device_status"
                f"{where} ORDER BY source_key, agent_instance_id, device_key, segmentation_policy",
                parameters,
            ).fetchall()
        return tuple(_row_to_device_status(row) for row in rows)

    @staticmethod
    def _time_clauses(
        *,
        start: datetime | str | int | None,
        end: datetime | str | int | None,
        parameters: list[Any],
    ) -> list[str]:
        clauses: list[str] = []
        start_us = _as_micros(start)
        if start_us is not None:
            clauses.append("end_at_us >= ?")
            parameters.append(start_us)
        end_us = _as_micros(end)
        if end_us is not None:
            clauses.append("start_at_us < ?")
            parameters.append(end_us)
        return clauses

    def runs(
        self,
        *,
        source_key: str | None = None,
        agent_instance_id: int | None = None,
        device_key: str | None = None,
        start: datetime | str | int | None = None,
        end: datetime | str | int | None = None,
        limit: int | None = None,
    ) -> tuple[SegmentationRunRecord, ...]:
        clauses: list[str] = []
        parameters: list[Any] = []
        for column, value in (("source_key", source_key), ("device_key", device_key)):
            if value is not None:
                clauses.append(f"{column} = ?")
                parameters.append(value)
        if agent_instance_id is not None:
            clauses.append("agent_instance_id = ?")
            parameters.append(int(agent_instance_id))
        clauses.extend(self._time_clauses(start=start, end=end, parameters=parameters))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        suffix = ""
        if limit is not None:
            suffix = " LIMIT ?"
            parameters.append(int(limit))
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM segmentation_runs"
                f"{where} ORDER BY start_at_us, source_key, agent_instance_id, device_key, start_sequence, segment_id{suffix}",
                parameters,
            ).fetchall()
        return tuple(_row_to_run(row) for row in rows)

    def cycles(
        self,
        *,
        machine_run_id: str | None = None,
        source_key: str | None = None,
        agent_instance_id: int | None = None,
        device_key: str | None = None,
        start: datetime | str | int | None = None,
        end: datetime | str | int | None = None,
        limit: int | None = None,
    ) -> tuple[SegmentationCycleRecord, ...]:
        clauses: list[str] = []
        parameters: list[Any] = []
        for column, value in (
            ("machine_run_id", machine_run_id),
            ("source_key", source_key),
            ("device_key", device_key),
        ):
            if value is not None:
                clauses.append(f"{column} = ?")
                parameters.append(value)
        if agent_instance_id is not None:
            clauses.append("agent_instance_id = ?")
            parameters.append(int(agent_instance_id))
        clauses.extend(self._time_clauses(start=start, end=end, parameters=parameters))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        suffix = ""
        if limit is not None:
            suffix = " LIMIT ?"
            parameters.append(int(limit))
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM segmentation_cycles"
                f"{where} ORDER BY start_at_us, source_key, agent_instance_id, device_key, start_sequence, segment_id{suffix}",
                parameters,
            ).fetchall()
        return tuple(_row_to_cycle(row) for row in rows)

    def episodes(
        self,
        *,
        production_cycle_id: str | None = None,
        source_key: str | None = None,
        agent_instance_id: int | None = None,
        device_key: str | None = None,
        start: datetime | str | int | None = None,
        end: datetime | str | int | None = None,
        limit: int | None = None,
    ) -> tuple[SegmentationEpisodeRecord, ...]:
        clauses: list[str] = []
        parameters: list[Any] = []
        for column, value in (
            ("production_cycle_id", production_cycle_id),
            ("source_key", source_key),
            ("device_key", device_key),
        ):
            if value is not None:
                clauses.append(f"{column} = ?")
                parameters.append(value)
        if agent_instance_id is not None:
            clauses.append("agent_instance_id = ?")
            parameters.append(int(agent_instance_id))
        clauses.extend(self._time_clauses(start=start, end=end, parameters=parameters))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        suffix = ""
        if limit is not None:
            suffix = " LIMIT ?"
            parameters.append(int(limit))
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM segmentation_episodes"
                f"{where} ORDER BY start_at_us, source_key, agent_instance_id, device_key, first_sequence, segment_id{suffix}",
                parameters,
            ).fetchall()
        return tuple(_row_to_episode(row) for row in rows)

    def episodes_for_tool(
        self,
        tool: Any,
        *,
        role: str | None = None,
        source_key: str | None = None,
        device_key: str | None = None,
        limit: int | None = None,
    ) -> tuple[SegmentationEpisodeRecord, ...]:
        key = semantic_value_key(tool)
        if key is None:
            return ()
        normalized_role = role.strip().upper() if role is not None else None
        if normalized_role not in {None, "TOOL_NUMBER", "TOOL_GROUP"}:
            raise ValueError("tool role must be TOOL_NUMBER or TOOL_GROUP")

        clauses: list[str] = []
        parameters: list[Any] = []
        if source_key is not None:
            clauses.append("episode.source_key = ?")
            parameters.append(source_key)
        if device_key is not None:
            clauses.append("episode.device_key = ?")
            parameters.append(device_key)

        role_clauses: list[str] = []
        if normalized_role in {None, "TOOL_NUMBER"}:
            role_clauses.append("episode.entry_tool_number_key = ?")
            parameters.append(key)
        if normalized_role in {None, "TOOL_GROUP"}:
            role_clauses.append("episode.entry_tool_group_key = ?")
            parameters.append(key)

        transition_roles = (
            ("TOOL_NUMBER",) if normalized_role == "TOOL_NUMBER" else
            ("TOOL_GROUP",) if normalized_role == "TOOL_GROUP" else
            ("TOOL_NUMBER", "TOOL_GROUP")
        )
        placeholders = ", ".join("?" for _ in transition_roles)
        role_clauses.append(
            "EXISTS (SELECT 1 FROM segmentation_context_transitions AS transition "
            "WHERE transition.episode_id = episode.segment_id "
            f"AND transition.role IN ({placeholders}) AND transition.new_value_key = ?)"
        )
        parameters.extend((*transition_roles, key))
        clauses.append(f"({' OR '.join(role_clauses)})")

        where = f" WHERE {' AND '.join(clauses)}"
        suffix = ""
        if limit is not None:
            suffix = " LIMIT ?"
            parameters.append(int(limit))
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT DISTINCT episode.* FROM segmentation_episodes AS episode"
                f"{where} ORDER BY episode.start_at_us, episode.source_key, episode.agent_instance_id, episode.device_key, episode.first_sequence, episode.segment_id{suffix}",
                parameters,
            ).fetchall()
        return tuple(_row_to_episode(row) for row in rows)

    def context_transitions_for_episode(
        self, episode_id: str
    ) -> tuple[SegmentationContextTransitionRecord, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM segmentation_context_transitions WHERE episode_id = ? ORDER BY sequence, role, transition_id",
                (episode_id,),
            ).fetchall()
        return tuple(_row_to_transition(row) for row in rows)

    def boundaries_for_segment(
        self,
        segment_id: str,
        *,
        segment_type: str | None = None,
    ) -> tuple[SegmentationBoundaryRecord, ...]:
        clauses = ["segment_id = ?"]
        parameters: list[Any] = [segment_id]
        if segment_type is not None:
            normalized = segment_type.strip().lower()
            if normalized not in {SEGMENT_TYPE_RUN, SEGMENT_TYPE_CYCLE, SEGMENT_TYPE_EPISODE}:
                raise ValueError("unknown segment type")
            clauses.append("segment_type = ?")
            parameters.append(normalized)
        with self._connect() as connection:
            rows = connection.execute(
                f"SELECT * FROM segmentation_boundaries WHERE {' AND '.join(clauses)} ORDER BY CASE edge WHEN 'start' THEN 0 ELSE 1 END, boundary_id",
                parameters,
            ).fetchall()
        return tuple(_row_to_boundary(row) for row in rows)

    def boundary(
        self,
        segment_id: str,
        edge: str,
        *,
        segment_type: str | None = None,
    ) -> SegmentationBoundaryRecord | None:
        normalized_edge = edge.strip().lower()
        if normalized_edge not in {"start", "end"}:
            raise ValueError("boundary edge must be start or end")
        rows = self.boundaries_for_segment(segment_id, segment_type=segment_type)
        return next((item for item in rows if item.edge == normalized_edge), None)

    def latest_run(
        self,
        *,
        source_key: str | None = None,
        device_key: str | None = None,
    ) -> SegmentationRunRecord | None:
        rows = self.runs(source_key=source_key, device_key=device_key)
        return rows[-1] if rows else None

    def latest_episode(
        self,
        *,
        source_key: str | None = None,
        device_key: str | None = None,
    ) -> SegmentationEpisodeRecord | None:
        rows = self.episodes(source_key=source_key, device_key=device_key)
        return rows[-1] if rows else None

    def content_fingerprint(self) -> str:
        """Hash all logical projection content in a deterministic table order."""

        table_orders = (
            ("segmentation_projection_runs", "source_key, agent_instance_id, segmentation_policy"),
            ("segmentation_device_status", "source_key, agent_instance_id, device_key, segmentation_policy"),
            ("segmentation_runs", "source_key, agent_instance_id, device_key, start_sequence, segment_id"),
            ("segmentation_cycles", "source_key, agent_instance_id, device_key, start_sequence, segment_id"),
            ("segmentation_episodes", "source_key, agent_instance_id, device_key, first_sequence, segment_id"),
            ("segmentation_boundaries", "source_key, agent_instance_id, device_key, segment_type, segment_id, edge"),
            ("segmentation_context_transitions", "source_key, agent_instance_id, device_key, episode_id, sequence, role, transition_id"),
        )
        payload: dict[str, list[dict[str, Any]]] = {}
        with self._connect() as connection:
            for table, order in table_orders:
                rows = connection.execute(f"SELECT * FROM {table} ORDER BY {order}").fetchall()
                payload[table] = [dict(row) for row in rows]
        return canonical_sha256(payload)


__all__ = [
    "PROJECTION_STATUS_BLOCKED",
    "PROJECTION_STATUS_PARTIAL",
    "PROJECTION_STATUS_READY",
    "SEGMENTATION_SCHEMA_NAME",
    "SEGMENTATION_SCHEMA_VERSION",
    "SEGMENT_TYPE_CYCLE",
    "SEGMENT_TYPE_EPISODE",
    "SEGMENT_TYPE_RUN",
    "OperationalSegmentationStore",
    "SegmentationBoundaryRecord",
    "SegmentationContextTransitionRecord",
    "SegmentationCycleRecord",
    "SegmentationDeviceStatusRecord",
    "SegmentationEpisodeRecord",
    "SegmentationProjectionRecord",
    "SegmentationRunRecord",
    "semantic_value_key",
]
