"""Read-only S7 inspection of the complete MTConnect operational segmentation stack.

This module deliberately adds no segmentation semantics. It rebuilds the S6
projection from canonical observations and renders the already-derived S2-S6
state in an auditable form suitable for local reference-capture review.

Private recorder captures may be supplied as ZIP archives. ZIP support only
stages the existing recorder files into a temporary ``DurableRecorderStore``
layout; canonical parsing still goes through the recorder's established
projection code. The capture is never rewritten and no raw XML parser is
implemented here.
"""

from __future__ import annotations

import json
import tempfile
import zipfile
from collections.abc import Iterable
from pathlib import Path, PurePosixPath
from typing import Any

from ..canonical.projection import ProjectionReport, rebuild_from_recorder_archive
from ..canonical.store import CanonicalObservationStore
from ..model import _slug
from .policy import SEGMENTATION_POLICY
from .projection import rebuild_from_canonical_store
from .store import (
    PROJECTION_STATUS_READY,
    OperationalSegmentationStore,
    SegmentationBoundaryRecord,
    SegmentationContextTransitionRecord,
    SegmentationCycleRecord,
    SegmentationEpisodeRecord,
    SegmentationRunRecord,
)

VALIDATION_SCHEMA = "fcp.mtconnect.operational-segmentation-validation.v1"

WARNING_PROJECTION_NOT_READY = "PROJECTION_NOT_READY"
WARNING_TIMELINE_NOT_READY = "TIMELINE_NOT_READY"
WARNING_SEQUENCE_GAP = "SEQUENCE_GAP"
WARNING_TIMESTAMP_REGRESSION = "TIMESTAMP_REGRESSION"
WARNING_CONTEXT_CONTINUITY_LOST = "CONTEXT_CONTINUITY_LOST"
WARNING_PARTIAL_RUN = "PARTIAL_RUN"
WARNING_PARTIAL_TOOL_CONTEXT = "PARTIAL_TOOL_CONTEXT"


class OperationalValidationError(ValueError):
    """Raised when S7 cannot produce a trustworthy inspection report."""


def _boundary_payload(item: SegmentationBoundaryRecord) -> dict[str, Any]:
    return {
        "edge": item.edge,
        "reason": item.reason,
        "confidence": item.confidence,
        "inherited_reason": item.inherited_reason,
        "trigger_sequence": item.trigger_sequence,
        "evidence_sequence": item.evidence_sequence,
        "observed_at": item.observed_at,
        "raw_batch_sha256": item.raw_batch_sha256,
        "evidence_data_item_id": item.evidence_data_item_id,
    }


def _transition_payload(item: SegmentationContextTransitionRecord) -> dict[str, Any]:
    return {
        "sequence": item.sequence,
        "observed_at": item.observed_at,
        "role": item.role,
        "previous_value": item.previous_value,
        "new_value": item.new_value,
        "data_item_id": item.data_item_id,
        "establishes_context": item.establishes_context,
        "raw_batch_sha256": item.raw_batch_sha256,
    }


def _duration_payload(item: SegmentationRunRecord | SegmentationEpisodeRecord) -> dict[str, int]:
    return {
        "wall_duration_us": item.wall_duration_us,
        "active_duration_us": item.active_duration_us,
        "program_stopped_duration_us": item.program_stopped_duration_us,
        "feed_hold_duration_us": item.feed_hold_duration_us,
        "other_execution_duration_us": item.other_execution_duration_us,
        "unknown_duration_us": item.unknown_duration_us,
    }


def _episode_payload(
    store: OperationalSegmentationStore,
    episode: SegmentationEpisodeRecord,
) -> dict[str, Any]:
    transitions = store.context_transitions_for_episode(episode.segment_id)
    return {
        "episode_id": episode.segment_id,
        "fingerprint": episode.fingerprint,
        "start_sequence": episode.start_sequence,
        "end_sequence": episode.end_sequence,
        "first_sequence": episode.first_sequence,
        "last_sequence": episode.last_sequence,
        "start_at": episode.start_at,
        "end_at": episode.end_at,
        "start_reason": episode.start_reason,
        "end_reason": episode.end_reason,
        "start_confidence": episode.start_confidence,
        "end_confidence": episode.end_confidence,
        "partial_start": episode.partial_start,
        "partial_end": episode.partial_end,
        "entry_context": episode.entry_context,
        "tool_identity_role": episode.tool_identity_role,
        "tool_context_partial": episode.tool_context_partial,
        "durations": _duration_payload(episode),
        "boundaries": [
            _boundary_payload(item)
            for item in store.boundaries_for_segment(
                episode.segment_id,
                segment_type="episode",
            )
        ],
        "context_transitions": [_transition_payload(item) for item in transitions],
    }


def _cycle_payload(
    store: OperationalSegmentationStore,
    cycle: SegmentationCycleRecord,
) -> dict[str, Any]:
    episodes = store.episodes(production_cycle_id=cycle.segment_id)
    return {
        "cycle_id": cycle.segment_id,
        "fingerprint": cycle.fingerprint,
        "start_sequence": cycle.start_sequence,
        "end_sequence": cycle.end_sequence,
        "start_at": cycle.start_at,
        "end_at": cycle.end_at,
        "start_reason": cycle.start_reason,
        "end_reason": cycle.end_reason,
        "start_confidence": cycle.start_confidence,
        "end_confidence": cycle.end_confidence,
        "classification": cycle.classification,
        "evidence_flags": list(cycle.evidence_flags),
        "reason_codes": list(cycle.reason_codes),
        "wall_duration_us": cycle.wall_duration_us,
        "episode_count": len(episodes),
        "boundaries": [
            _boundary_payload(item)
            for item in store.boundaries_for_segment(
                cycle.segment_id,
                segment_type="cycle",
            )
        ],
        "episodes": [_episode_payload(store, item) for item in episodes],
    }


def _run_payload(
    store: OperationalSegmentationStore,
    run: SegmentationRunRecord,
) -> dict[str, Any]:
    cycles = store.cycles(machine_run_id=run.segment_id)
    return {
        "run_id": run.segment_id,
        "fingerprint": run.fingerprint,
        "start_sequence": run.start_sequence,
        "end_sequence": run.end_sequence,
        "start_at": run.start_at,
        "end_at": run.end_at,
        "start_reason": run.start_reason,
        "end_reason": run.end_reason,
        "start_confidence": run.start_confidence,
        "end_confidence": run.end_confidence,
        "partial_start": run.partial_start,
        "partial_end": run.partial_end,
        "durations": _duration_payload(run),
        "boundaries": [
            _boundary_payload(item)
            for item in store.boundaries_for_segment(
                run.segment_id,
                segment_type="run",
            )
        ],
        "cycles": [_cycle_payload(store, item) for item in cycles],
    }


def _device_warnings(
    *,
    projection_status: str,
    timeline_status: str,
    context_continuity_lost_at_end: bool,
    sequence_gap_count: int,
    timestamp_regression_count: int,
    runs: Iterable[SegmentationRunRecord],
    episodes: Iterable[SegmentationEpisodeRecord],
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []

    def add(code: str, detail: str, count: int | None = None) -> None:
        payload: dict[str, Any] = {"code": code, "detail": detail}
        if count is not None:
            payload["count"] = count
        warnings.append(payload)

    if projection_status != PROJECTION_STATUS_READY:
        add(
            WARNING_PROJECTION_NOT_READY,
            f"Agent projection status is {projection_status}.",
        )
    if timeline_status != "READY":
        add(
            WARNING_TIMELINE_NOT_READY,
            f"Device timeline status is {timeline_status}.",
        )
    if sequence_gap_count:
        add(
            WARNING_SEQUENCE_GAP,
            "Agent sequence discontinuities affected this device timeline.",
            sequence_gap_count,
        )
    if timestamp_regression_count:
        add(
            WARNING_TIMESTAMP_REGRESSION,
            "Timestamp regressions affected this device timeline.",
            timestamp_regression_count,
        )
    if context_continuity_lost_at_end:
        add(
            WARNING_CONTEXT_CONTINUITY_LOST,
            "Context continuity is unresolved at the end of the capture.",
        )

    partial_runs = sum(1 for item in runs if item.partial_start or item.partial_end)
    if partial_runs:
        add(
            WARNING_PARTIAL_RUN,
            "One or more MachineRuns have a partial capture/discontinuity edge.",
            partial_runs,
        )

    partial_episodes = sum(1 for item in episodes if item.tool_context_partial)
    if partial_episodes:
        add(
            WARNING_PARTIAL_TOOL_CONTEXT,
            "One or more OperationalEpisodes have unresolved tool context.",
            partial_episodes,
        )

    return warnings


def build_validation_report(
    store: OperationalSegmentationStore,
    *,
    source_key: str | None = None,
    agent_instance_id: int | None = None,
) -> dict[str, Any]:
    """Return a deterministic, JSON-serialisable S7 inspection report."""

    projections = tuple(
        item
        for item in store.projection_runs()
        if (source_key is None or item.source_key == source_key)
        and (
            agent_instance_id is None
            or item.agent_instance_id == int(agent_instance_id)
        )
    )
    if not projections:
        raise OperationalValidationError("no operational projection matches the requested scope")

    projection_payloads: list[dict[str, Any]] = []
    for projection in projections:
        statuses = store.device_statuses(
            source_key=projection.source_key,
            agent_instance_id=projection.agent_instance_id,
        )
        device_payloads: list[dict[str, Any]] = []
        for status in statuses:
            runs = store.runs(
                source_key=status.source_key,
                agent_instance_id=status.agent_instance_id,
                device_key=status.device_key,
            )
            cycles = store.cycles(
                source_key=status.source_key,
                agent_instance_id=status.agent_instance_id,
                device_key=status.device_key,
            )
            episodes = store.episodes(
                source_key=status.source_key,
                agent_instance_id=status.agent_instance_id,
                device_key=status.device_key,
            )
            device_payloads.append(
                {
                    "device_key": status.device_key,
                    "machine_id": status.machine_id,
                    "device_uuid": status.device_uuid,
                    "timeline_status": status.timeline_status,
                    "context_continuity_lost_at_end": (
                        status.context_continuity_lost_at_end
                    ),
                    "sequence_gap_count": status.sequence_gap_count,
                    "timestamp_regression_count": status.timestamp_regression_count,
                    "run_count": len(runs),
                    "cycle_count": len(cycles),
                    "episode_count": len(episodes),
                    "warnings": _device_warnings(
                        projection_status=projection.status,
                        timeline_status=status.timeline_status,
                        context_continuity_lost_at_end=(
                            status.context_continuity_lost_at_end
                        ),
                        sequence_gap_count=status.sequence_gap_count,
                        timestamp_regression_count=status.timestamp_regression_count,
                        runs=runs,
                        episodes=episodes,
                    ),
                    "runs": [_run_payload(store, item) for item in runs],
                }
            )

        projection_payloads.append(
            {
                "projection_id": projection.projection_id,
                "source_key": projection.source_key,
                "agent_instance_id": projection.agent_instance_id,
                "status": projection.status,
                "input_fingerprint": projection.input_fingerprint,
                "output_fingerprint": projection.output_fingerprint,
                "first_sequence": projection.first_sequence,
                "last_sequence": projection.last_sequence,
                "observation_count": projection.observation_count,
                "device_count": projection.device_count,
                "run_count": projection.run_count,
                "cycle_count": projection.cycle_count,
                "episode_count": projection.episode_count,
                "devices": device_payloads,
            }
        )

    return {
        "schema": VALIDATION_SCHEMA,
        "segmentation_policy": SEGMENTATION_POLICY,
        "content_fingerprint": store.content_fingerprint(),
        "projections": projection_payloads,
    }


def rebuild_validation_report(
    canonical_store: CanonicalObservationStore,
    segmentation_store: OperationalSegmentationStore,
    *,
    source_key: str | None = None,
    agent_instance_id: int | None = None,
) -> dict[str, Any]:
    """Rebuild S6 from canonical SQLite, then return the deterministic S7 report."""

    rebuild_from_canonical_store(canonical_store, segmentation_store)
    return build_validation_report(
        segmentation_store,
        source_key=source_key,
        agent_instance_id=agent_instance_id,
    )


def _safe_archive_member(name: str) -> PurePosixPath:
    member = PurePosixPath(name)
    if member.is_absolute() or any(part in {"", ".", ".."} for part in member.parts):
        raise OperationalValidationError(f"unsafe capture archive member: {name!r}")
    return member


def stage_reference_capture_zip(
    capture_zip: Path | str,
    data_dir: Path | str,
) -> tuple[tuple[str, int], ...]:
    """Stage raw recorder bytes from a private ZIP into a temporary store layout.

    Only manifest/payload pairs are copied. Manifest JSON is parsed solely to
    discover its source/Agent partition; the bytes are written unchanged so the
    canonical projection remains responsible for schema and digest validation.
    """

    capture_path = Path(capture_zip)
    if not capture_path.is_file():
        raise OperationalValidationError(f"capture ZIP does not exist: {capture_path}")
    destination = Path(data_dir)
    scopes: set[tuple[str, int]] = set()

    with zipfile.ZipFile(capture_path) as archive:
        members = {_safe_archive_member(item.filename): item for item in archive.infolist()}
        manifests = sorted(
            member for member in members if member.name.endswith(".manifest.json")
        )
        if not manifests:
            raise OperationalValidationError("capture ZIP contains no raw batch manifests")

        for member in manifests:
            try:
                payload = json.loads(archive.read(members[member]).decode("utf-8"))
                source_name = str(payload["source_name"]).strip()
                instance = int(payload["agent_instance_id"])
            except (KeyError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise OperationalValidationError(
                    f"invalid capture manifest {member.as_posix()!r}: {exc}"
                ) from exc
            if not source_name:
                raise OperationalValidationError(
                    f"capture manifest {member.as_posix()!r} has no source_name"
                )

            stem = member.name.removesuffix(".manifest.json")
            payload_candidates = (
                member.with_name(f"{stem}.gz"),
                member.with_name(stem),
            )
            raw_member = next((item for item in payload_candidates if item in members), None)
            if raw_member is None:
                raise OperationalValidationError(
                    f"capture manifest {member.as_posix()!r} has no sibling payload"
                )

            day = member.parent.name
            target = (
                destination
                / "sources"
                / "mtconnect_recorder"
                / "raw"
                / _slug(source_name)
                / str(instance)
                / day
            )
            target.mkdir(parents=True, exist_ok=True)
            manifest_target = target / member.name
            raw_target = target / raw_member.name
            if manifest_target.exists() or raw_target.exists():
                raise OperationalValidationError(
                    f"capture staging target already exists for {member.name!r}"
                )
            manifest_target.write_bytes(archive.read(members[member]))
            raw_target.write_bytes(archive.read(members[raw_member]))
            scopes.add((source_name, instance))

    return tuple(sorted(scopes))


def validate_reference_capture_zip(
    capture_zip: Path | str,
    *,
    workspace: Path | str | None = None,
    source_key: str | None = None,
    agent_instance_id: int | None = None,
) -> tuple[dict[str, Any], tuple[ProjectionReport, ...]]:
    """Run raw→canonical→S2-S6 locally without modifying the private capture."""

    if workspace is None:
        with tempfile.TemporaryDirectory(prefix="fcp-mtconnect-s7-") as temporary:
            return validate_reference_capture_zip(
                capture_zip,
                workspace=Path(temporary),
                source_key=source_key,
                agent_instance_id=agent_instance_id,
            )

    root = Path(workspace)
    data_dir = root / "recorder-data"
    canonical_path = root / "canonical.sqlite3"
    segmentation_path = root / "operational_segmentation.sqlite3"
    scopes = stage_reference_capture_zip(capture_zip, data_dir)
    source_names = sorted({name for name, _instance in scopes})
    canonical_reports = rebuild_from_recorder_archive(
        data_dir=data_dir,
        database_path=canonical_path,
        sources=source_names,
    )
    failures = [item for item in canonical_reports if not item.ok]
    if failures:
        details = "; ".join(
            f"{item.source_name}/{item.agent_instance_id}: "
            f"{item.rejected_batches} rejected, {item.skipped_manifests} skipped"
            for item in failures
        )
        raise OperationalValidationError(
            f"canonical reference-capture projection was not clean: {details}"
        )

    canonical_store = CanonicalObservationStore(canonical_path)
    segmentation_store = OperationalSegmentationStore(segmentation_path)
    report = rebuild_validation_report(
        canonical_store,
        segmentation_store,
        source_key=source_key,
        agent_instance_id=agent_instance_id,
    )
    return report, canonical_reports


def render_validation_report(report: dict[str, Any]) -> str:
    """Render the S7 report as compact deterministic inspection text."""

    lines = [
        "MTConnect operational segmentation validation",
        f"policy: {report['segmentation_policy']}",
        f"content fingerprint: {report['content_fingerprint']}",
    ]
    for projection in report["projections"]:
        lines.extend(
            [
                "",
                (
                    f"Agent {projection['source_key']} / "
                    f"{projection['agent_instance_id']} — {projection['status']}"
                ),
                (
                    f"  observations={projection['observation_count']} "
                    f"devices={projection['device_count']} runs={projection['run_count']} "
                    f"cycles={projection['cycle_count']} "
                    f"episodes={projection['episode_count']}"
                ),
            ]
        )
        for device in projection["devices"]:
            lines.append(
                f"  Device {device['device_key']} — timeline={device['timeline_status']}"
            )
            lines.append(
                f"    MachineRuns={device['run_count']} "
                f"ProductionCycles={device['cycle_count']} "
                f"OperationalEpisodes={device['episode_count']}"
            )
            for warning in device["warnings"]:
                count = f" count={warning['count']}" if "count" in warning else ""
                lines.append(
                    f"    WARNING {warning['code']}{count}: {warning['detail']}"
                )
            for run in device["runs"]:
                durations = run["durations"]
                lines.append(
                    f"    Run {run['start_sequence']}..{run['end_sequence']} "
                    f"{run['start_reason']}→{run['end_reason']} "
                    f"wall={durations['wall_duration_us']}us "
                    f"ACTIVE={durations['active_duration_us']}us "
                    f"PROGRAM_STOPPED={durations['program_stopped_duration_us']}us"
                )
                for cycle in run["cycles"]:
                    lines.append(
                        f"      Cycle {cycle['start_sequence']}..{cycle['end_sequence']} "
                        f"{cycle['classification']} confidence="
                        f"{cycle['start_confidence']}/{cycle['end_confidence']} "
                        f"flags={','.join(cycle['evidence_flags']) or '-'}"
                    )
                    lines.append(f"        episodes={cycle['episode_count']}")
                    for episode in cycle["episodes"]:
                        context = episode["entry_context"]
                        durations = episode["durations"]
                        tool = context.get("tool_number")
                        if tool is None:
                            tool = context.get("tool_group")
                        lines.append(
                            f"        Episode {episode['first_sequence']}.."
                            f"{episode['last_sequence']} tool={tool!r} "
                            f"partial_tool={episode['tool_context_partial']} "
                            f"wall={durations['wall_duration_us']}us "
                            f"ACTIVE={durations['active_duration_us']}us "
                            f"PROGRAM_STOPPED="
                            f"{durations['program_stopped_duration_us']}us"
                        )
                        for transition in episode["context_transitions"]:
                            if transition["role"] in {
                                "PROGRAM",
                                "PROGRAM_COMMENT",
                                "LINE",
                                "TOOL_NUMBER",
                                "TOOL_GROUP",
                                "PALLET",
                            }:
                                lines.append(
                                    f"          ctx seq={transition['sequence']} "
                                    f"{transition['role']}: "
                                    f"{transition['previous_value']!r}→"
                                    f"{transition['new_value']!r}"
                                )
    return "\n".join(lines) + "\n"


__all__ = [
    "VALIDATION_SCHEMA",
    "OperationalValidationError",
    "build_validation_report",
    "rebuild_validation_report",
    "render_validation_report",
    "stage_reference_capture_zip",
    "validate_reference_capture_zip",
]
