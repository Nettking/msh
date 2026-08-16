"""S6 rebuild/incremental projection orchestration over canonical observations."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from ..canonical.observation import CanonicalObservation
from .cycles import ProductionCycle, infer_production_cycles
from .episodes import OperationalEpisode, segment_operational_episodes
from .model import canonical_sha256
from .policy import SEGMENTATION_POLICY, device_key
from .runs import MachineRun, segment_machine_runs_from_reports
from .store import (
    PROJECTION_STATUS_BLOCKED,
    PROJECTION_STATUS_PARTIAL,
    PROJECTION_STATUS_READY,
    OperationalSegmentationStore,
    SegmentationProjectionRecord,
)
from .timeline import DeviceTimelineReport, TimelineStatus, build_device_timeline_reports

if TYPE_CHECKING:
    from ..canonical.store import CanonicalObservationStore


class SegmentationProjectionError(ValueError):
    """Raised when canonical input cannot form one deterministic S6 projection."""


@dataclass(frozen=True)
class AgentSegmentationBundle:
    source_key: str
    agent_instance_id: int
    observations: tuple[CanonicalObservation, ...]
    reports: tuple[DeviceTimelineReport, ...]
    runs: tuple[MachineRun, ...]
    cycles: tuple[ProductionCycle, ...]
    episodes: tuple[OperationalEpisode, ...]
    input_fingerprint: str
    output_fingerprint: str
    status: str


@dataclass(frozen=True)
class SegmentationProjectionResult:
    projections: tuple[SegmentationProjectionRecord, ...]
    content_fingerprint: str


def _observation_payload(item: CanonicalObservation) -> dict[str, object]:
    payload = asdict(item)
    payload["attributes"] = dict(item.attributes)
    return payload


def _input_fingerprint(observations: Sequence[CanonicalObservation]) -> str:
    return canonical_sha256(
        {
            "kind": "operational-segmentation-input",
            "segmentation_policy": SEGMENTATION_POLICY,
            "observations": [_observation_payload(item) for item in observations],
        }
    )


def _projection_status(
    reports: Sequence[DeviceTimelineReport],
    runs: Sequence[MachineRun],
    episodes: Sequence[OperationalEpisode],
) -> str:
    if any(report.status is not TimelineStatus.READY for report in reports):
        return PROJECTION_STATUS_BLOCKED
    if any(
        report.context_continuity_lost_at_end
        or report.agent_sequence_discontinuities
        or report.timestamp_discontinuities
        for report in reports
    ):
        return PROJECTION_STATUS_PARTIAL
    if any(run.partial_start or run.partial_end for run in runs):
        return PROJECTION_STATUS_PARTIAL
    if any(episode.tool_context_partial for episode in episodes):
        return PROJECTION_STATUS_PARTIAL
    return PROJECTION_STATUS_READY


def _output_fingerprint(
    *,
    source_key: str,
    agent_instance_id: int,
    reports: Sequence[DeviceTimelineReport],
    runs: Sequence[MachineRun],
    cycles: Sequence[ProductionCycle],
    episodes: Sequence[OperationalEpisode],
    status: str,
) -> str:
    return canonical_sha256(
        {
            "kind": "operational-segmentation-output",
            "segmentation_policy": SEGMENTATION_POLICY,
            "source_key": source_key,
            "agent_instance_id": agent_instance_id,
            "status": status,
            "devices": [
                {
                    "device_key": report.device_key,
                    "timeline_status": report.status.value,
                    "context_continuity_lost_at_end": report.context_continuity_lost_at_end,
                    "sequence_gaps": [
                        {
                            "previous_sequence": item.previous_sequence,
                            "next_sequence": item.next_sequence,
                            "missing_start_sequence": item.missing_start_sequence,
                            "missing_end_sequence": item.missing_end_sequence,
                        }
                        for item in report.agent_sequence_discontinuities
                    ],
                    "timestamp_regressions": [
                        {
                            "previous_sequence": item.previous_sequence,
                            "next_sequence": item.next_sequence,
                            "previous_observed_at_us": item.previous_observed_at_us,
                            "next_observed_at_us": item.next_observed_at_us,
                        }
                        for item in report.timestamp_discontinuities
                    ],
                }
                for report in sorted(reports, key=lambda item: item.device_key)
            ],
            "runs": [
                {"segment_id": item.run_id, "fingerprint": item.fingerprint}
                for item in sorted(
                    runs,
                    key=lambda item: (item.device_key, item.start_sequence, item.run_id),
                )
            ],
            "cycles": [
                {"segment_id": item.cycle_id, "fingerprint": item.fingerprint}
                for item in sorted(
                    cycles,
                    key=lambda item: (item.device_key, item.start_sequence, item.cycle_id),
                )
            ],
            "episodes": [
                {"segment_id": item.episode_id, "fingerprint": item.fingerprint}
                for item in sorted(
                    episodes,
                    key=lambda item: (item.device_key, item.first_sequence, item.episode_id),
                )
            ],
        }
    )


def build_agent_segmentation_bundle(
    observations: Iterable[CanonicalObservation],
) -> AgentSegmentationBundle:
    """Run S2-S5 once for one complete canonical Agent-instance scope."""

    rows = tuple(sorted(observations, key=lambda item: item.sequence))
    if not rows:
        raise SegmentationProjectionError("Agent segmentation input is empty")
    source_keys = {item.source_key for item in rows}
    instances = {item.agent_instance_id for item in rows}
    if len(source_keys) != 1 or len(instances) != 1:
        raise SegmentationProjectionError(
            "Agent segmentation input must contain exactly one source/instance scope"
        )
    if len({item.sequence for item in rows}) != len(rows):
        raise SegmentationProjectionError("Agent segmentation input contains duplicate sequence")

    source_key = next(iter(source_keys))
    agent_instance_id = next(iter(instances))
    reports = build_device_timeline_reports(rows)
    reports_by_scope = {
        (report.source_key, report.agent_instance_id, report.device_key): report
        for report in reports
    }
    rows_by_scope: defaultdict[
        tuple[str, int, str], list[CanonicalObservation]
    ] = defaultdict(list)
    for item in rows:
        rows_by_scope[(item.source_key, item.agent_instance_id, device_key(item))].append(item)

    runs = segment_machine_runs_from_reports(reports)
    cycles: list[ProductionCycle] = []
    episodes: list[OperationalEpisode] = []
    for run in runs:
        scope = (run.source_key, run.agent_instance_id, run.device_key)
        report = reports_by_scope.get(scope)
        if report is None:
            raise SegmentationProjectionError("MachineRun has no matching device report")
        scoped_rows = rows_by_scope.get(scope, ())
        inferred = infer_production_cycles(run, report, scoped_rows)
        cycles.extend(inferred)
        for cycle in inferred:
            episodes.extend(segment_operational_episodes(cycle, report, scoped_rows))

    ordered_cycles = tuple(
        sorted(
            cycles,
            key=lambda item: (
                item.source_key,
                item.agent_instance_id,
                item.device_key,
                item.start_sequence,
                item.cycle_id,
            ),
        )
    )
    ordered_episodes = tuple(
        sorted(
            episodes,
            key=lambda item: (
                item.source_key,
                item.agent_instance_id,
                item.device_key,
                item.first_sequence,
                item.episode_id,
            ),
        )
    )
    status = _projection_status(reports, runs, ordered_episodes)
    input_fingerprint = _input_fingerprint(rows)
    output_fingerprint = _output_fingerprint(
        source_key=source_key,
        agent_instance_id=agent_instance_id,
        reports=reports,
        runs=runs,
        cycles=ordered_cycles,
        episodes=ordered_episodes,
        status=status,
    )
    return AgentSegmentationBundle(
        source_key=source_key,
        agent_instance_id=agent_instance_id,
        observations=rows,
        reports=reports,
        runs=runs,
        cycles=ordered_cycles,
        episodes=ordered_episodes,
        input_fingerprint=input_fingerprint,
        output_fingerprint=output_fingerprint,
        status=status,
    )


def project_agent_instance(
    observations: Iterable[CanonicalObservation],
    store: OperationalSegmentationStore,
    *,
    source_key: str,
    agent_instance_id: int,
) -> SegmentationProjectionRecord | None:
    """Rebuild one affected Agent instance, including every device it carries."""

    rows = tuple(
        item
        for item in observations
        if item.source_key == source_key
        and item.agent_instance_id == int(agent_instance_id)
    )
    if not rows:
        store.delete_agent_instance(
            source_key=source_key,
            agent_instance_id=int(agent_instance_id),
        )
        return None

    bundle = build_agent_segmentation_bundle(rows)
    projection_id = f"segmentation-projection:{canonical_sha256({'segmentation_policy': SEGMENTATION_POLICY, 'source_key': source_key, 'agent_instance_id': int(agent_instance_id)})}"
    return store.replace_agent_instance(
        projection_id=projection_id,
        segmentation_policy=SEGMENTATION_POLICY,
        input_fingerprint=bundle.input_fingerprint,
        output_fingerprint=bundle.output_fingerprint,
        status=bundle.status,
        observations=bundle.observations,
        reports=bundle.reports,
        runs=bundle.runs,
        cycles=bundle.cycles,
        episodes=bundle.episodes,
    )


def rebuild_operational_segmentation(
    observations: Iterable[CanonicalObservation],
    store: OperationalSegmentationStore,
) -> SegmentationProjectionResult:
    """Delete and deterministically reproduce S6 from canonical observations."""

    rows = tuple(observations)
    grouped: defaultdict[
        tuple[str, int], list[CanonicalObservation]
    ] = defaultdict(list)
    for item in rows:
        grouped[(item.source_key, item.agent_instance_id)].append(item)

    store.clear()
    projections: list[SegmentationProjectionRecord] = []
    for source_key, agent_instance_id in sorted(grouped):
        projection = project_agent_instance(
            grouped[(source_key, agent_instance_id)],
            store,
            source_key=source_key,
            agent_instance_id=agent_instance_id,
        )
        if projection is not None:
            projections.append(projection)

    return SegmentationProjectionResult(
        projections=tuple(projections),
        content_fingerprint=store.content_fingerprint(),
    )


def rebuild_from_canonical_store(
    canonical_store: CanonicalObservationStore,
    segmentation_store: OperationalSegmentationStore,
) -> SegmentationProjectionResult:
    """Rebuild operational state by reading canonical SQLite only, never raw XML."""

    return rebuild_operational_segmentation(
        canonical_store.query_observations(),
        segmentation_store,
    )


def project_agent_instance_from_canonical_store(
    canonical_store: CanonicalObservationStore,
    segmentation_store: OperationalSegmentationStore,
    *,
    source_key: str,
    agent_instance_id: int,
) -> SegmentationProjectionRecord | None:
    """Bounded incremental S6 rebuild of one canonical Agent-instance partition."""

    rows = canonical_store.query_observations(
        source_key=source_key,
        agent_instance_id=int(agent_instance_id),
    )
    return project_agent_instance(
        rows,
        segmentation_store,
        source_key=source_key,
        agent_instance_id=int(agent_instance_id),
    )


__all__ = [
    "AgentSegmentationBundle",
    "SegmentationProjectionError",
    "SegmentationProjectionResult",
    "build_agent_segmentation_bundle",
    "project_agent_instance",
    "project_agent_instance_from_canonical_store",
    "rebuild_from_canonical_store",
    "rebuild_operational_segmentation",
]
