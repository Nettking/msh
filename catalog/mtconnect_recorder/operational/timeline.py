"""Sequence-safe Agent and device stream preparation for operational segmentation.

S2 must establish MTConnect Agent-wide sequence continuity *before* any device
partitioning. Sequence numbers belong to the Agent buffer, not to an individual
MTConnect Device, so numerical jumps in a device-filtered subsequence are not
proof of missing evidence.

This module deliberately stops before state reconstruction. It orders canonical
Agent evidence, surfaces genuine Agent sequence gaps, and then partitions the
already-checked stream by deterministic canonical ``device_key``. It does not
interpret execution state, reconstruct context, account durations, or infer
runs/cycles.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

from ..canonical.observation import CanonicalObservation
from .policy import device_key


class AgentStreamError(ValueError):
    """Raised when canonical input violates the Agent stream contract."""


@dataclass(frozen=True)
class AgentSequenceDiscontinuity:
    """One genuine gap in a complete canonical MTConnect Agent sequence."""

    source_key: str
    agent_instance_id: int
    previous_sequence: int
    next_sequence: int
    missing_start_sequence: int
    missing_end_sequence: int
    missing_count: int


@dataclass(frozen=True)
class AgentStreamReport:
    """Sequence-ordered canonical evidence for one Agent buffer instance.

    Callers must supply the complete canonical stream for each
    ``(source_key, agent_instance_id)`` represented in the input. Device
    partitioning happens only after this report has established Agent-wide
    continuity.
    """

    source_key: str
    agent_instance_id: int
    observations: tuple[CanonicalObservation, ...]
    sequence_discontinuities: tuple[AgentSequenceDiscontinuity, ...]

    @property
    def is_sequence_continuous(self) -> bool:
        return not self.sequence_discontinuities


@dataclass(frozen=True)
class DeviceStreamPartition:
    """One device's observations from an already-checked Agent stream.

    ``observations`` remain in Agent sequence order, but their sequence numbers
    are not expected to be adjacent because observations for other devices may
    be interleaved between them. ``agent_sequence_discontinuities`` are copied
    from the parent Agent report; they are Agent-wide evidence-loss facts, not
    gaps recomputed from this device-filtered subsequence.
    """

    source_key: str
    agent_instance_id: int
    device_key: str
    observations: tuple[CanonicalObservation, ...]
    agent_sequence_discontinuities: tuple[AgentSequenceDiscontinuity, ...]



def build_agent_stream_reports(
    observations: Iterable[CanonicalObservation],
) -> tuple[AgentStreamReport, ...]:
    """Order complete canonical Agent streams and surface genuine sequence gaps.

    Canonical sequence is the only ordering authority. Timestamps are retained
    as evidence on each observation but never participate in ordering here; a
    timestamp regression is a separate S2 concern.

    Duplicate sequence numbers inside one Agent instance are rejected. The
    canonical layer promises one observation per natural identity, so silently
    choosing one duplicate here would hide an upstream contract violation.
    """

    grouped: defaultdict[
        tuple[str, int], list[CanonicalObservation]
    ] = defaultdict(list)
    for observation in observations:
        grouped[(observation.source_key, observation.agent_instance_id)].append(
            observation
        )

    reports: list[AgentStreamReport] = []
    for (source_key, agent_instance_id), rows in sorted(grouped.items()):
        ordered = tuple(sorted(rows, key=lambda item: item.sequence))

        seen_sequences: set[int] = set()
        for observation in ordered:
            if observation.sequence in seen_sequences:
                raise AgentStreamError(
                    "duplicate canonical sequence in Agent stream: "
                    f"source={source_key!r}, instance={agent_instance_id}, "
                    f"sequence={observation.sequence}"
                )
            seen_sequences.add(observation.sequence)

        discontinuities: list[AgentSequenceDiscontinuity] = []
        for previous, current in zip(ordered, ordered[1:], strict=False):
            if current.sequence == previous.sequence + 1:
                continue
            discontinuities.append(
                AgentSequenceDiscontinuity(
                    source_key=source_key,
                    agent_instance_id=agent_instance_id,
                    previous_sequence=previous.sequence,
                    next_sequence=current.sequence,
                    missing_start_sequence=previous.sequence + 1,
                    missing_end_sequence=current.sequence - 1,
                    missing_count=current.sequence - previous.sequence - 1,
                )
            )

        reports.append(
            AgentStreamReport(
                source_key=source_key,
                agent_instance_id=agent_instance_id,
                observations=ordered,
                sequence_discontinuities=tuple(discontinuities),
            )
        )

    return tuple(reports)



def partition_agent_stream(
    report: AgentStreamReport,
) -> tuple[DeviceStreamPartition, ...]:
    """Partition one checked Agent stream without reinterpreting sequence gaps.

    The parent report is the sole authority for sequence continuity. This
    function only filters its already-ordered observations by ``device_key``.
    It deliberately never applies an adjacency rule to a device subsequence.
    """

    grouped: defaultdict[str, list[CanonicalObservation]] = defaultdict(list)
    previous_sequence: int | None = None
    for observation in report.observations:
        if (
            observation.source_key != report.source_key
            or observation.agent_instance_id != report.agent_instance_id
        ):
            raise AgentStreamError(
                "observation outside AgentStreamReport scope: "
                f"report=({report.source_key!r}, {report.agent_instance_id}), "
                f"observation=({observation.source_key!r}, "
                f"{observation.agent_instance_id}, {observation.sequence})"
            )
        if previous_sequence is not None and observation.sequence <= previous_sequence:
            raise AgentStreamError(
                "AgentStreamReport observations are not strictly sequence ordered"
            )
        previous_sequence = observation.sequence
        grouped[device_key(observation)].append(observation)

    return tuple(
        DeviceStreamPartition(
            source_key=report.source_key,
            agent_instance_id=report.agent_instance_id,
            device_key=canonical_device,
            observations=tuple(grouped[canonical_device]),
            agent_sequence_discontinuities=report.sequence_discontinuities,
        )
        for canonical_device in sorted(grouped)
    )



def partition_agent_streams(
    reports: Iterable[AgentStreamReport],
) -> tuple[DeviceStreamPartition, ...]:
    """Partition multiple checked Agent streams into deterministic device scopes."""

    partitions: list[DeviceStreamPartition] = []
    for report in reports:
        partitions.extend(partition_agent_stream(report))
    return tuple(
        sorted(
            partitions,
            key=lambda item: (
                item.source_key,
                item.agent_instance_id,
                item.device_key,
            ),
        )
    )


__all__ = [
    "AgentSequenceDiscontinuity",
    "AgentStreamError",
    "AgentStreamReport",
    "DeviceStreamPartition",
    "build_agent_stream_reports",
    "partition_agent_stream",
    "partition_agent_streams",
]
