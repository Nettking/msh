"""Sequence-safe Agent stream preparation for operational segmentation.

S2 must establish MTConnect Agent-wide sequence continuity *before* any device
partitioning. Sequence numbers belong to the Agent buffer, not to an individual
MTConnect Device, so numerical jumps in a device-filtered subsequence are not
proof of missing evidence.

This module deliberately stops at that boundary. It does not reconstruct device
state, interpret semantic roles, account durations, or infer runs/cycles.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

from ..canonical.observation import CanonicalObservation


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


__all__ = [
    "AgentSequenceDiscontinuity",
    "AgentStreamError",
    "AgentStreamReport",
    "build_agent_stream_reports",
]
