"""Shared deterministic models for MTConnect operational segmentation.

The operational layer keeps boundary semantics explicit so later phases can
consume observed and inferred intervals without reverse-engineering why a
segment starts or ends. These values are part of the segmentation policy and
therefore must not be silently reinterpreted under the same policy identifier.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any


class BoundaryReason(str, Enum):
    """Named reasons allowed to create operational segment boundaries."""

    EXECUTION_ACTIVE = "EXECUTION_ACTIVE"
    EXECUTION_READY = "EXECUTION_READY"
    CAPTURE_BEGINS_ACTIVE = "CAPTURE_BEGINS_ACTIVE"
    CAPTURE_END = "CAPTURE_END"
    SEQUENCE_GAP = "SEQUENCE_GAP"
    AGENT_INSTANCE_END = "AGENT_INSTANCE_END"
    TIMESTAMP_REGRESSION = "TIMESTAMP_REGRESSION"
    PALLET_CONTEXT_CHANGE = "PALLET_CONTEXT_CHANGE"


class BoundaryConfidence(str, Enum):
    """How directly canonical evidence establishes a boundary."""

    OBSERVED = "OBSERVED"
    STRONG_INFERENCE = "STRONG_INFERENCE"
    PARTIAL = "PARTIAL"


@dataclass(frozen=True)
class OperationalBoundary:
    """One explainable boundary anchored to canonical device evidence."""

    source_key: str
    agent_instance_id: int
    device_key: str
    reason: BoundaryReason
    confidence: BoundaryConfidence
    trigger_sequence: int
    evidence_sequence: int
    observed_at: str
    observed_at_us: int


def canonical_sha256(payload: Mapping[str, Any]) -> str:
    """Hash a mapping in the operational layer's deterministic JSON form."""

    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "BoundaryConfidence",
    "BoundaryReason",
    "OperationalBoundary",
    "canonical_sha256",
]
