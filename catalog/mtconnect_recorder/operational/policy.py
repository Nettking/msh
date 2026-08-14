"""Versioned policy primitives for operational MTConnect projections."""

from __future__ import annotations

from ..canonical.observation import CanonicalObservation

SEGMENTATION_POLICY = "fcp.mtconnect.operational-segmentation.v1"


def device_key(observation: CanonicalObservation) -> str:
    """Return the strongest deterministic device identity on an observation."""

    if observation.device_uuid and observation.device_uuid.strip():
        return f"uuid:{observation.device_uuid.strip()}"
    if observation.machine_id and observation.machine_id.strip():
        return f"machine:{observation.machine_id.strip()}"
    raise ValueError("canonical observation has no usable device identity")


__all__ = ["SEGMENTATION_POLICY", "device_key"]
