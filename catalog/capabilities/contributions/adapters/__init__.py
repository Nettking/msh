"""New CF4 authority adapters."""

from .ai import AICandidateSource, AICandidateSpec, AIContributionAdapter
from .compute import ComputeCandidateSource, ComputeContributionAdapter
from .recorder import RecorderCandidateSource, RecorderContributionAdapter
from .storage import (
    StorageCandidateSource,
    StorageCandidateSpec,
    StorageContributionAdapter,
)

__all__ = [
    "AICandidateSource",
    "AICandidateSpec",
    "AIContributionAdapter",
    "ComputeCandidateSource",
    "ComputeContributionAdapter",
    "RecorderCandidateSource",
    "RecorderContributionAdapter",
    "StorageCandidateSource",
    "StorageCandidateSpec",
    "StorageContributionAdapter",
]
