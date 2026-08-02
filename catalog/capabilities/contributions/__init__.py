"""Capability-first contribution recommendation and activation boundary."""

from .adapters import (
    AICandidateSource,
    AICandidateSpec,
    AIContributionAdapter,
    ComputeCandidateSource,
    ComputeContributionAdapter,
    RecorderCandidateSource,
    RecorderContributionAdapter,
    StorageCandidateSource,
    StorageCandidateSpec,
    StorageContributionAdapter,
)
from .candidates import ContributionCandidateGenerator
from .errors import (
    AdapterResolutionError,
    CandidateNotFoundError,
    ContributionPolicyError,
    ContributionServiceError,
)
from .policy import ContributionPolicyEvaluator, PolicyEvaluation
from .service import ContributionService
from .store import SQLiteContributionIntentStore
from .types import AdapterOutcome, CandidateRecommendation, LocalContributionDescriptor

__all__ = [
    "AICandidateSource",
    "AICandidateSpec",
    "AIContributionAdapter",
    "AdapterOutcome",
    "AdapterResolutionError",
    "CandidateNotFoundError",
    "CandidateRecommendation",
    "ComputeCandidateSource",
    "ComputeContributionAdapter",
    "ContributionCandidateGenerator",
    "ContributionPolicyError",
    "ContributionPolicyEvaluator",
    "ContributionService",
    "ContributionServiceError",
    "LocalContributionDescriptor",
    "PolicyEvaluation",
    "RecorderCandidateSource",
    "RecorderContributionAdapter",
    "SQLiteContributionIntentStore",
    "StorageCandidateSource",
    "StorageCandidateSpec",
    "StorageContributionAdapter",
]
