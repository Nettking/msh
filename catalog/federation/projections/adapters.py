"""Public exports for framework-neutral Federation projection adapters."""

from .authority_adapter import (
    ActivityRecord,
    DeviceRecord,
    FederatedCapabilityRecord,
    FederationAuthorityAdapter,
    FederationAuthoritySnapshot,
)
from .benchmark_adapter import (
    BenchmarkRecord,
    BenchmarkResultsAdapter,
    BenchmarkSnapshot,
)
from .onboarding_adapter import (
    ContributionRecord,
    OnboardingContractsAdapter,
    OnboardingSnapshot,
)
from .provider_adapter import (
    ProviderOperatorAdapter,
    ProviderRecord,
    ProviderSnapshot,
)
from .storage_job_adapters import (
    JobAuthorityAdapter,
    JobAuthoritySnapshot,
    JobRecord,
    StorageAuthorityAdapter,
    StorageAuthoritySnapshot,
    StorageGroupRecord,
    StorageProviderRecord,
)

__all__ = [
    "ActivityRecord",
    "BenchmarkRecord",
    "BenchmarkResultsAdapter",
    "BenchmarkSnapshot",
    "ContributionRecord",
    "DeviceRecord",
    "FederatedCapabilityRecord",
    "FederationAuthorityAdapter",
    "FederationAuthoritySnapshot",
    "JobAuthorityAdapter",
    "JobAuthoritySnapshot",
    "JobRecord",
    "OnboardingContractsAdapter",
    "OnboardingSnapshot",
    "ProviderOperatorAdapter",
    "ProviderRecord",
    "ProviderSnapshot",
    "StorageAuthorityAdapter",
    "StorageAuthoritySnapshot",
    "StorageGroupRecord",
    "StorageProviderRecord",
]
