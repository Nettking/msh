"""Contribution-service errors."""


class ContributionServiceError(RuntimeError):
    """Base error for contribution recommendation and activation failures."""


class CandidateNotFoundError(ContributionServiceError):
    """Raised when a requested candidate is not in the current recommendation set."""


class AdapterResolutionError(ContributionServiceError):
    """Raised when zero or several authority adapters match one candidate."""


class ContributionPolicyError(ContributionServiceError):
    """Raised when policy evaluation cannot produce a safe decision."""
