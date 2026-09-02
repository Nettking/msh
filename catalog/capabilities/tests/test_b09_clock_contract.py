from __future__ import annotations

from catalog.capabilities.storage_authority_enrollment import (
    DEFAULT_LEASE_SECONDS,
    MAX_TRUSTED_V1_CLOCK_OFFSET_SECONDS,
)
from catalog.capabilities.storage_authority_lease import DEFAULT_RENEW_BEFORE_SECONDS


def test_v1_clock_offset_budget_stays_inside_half_the_renewal_margin() -> None:
    """Clock prerequisite must leave useful lease-renewal margin on fast hosts."""

    assert DEFAULT_LEASE_SECONDS == 300
    assert DEFAULT_RENEW_BEFORE_SECONDS == 60
    assert MAX_TRUSTED_V1_CLOCK_OFFSET_SECONDS == 30
    assert MAX_TRUSTED_V1_CLOCK_OFFSET_SECONDS <= DEFAULT_RENEW_BEFORE_SECONDS // 2
