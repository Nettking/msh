"""Capability-first federation discovery, selection, join, and reconnect."""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from catalog.federation.errors import (
    AuthenticationError,
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.models import NodeIdentity
from catalog.federation.onboarding_models import (
    FederationConnectionState,
    FederationDiscoveryResult,
    FederationSessionBinding,
)

from .authority import SessionOnboardingAuthority
from .sources import (
    FederationDiscoveryCandidate,
    FederationDiscoverySource,
)

MAX_DISCOVERY_SOURCES = 8
MAX_DISCOVERY_CANDIDATES = 64
MAX_DISCOVERY_TIMEOUT_SECONDS = 30


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _positive_bound(value: int, field_name: str, *, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise FederationValidationError(
            "invalid-discovery-bound",
            field_name,
            f"must be between 1 and {maximum}",
        )
    return value


def _safe_failure_result(
    *,
    source_index: int,
    now: datetime,
    deadline: datetime,
    reason: str,
) -> FederationDiscoveryResult:
    digest = hashlib.sha256(
        f"fcp-onboarding-source-failure-v1\0{source_index}".encode()
    ).hexdigest()
    return FederationDiscoveryResult(
        discovery_id=f"discovery-unavailable-{digest[:24]}",
        federation_label="Federation discovery unavailable",
        federation_fingerprint=f"unavailable-{digest[:32]}",
        transport_kind="unknown",
        verification_code="UNAVAILABLE",
        prior_trust=False,
        state=FederationConnectionState.UNAVAILABLE,
        discovered_at=now,
        expires_at=deadline,
        unavailable_reason=reason,
    )


def _safe_failure_reason(error: Exception) -> str:
    code = getattr(error, "code", "")
    return {
        "discovery-deadline-expired": "Discovery window expired.",
        "relay-discovery-unavailable": "Relay discovery is temporarily unavailable.",
        "relay-discovery-limit-exceeded": (
            "Discovery source returned too many candidates."
        ),
        "too-many-discovery-candidates": (
            "Discovery source returned too many candidates."
        ),
    }.get(code, "Federation discovery could not be completed safely.")


@dataclass(frozen=True)
class _DiscoveryRecord:
    candidate: FederationDiscoveryCandidate
    result: FederationDiscoveryResult


class FederationOnboardingDiscoveryService:
    """Bounded discovery orchestrator that grants no authority by itself."""

    def __init__(
        self,
        authority: SessionOnboardingAuthority,
        sources: tuple[FederationDiscoverySource, ...] | list[FederationDiscoverySource],
        *,
        clock=_utc_now,
        timeout_seconds: int = 3,
        max_candidates: int = MAX_DISCOVERY_CANDIDATES,
        allow_authenticated_auto_accept: bool = False,
    ) -> None:
        values = tuple(sources)
        if len(values) > MAX_DISCOVERY_SOURCES:
            raise FederationValidationError(
                "too-many-discovery-sources",
                "sources",
                f"must contain at most {MAX_DISCOVERY_SOURCES} sources",
            )
        self.authority = authority
        self.sources = values
        self._clock = clock
        self.timeout_seconds = _positive_bound(
            timeout_seconds,
            "timeout_seconds",
            maximum=MAX_DISCOVERY_TIMEOUT_SECONDS,
        )
        self.max_candidates = _positive_bound(
            max_candidates,
            "max_candidates",
            maximum=MAX_DISCOVERY_CANDIDATES,
        )
        if not isinstance(allow_authenticated_auto_accept, bool):
            raise FederationValidationError(
                "invalid-boolean",
                "allow_authenticated_auto_accept",
                "must be a boolean",
            )
        self.allow_authenticated_auto_accept = allow_authenticated_auto_accept
        self._records: dict[str, _DiscoveryRecord] = {}
        self._attempted = False
        self._blocked_local_creation = False

    @staticmethod
    def _candidate_score(record: _DiscoveryRecord) -> tuple[int, int, datetime]:
        return (
            int(record.result.state is not FederationConnectionState.UNAVAILABLE),
            int(record.result.prior_trust),
            record.result.expires_at,
        )

    def discover(self) -> tuple[FederationDiscoveryResult, ...]:
        """Collect bounded safe results and retain authority material privately."""

        now = self._clock().astimezone(timezone.utc)
        deadline = now + timedelta(seconds=self.timeout_seconds)
        selected: dict[str, _DiscoveryRecord] = {}
        failures: list[FederationDiscoveryResult] = []
        self._records = {}
        self._attempted = True
        self._blocked_local_creation = False

        for source_index, source in enumerate(self.sources):
            if self._clock().astimezone(timezone.utc) >= deadline:
                failures.append(
                    _safe_failure_result(
                        source_index=source_index,
                        now=now,
                        deadline=deadline,
                        reason="Discovery window expired.",
                    )
                )
                self._blocked_local_creation = True
                break
            try:
                candidates = source.discover(now=now, deadline=deadline)
            except (FederationValidationError, FederationOperationError) as error:
                failures.append(
                    _safe_failure_result(
                        source_index=source_index,
                        now=now,
                        deadline=deadline,
                        reason=_safe_failure_reason(error),
                    )
                )
                self._blocked_local_creation = True
                continue
            except Exception:  # noqa: BLE001 - sources are an untrusted boundary
                failures.append(
                    _safe_failure_result(
                        source_index=source_index,
                        now=now,
                        deadline=deadline,
                        reason="Federation discovery could not be completed safely.",
                    )
                )
                self._blocked_local_creation = True
                continue

            for candidate in candidates:
                if not isinstance(candidate, FederationDiscoveryCandidate):
                    failures.append(
                        _safe_failure_result(
                            source_index=source_index,
                            now=now,
                            deadline=deadline,
                            reason=(
                                "Federation discovery could not be completed safely."
                            ),
                        )
                    )
                    self._blocked_local_creation = True
                    break
                result = candidate.to_result(now=now)
                record = _DiscoveryRecord(candidate=candidate, result=result)
                current = selected.get(result.discovery_id)
                if current is None or self._candidate_score(record) > self._candidate_score(
                    current
                ):
                    selected[result.discovery_id] = record
                if len(selected) >= self.max_candidates:
                    break
            if len(selected) >= self.max_candidates:
                break

        for discovery_id, record in selected.items():
            if record.result.state is FederationConnectionState.UNAVAILABLE:
                self._blocked_local_creation = True
            else:
                self._records[discovery_id] = record

        results = [record.result for record in selected.values()]
        results.extend(failures)
        return tuple(
            sorted(
                results,
                key=lambda result: (
                    result.state is FederationConnectionState.UNAVAILABLE,
                    result.federation_label.casefold(),
                    result.federation_fingerprint,
                    result.discovery_id,
                ),
            )
        )

    def _select(self, discovery_id: str | None) -> _DiscoveryRecord | None:
        if not self._attempted:
            raise FederationOperationError(
                "onboarding-discovery-required",
                "federation discovery must run before connection",
                "discovery_id",
            )
        now = self._clock().astimezone(timezone.utc)
        expired = [
            key
            for key, record in self._records.items()
            if record.result.expires_at <= now
        ]
        for key in expired:
            del self._records[key]
        if expired:
            self._blocked_local_creation = True
        if discovery_id is not None:
            record = self._records.get(discovery_id)
            if record is None:
                raise FederationOperationError(
                    "unknown-discovery-candidate",
                    "the selected discovery candidate is unavailable or expired",
                    "discovery_id",
                )
            return record
        if len(self._records) == 1:
            return next(iter(self._records.values()))
        if len(self._records) > 1:
            raise FederationOperationError(
                "candidate-selection-required",
                "several federation candidates require an explicit selection",
                "discovery_id",
            )
        return None

    def connect(
        self,
        identity: NodeIdentity,
        *,
        request_id: str,
        discovery_id: str | None = None,
        verification_code: str | None = None,
        local_display_name: str = "Local FCP federation",
        local_session_id: str | None = None,
    ) -> FederationSessionBinding:
        """Join one selected candidate or create locally when none exists."""

        record = self._select(discovery_id)
        if record is None:
            if self._blocked_local_creation:
                raise FederationOperationError(
                    "onboarding-discovery-unavailable",
                    "local federation creation is blocked until discovery succeeds",
                    "discovery",
                )
            return self.authority.create_local(
                identity,
                display_name=local_display_name,
                request_id=request_id,
                session_id=local_session_id,
            )

        verification_required = not record.result.prior_trust and not (
            self.allow_authenticated_auto_accept
            and record.candidate.auto_accept_authorized
        )
        if verification_required and (
            not isinstance(verification_code, str)
            or not hmac.compare_digest(
                verification_code,
                record.result.verification_code,
            )
        ):
            raise AuthenticationError(
                "onboarding-verification-failed",
                "the federation verification code did not match",
                "verification_code",
            )
        return self.authority.verified_join(
            identity,
            record.candidate.join_material,
            request_id=request_id,
            expected_federation_id=record.candidate.federation_id,
        )

    def reconnect(
        self,
        identity: NodeIdentity,
        binding: FederationSessionBinding,
    ) -> FederationSessionBinding:
        """Revalidate saved trust without creating tokens or memberships."""

        return self.authority.reconnect(identity, binding)
