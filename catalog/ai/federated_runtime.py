"""F7.7 runtime variant that consumes current F8.2 reports for remote providers."""

from __future__ import annotations

from datetime import datetime, timedelta

from catalog.capabilities.provider_reports import (
    MAX_QUEUE_DEPTH,
    UTILIZATION_SCALE,
    ProviderResourceReport,
    ProviderStatus,
)
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)

from .remote_contracts import _node_id
from .runtime import (
    MAX_REGISTERED_AI_PROVIDERS,
    LanguageModelRuntime,
    _ProviderState,
    _safe_provider_name,
)
from .runtime_contracts import _logical_id, _text


class TrustedFederatedLanguageModelRuntime(LanguageModelRuntime):
    """Use external F8.2 health without changing the F7 selection algorithm.

    Local providers retain the original synthesized runtime report. Providers
    exposing ``resource_report(now)`` contribute their current F8.2 report,
    augmented conservatively with requests reserved by this runtime instance.
    """

    def register(
        self,
        provider,
        *,
        status: ProviderStatus = ProviderStatus.READY,
    ) -> None:
        """Accept derived MSH node identities only for F8.2-backed providers."""

        if not callable(getattr(provider, "resource_report", None)):
            super().register(provider, status=status)
            return
        capability_id = _logical_id(
            getattr(provider, "capability_id", None), "capability_id"
        )
        _node_id(getattr(provider, "node_id", None), "node_id")
        provider_session = _logical_id(
            getattr(provider, "session_id", None), "provider.session_id"
        )
        if provider_session != self.session_id:
            raise FederationValidationError(
                "cross-session-ai-provider",
                "provider.session_id",
                "provider must belong to the runtime session",
            )
        _safe_provider_name(getattr(provider, "display_name", None))
        _text(
            getattr(provider, "protocol", None),
            "provider.protocol",
            maximum=128,
        )
        _text(
            getattr(provider, "protocol_version", None),
            "provider.protocol_version",
            maximum=32,
        )
        models = tuple(
            _text(item, f"provider.models[{index}]", maximum=256)
            for index, item in enumerate(getattr(provider, "models", ()))
        )
        modalities = tuple(
            _logical_id(item, f"provider.modalities[{index}]")
            for index, item in enumerate(getattr(provider, "modalities", ()))
        )
        if not models or not modalities:
            raise FederationValidationError(
                "incomplete-ai-provider",
                "provider",
                "must advertise at least one model and modality",
            )
        max_concurrent = getattr(provider, "max_concurrent_jobs", None)
        if (
            isinstance(max_concurrent, bool)
            or not isinstance(max_concurrent, int)
            or max_concurrent <= 0
        ):
            raise FederationValidationError(
                "invalid-provider-capacity",
                "max_concurrent_jobs",
                "must be a positive integer",
            )
        try:
            provider_status = ProviderStatus(status)
        except (TypeError, ValueError) as exc:
            raise FederationValidationError(
                "invalid-provider-status",
                "status",
                "unknown provider status",
            ) from exc
        with self._condition:
            if capability_id in self._providers:
                raise FederationValidationError(
                    "duplicate-ai-provider",
                    "capability_id",
                    "provider is already registered",
                )
            if len(self._providers) >= MAX_REGISTERED_AI_PROVIDERS:
                raise FederationValidationError(
                    "too-many-ai-providers",
                    "providers",
                    f"must not exceed {MAX_REGISTERED_AI_PROVIDERS}",
                )
            self._providers[capability_id] = _ProviderState(
                provider=provider,
                status=provider_status,
            )
            self._condition.notify_all()

    def _reports(self, now: datetime) -> tuple[ProviderResourceReport, ...]:
        reports: list[ProviderResourceReport] = []
        waiting = max(
            0,
            self._pending_requests
            - sum(state.active_jobs for state in self._providers.values()),
        )
        for capability_id, state in sorted(self._providers.items()):
            provider = state.provider
            source = getattr(provider, "resource_report", None)
            if callable(source):
                try:
                    external = source(now)
                except (FederationOperationError, FederationValidationError):
                    external = None
                if external is None:
                    continue
                if not isinstance(external, ProviderResourceReport):
                    raise FederationValidationError(
                        "invalid-external-provider-report",
                        "resource_report",
                        "must return ProviderResourceReport or None",
                    )
                identity = (
                    external.capability_id == capability_id
                    and external.node_id == provider.node_id
                    and external.session_id == provider.session_id
                    and external.protocol == provider.protocol
                    and external.protocol_version == provider.protocol_version
                )
                if not identity or not external.is_fresh_at(now):
                    continue
                combined_active = min(
                    external.max_concurrent_jobs,
                    external.active_jobs + state.active_jobs,
                )
                queue_depth = min(
                    MAX_QUEUE_DEPTH,
                    external.queue_depth + state.queue_depth + waiting,
                )
                utilization = max(
                    external.utilization_millis,
                    int(
                        UTILIZATION_SCALE
                        * combined_active
                        / external.max_concurrent_jobs
                    ),
                )
                status = (
                    external.status
                    if state.status is ProviderStatus.READY
                    else state.status
                )
                reports.append(
                    ProviderResourceReport(
                        capability_id=external.capability_id,
                        node_id=external.node_id,
                        session_id=external.session_id,
                        capability_type=external.capability_type,
                        protocol=external.protocol,
                        protocol_version=external.protocol_version,
                        status=status,
                        report_revision=(
                            external.report_revision + state.report_revision
                        ),
                        max_concurrent_jobs=external.max_concurrent_jobs,
                        active_jobs=combined_active,
                        queue_depth=queue_depth,
                        utilization_millis=min(
                            UTILIZATION_SCALE,
                            utilization,
                        ),
                        attributes=external.attributes,
                        reported_at=external.reported_at,
                        expires_at=external.expires_at,
                        report_protocol_version=(
                            external.report_protocol_version
                        ),
                    )
                )
                continue
            expires = now + timedelta(seconds=30)
            reports.append(
                ProviderResourceReport(
                    capability_id=capability_id,
                    node_id=provider.node_id,
                    session_id=provider.session_id,
                    capability_type="language-model",
                    protocol=provider.protocol,
                    protocol_version=provider.protocol_version,
                    status=state.status,
                    report_revision=state.report_revision,
                    max_concurrent_jobs=provider.max_concurrent_jobs,
                    active_jobs=state.active_jobs,
                    queue_depth=state.queue_depth + waiting,
                    utilization_millis=(
                        int(
                            UTILIZATION_SCALE
                            * state.active_jobs
                            / provider.max_concurrent_jobs
                        )
                    ),
                    attributes={
                        "models": list(provider.models),
                        "modalities": list(provider.modalities),
                        "features": {
                            "timeout": bool(provider.supports_timeout),
                            "cancellation": bool(
                                provider.supports_cancellation
                            ),
                        },
                    },
                    reported_at=now,
                    expires_at=expires,
                )
            )
        return tuple(reports)

    def provider_status(self) -> tuple[dict[str, object], ...]:
        now = self._clock()
        reports = {
            report.capability_id: report for report in self._reports(now)
        }
        values: list[dict[str, object]] = []
        with self._condition:
            for capability_id, state in sorted(self._providers.items()):
                provider = state.provider
                report = reports.get(capability_id)
                if report is None:
                    values.append(
                        {
                            "capability_id": capability_id,
                            "provider_name": _safe_provider_name(
                                provider.display_name
                            ),
                            "status": ProviderStatus.UNAVAILABLE.value,
                            "models": [],
                            "modalities": [],
                            "active_jobs": state.active_jobs,
                            "max_concurrent_jobs": 0,
                            "queue_depth": state.queue_depth,
                            "supports_timeout": bool(
                                provider.supports_timeout
                            ),
                            "supports_cancellation": bool(
                                provider.supports_cancellation
                            ),
                        }
                    )
                    continue
                features = report.attributes.get("features", {})
                values.append(
                    {
                        "capability_id": capability_id,
                        "provider_name": _safe_provider_name(
                            provider.display_name
                        ),
                        "status": report.status.value,
                        "models": list(
                            report.attributes.get("models", [])
                        ),
                        "modalities": list(
                            report.attributes.get("modalities", [])
                        ),
                        "active_jobs": report.active_jobs,
                        "max_concurrent_jobs": (
                            report.max_concurrent_jobs
                        ),
                        "queue_depth": report.queue_depth,
                        "supports_timeout": bool(
                            features.get("timeout", False)
                        ) if isinstance(features, dict) else False,
                        "supports_cancellation": bool(
                            features.get("cancellation", False)
                        ) if isinstance(features, dict) else False,
                    }
                )
        return tuple(values)
