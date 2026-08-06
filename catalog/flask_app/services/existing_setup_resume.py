"""Refresh an existing MSH setup after a code update.

This workflow deliberately reuses durable identity, Federation membership and
operator choices. It never creates a device identity, creates a Federation, or
replaces authority. After reconnecting, it refreshes local inspection and
benchmark evidence and reconciles existing contribution intent.
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from typing import Callable

from catalog.federation.errors import (
    AuthenticationError,
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
)

_TERMINAL_RECONNECT_CODES = frozenset(
    {
        "malformed-remote-pairing-state",
        "not-session-member",
        "pairing-actor-mismatch",
        "pairing-membership-mismatch",
        "pairing-membership-missing",
        "revoked-node",
        "unknown-node",
    }
)


class ExistingSetupRequired(RuntimeError):
    """No durable identity and Federation binding are available to resume."""


class ExistingSetupResumeError(RuntimeError):
    """The existing setup could not be refreshed safely."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class ResumeReport:
    device_id: str
    federation_id: str
    inspection_revision: int
    benchmark_runs: tuple[tuple[str, str, str], ...]
    unavailable_benchmarks: tuple[tuple[str, str], ...]
    warnings: tuple[str, ...]
    reconciled_contributions: int

    @property
    def partial(self) -> bool:
        return bool(self.warnings or self.unavailable_benchmarks)


class ExistingSetupResumeService:
    """Reconnect and refresh only already-authorized local state."""

    def __init__(
        self,
        *,
        onboarding_service: object,
        inspection_service: object,
        benchmark_service: object,
        contribution_service: object | None = None,
        reconnect_timeout_seconds: float = 90.0,
        reconnect_interval_seconds: float = 2.0,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if reconnect_timeout_seconds <= 0 or reconnect_interval_seconds <= 0:
            raise ValueError("reconnect timing must be positive")
        self.onboarding_service = onboarding_service
        self.inspection_service = inspection_service
        self.benchmark_service = benchmark_service
        self.contribution_service = contribution_service
        self.reconnect_timeout_seconds = float(reconnect_timeout_seconds)
        self.reconnect_interval_seconds = float(reconnect_interval_seconds)
        self._monotonic = monotonic
        self._sleep = sleep

    @staticmethod
    def _code(exc: BaseException, fallback: str) -> str:
        value = getattr(exc, "code", fallback)
        return str(value or fallback)

    def _saved_context(self) -> object:
        identity_loader = getattr(self.onboarding_service, "identity_or_none", None)
        binding_loader = getattr(self.onboarding_service, "binding_or_none", None)
        context_loader = getattr(self.onboarding_service, "authorized_context", None)
        if not all(callable(value) for value in (identity_loader, binding_loader, context_loader)):
            raise ExistingSetupResumeError("resume-service-unavailable")

        identity = identity_loader()
        binding = binding_loader()
        if identity is None or binding is None:
            raise ExistingSetupRequired(
                "no saved identity and Federation binding are available"
            )

        deadline = self._monotonic() + self.reconnect_timeout_seconds
        last_code = "federation-reconnect-unavailable"
        while True:
            try:
                context = context_loader()
                if context is not None:
                    return context
                last_code = "federation-context-unavailable"
            except (
                AuthenticationError,
                AuthorizationError,
                FederationOperationError,
                FederationValidationError,
                OSError,
                TimeoutError,
            ) as exc:
                last_code = self._code(exc, "federation-reconnect-unavailable")
                if last_code in _TERMINAL_RECONNECT_CODES:
                    raise ExistingSetupResumeError(last_code) from exc
            if self._monotonic() >= deadline:
                raise ExistingSetupResumeError(last_code)
            self._sleep(self.reconnect_interval_seconds)

    def resume(self) -> ResumeReport:
        context = self._saved_context()
        credentials = getattr(context, "credentials", None)
        binding = getattr(context, "binding", None)
        identity = getattr(credentials, "identity", None)
        device_id = str(getattr(identity, "node_id", "") or "")
        federation_id = str(getattr(binding, "federation_id", "") or "")
        if not device_id or not federation_id:
            raise ExistingSetupResumeError("invalid-authorized-context")

        inspection_runner = getattr(self.inspection_service, "run", None)
        if not callable(inspection_runner):
            raise ExistingSetupResumeError("inspection-service-unavailable")
        try:
            snapshot = inspection_runner()
        except (
            AuthenticationError,
            AuthorizationError,
            FederationOperationError,
            FederationValidationError,
            OSError,
            TimeoutError,
        ) as exc:
            raise ExistingSetupResumeError(
                self._code(exc, "inspection-refresh-failed")
            ) from exc

        revision = getattr(snapshot, "revision", None)
        if isinstance(revision, bool) or not isinstance(revision, int) or revision <= 0:
            raise ExistingSetupResumeError("invalid-inspection-refresh")

        planner = getattr(self.benchmark_service, "plan", None)
        runner = getattr(self.benchmark_service, "run", None)
        if not callable(planner) or not callable(runner):
            raise ExistingSetupResumeError("benchmark-service-unavailable")

        try:
            plan = tuple(planner(snapshot))
        except (
            FederationOperationError,
            FederationValidationError,
            OSError,
            TimeoutError,
        ) as exc:
            raise ExistingSetupResumeError(
                self._code(exc, "benchmark-plan-failed")
            ) from exc

        runs: list[tuple[str, str, str]] = []
        unavailable: list[tuple[str, str]] = []
        warnings: list[str] = []
        for item in plan:
            benchmark_id = str(getattr(item, "benchmark_id", "") or "")
            target_service_id = str(
                getattr(item, "target_service_id", "") or ""
            )
            if not benchmark_id or not target_service_id:
                warnings.append("invalid-benchmark-plan-item")
                continue
            if not bool(getattr(item, "runnable", False)):
                unavailable.append((benchmark_id, target_service_id))
                continue
            try:
                result = runner(
                    benchmark_id=benchmark_id,
                    target_service_id=target_service_id,
                )
                state = getattr(getattr(result, "state", None), "value", None)
                runs.append(
                    (
                        benchmark_id,
                        target_service_id,
                        str(state or getattr(result, "state", "unknown")),
                    )
                )
            except (
                AuthenticationError,
                AuthorizationError,
                FederationOperationError,
                FederationValidationError,
                OSError,
                TimeoutError,
            ) as exc:
                warnings.append(self._code(exc, "benchmark-refresh-failed"))

        reconciled = 0
        contribution = self.contribution_service
        if contribution is not None:
            has_intents = getattr(contribution, "has_persisted_intents", None)
            reconcile = getattr(contribution, "reconcile", None)
            try:
                if callable(has_intents) and has_intents() and callable(reconcile):
                    reconciled = len(tuple(reconcile()))
            except (
                AuthenticationError,
                AuthorizationError,
                FederationOperationError,
                FederationValidationError,
                OSError,
                TimeoutError,
            ) as exc:
                warnings.append(
                    self._code(exc, "contribution-reconciliation-failed")
                )

        return ResumeReport(
            device_id=device_id,
            federation_id=federation_id,
            inspection_revision=revision,
            benchmark_runs=tuple(runs),
            unavailable_benchmarks=tuple(unavailable),
            warnings=tuple(dict.fromkeys(warnings)),
            reconciled_contributions=reconciled,
        )


def _print_report(report: ResumeReport) -> None:
    print("Existing MSH setup refreshed safely.", flush=True)
    print(f"  Inspection revision: {report.inspection_revision}", flush=True)
    print(f"  Benchmarks completed: {len(report.benchmark_runs)}", flush=True)
    for benchmark_id, target, state in report.benchmark_runs:
        print(f"    - {benchmark_id} / {target}: {state}", flush=True)
    if report.unavailable_benchmarks:
        print(
            f"  Benchmarks unavailable: {len(report.unavailable_benchmarks)}",
            flush=True,
        )
    print(
        f"  Contribution intents reconciled: {report.reconciled_contributions}",
        flush=True,
    )
    for warning in report.warnings:
        print(f"  Warning: {warning}", flush=True)


def main() -> int:
    from catalog.flask_app.app import create_app
    from catalog.flask_app.services.capability_benchmark_service import (
        get_capability_benchmark_service,
    )
    from catalog.flask_app.services.capability_contribution_service import (
        get_capability_contribution_service,
    )
    from catalog.flask_app.services.capability_inspection_service import (
        get_capability_inspection_service,
    )
    from catalog.flask_app.services.capability_onboarding_service import (
        get_capability_onboarding_service,
    )

    app = create_app()
    with app.app_context():
        service = ExistingSetupResumeService(
            onboarding_service=get_capability_onboarding_service(),
            inspection_service=get_capability_inspection_service(),
            benchmark_service=get_capability_benchmark_service(),
            contribution_service=get_capability_contribution_service(),
        )
        try:
            report = service.resume()
        except ExistingSetupRequired:
            print(
                "No saved MSH identity and Federation membership were found. "
                "First-time onboarding is required.",
                flush=True,
            )
            return 2
        except ExistingSetupResumeError as exc:
            print(
                f"Existing MSH setup needs repair: {exc.code}",
                flush=True,
            )
            return 3
        except Exception as exc:  # noqa: BLE001 - CLI boundary must not leak secrets
            print(
                "Existing MSH setup refresh failed safely: "
                f"{type(exc).__name__}",
                flush=True,
            )
            return 3

    _print_report(report)
    return 4 if report.partial else 0


if __name__ == "__main__":
    sys.exit(main())


__all__ = [
    "ExistingSetupRequired",
    "ExistingSetupResumeError",
    "ExistingSetupResumeService",
    "ResumeReport",
    "main",
]
