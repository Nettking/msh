"""Reconnect an existing FCP setup after a code update.

This workflow deliberately reuses durable identity, Federation membership,
inspection evidence, benchmark evidence, and operator choices. It never creates
a device identity, creates a Federation, replaces authority, runs inspection,
executes benchmarks, or reconciles contribution authority. The long-running
Flask app owns the single evidence-gated contribution reconciliation.
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
    """The existing setup could not be resumed safely."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class ResumeReport:
    device_id: str
    federation_id: str
    inspection_revision: int
    # The historical field name is retained for compatibility. These entries
    # describe saved benchmark evidence reused during resume; no run occurs.
    benchmark_runs: tuple[tuple[str, str, str], ...]
    unavailable_benchmarks: tuple[tuple[str, str], ...]
    warnings: tuple[str, ...]
    reconciled_contributions: int

    @property
    def partial(self) -> bool:
        return bool(self.warnings or self.unavailable_benchmarks)


class ExistingSetupResumeService:
    """Reconnect and reuse only already-authorized, already-persisted state."""

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
        progress: Callable[[str], None] | None = None,
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
        self._progress = progress or (lambda _message: None)

    def _say(self, message: str) -> None:
        self._progress(message)

    @staticmethod
    def _code(exc: BaseException, fallback: str) -> str:
        value = getattr(exc, "code", fallback)
        return str(value or fallback)

    def _saved_context(self) -> object:
        identity_loader = getattr(self.onboarding_service, "identity_or_none", None)
        binding_loader = getattr(self.onboarding_service, "binding_or_none", None)
        reconnect = getattr(self.onboarding_service, "reconnect", None)
        context_loader = getattr(self.onboarding_service, "authorized_context", None)
        if not all(
            callable(value)
            for value in (identity_loader, binding_loader, context_loader)
        ):
            raise ExistingSetupResumeError("resume-service-unavailable")

        identity = identity_loader()
        binding = binding_loader()
        if identity is None or binding is None:
            raise ExistingSetupRequired(
                "no saved identity and Federation binding are available"
            )

        self._say("[1/4] Reconnecting the saved Federation membership...")
        deadline = self._monotonic() + self.reconnect_timeout_seconds
        last_code = "federation-reconnect-unavailable"
        attempt = 0
        while True:
            attempt += 1
            try:
                if callable(reconnect):
                    reconnect()
                context = context_loader()
                if context is not None:
                    self._say(
                        f"      Federation membership verified after {attempt} attempt"
                        f"{'s' if attempt != 1 else ''}."
                    )
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
                self._say(
                    f"      Reconnect attempt {attempt} did not complete: {last_code}."
                )
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

        warnings: list[str] = []
        inspection_loader = getattr(self.inspection_service, "load", None)
        if not callable(inspection_loader):
            raise ExistingSetupResumeError("inspection-service-unavailable")
        self._say("[2/4] Loading the saved device inspection evidence...")
        try:
            snapshot = inspection_loader()
        except (
            AuthenticationError,
            AuthorizationError,
            FederationOperationError,
            FederationValidationError,
            OSError,
            TimeoutError,
        ) as exc:
            raise ExistingSetupResumeError(
                self._code(exc, "inspection-evidence-load-failed")
            ) from exc

        revision = 0
        if snapshot is None:
            warnings.append("inspection-evidence-missing")
            self._say(
                "      No saved inspection evidence was found; inspection was not "
                "run automatically."
            )
        else:
            snapshot_device_id = str(getattr(snapshot, "device_id", device_id) or "")
            if snapshot_device_id != device_id:
                raise ExistingSetupResumeError("inspection-device-mismatch")
            revision_value = getattr(snapshot, "revision", None)
            if (
                isinstance(revision_value, bool)
                or not isinstance(revision_value, int)
                or revision_value <= 0
            ):
                raise ExistingSetupResumeError("invalid-inspection-evidence")
            revision = revision_value
            state_loader = getattr(self.inspection_service, "state", None)
            inspection_state = (
                str(state_loader(snapshot)) if callable(state_loader) else "current"
            )
            if inspection_state == "expired":
                warnings.append("inspection-evidence-expired")
                self._say(
                    f"      Inspection revision {revision} is saved but expired; "
                    "it was not rerun automatically."
                )
            else:
                self._say(f"      Reusing inspection revision {revision}.")

        result_loader = getattr(self.benchmark_service, "list_results", None)
        if not callable(result_loader):
            raise ExistingSetupResumeError("benchmark-service-unavailable")
        self._say("[3/4] Loading saved benchmark evidence without executing checks...")
        try:
            saved_results = tuple(result_loader())
        except (
            AuthenticationError,
            AuthorizationError,
            FederationOperationError,
            FederationValidationError,
            OSError,
            TimeoutError,
        ) as exc:
            raise ExistingSetupResumeError(
                self._code(exc, "benchmark-evidence-load-failed")
            ) from exc

        runs: list[tuple[str, str, str]] = []
        for result in saved_results:
            if str(getattr(result, "device_id", "") or "") != device_id:
                continue
            benchmark_id = str(getattr(result, "benchmark_id", "") or "")
            target_service_id = str(
                getattr(result, "target_service_id", "") or ""
            )
            state = getattr(getattr(result, "state", None), "value", None)
            state_text = str(state or getattr(result, "state", "unknown"))
            if not benchmark_id or not target_service_id:
                warnings.append("invalid-benchmark-evidence")
                continue
            runs.append((benchmark_id, target_service_id, state_text))
        self._say(
            f"      Reusing {len(runs)} saved benchmark result"
            f"{'s' if len(runs) != 1 else ''}; no benchmark was executed."
        )

        self._say("[4/4] Leaving saved contribution intent unchanged...")
        self._say(
            "      The long-running Flask app will reconcile it once, after "
            "validating saved capability evidence."
        )

        return ResumeReport(
            device_id=device_id,
            federation_id=federation_id,
            inspection_revision=revision,
            benchmark_runs=tuple(runs),
            unavailable_benchmarks=(),
            warnings=tuple(dict.fromkeys(warnings)),
            reconciled_contributions=0,
        )


def _print_report(report: ResumeReport) -> None:
    print("Existing FCP setup resumed safely.", flush=True)
    print(f"  Saved inspection revision: {report.inspection_revision}", flush=True)
    print(f"  Saved benchmark results reused: {len(report.benchmark_runs)}", flush=True)
    for benchmark_id, target, state in report.benchmark_runs:
        print(f"    - {benchmark_id} / {target}: {state}", flush=True)
    print("  Contribution authority changes: deferred to Flask startup", flush=True)
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
            progress=lambda message: print(message, flush=True),
        )
        try:
            report = service.resume()
        except ExistingSetupRequired:
            print(
                "No saved FCP identity and Federation membership were found. "
                "First-time onboarding is required.",
                flush=True,
            )
            return 2
        except ExistingSetupResumeError as exc:
            print(
                f"Existing FCP setup needs repair: {exc.code}",
                flush=True,
            )
            return 3
        except Exception as exc:  # noqa: BLE001 - CLI boundary must not leak secrets
            print(
                "Existing FCP setup resume failed safely: "
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
