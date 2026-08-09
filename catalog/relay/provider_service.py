"""Product relay composition for trusted F8 provider authority.

The Phase 2 relay remains authoritative for identity, membership, capability
announcements and authenticated routing. This supported product wrapper adds
only the already-defined F8.1 provider-enrollment and F8.2 provider-health
services beside the coordinator database. A READY capability announcement is
still metadata: it must be explicitly enrolled/approved before health can be
published or a remote provider can become executable.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sqlite3
import sys
from collections.abc import Sequence
from contextlib import suppress
from pathlib import Path
from typing import Any

from catalog.capabilities.provider_enrollment import (
    FederatedProviderEnrollmentService,
    SQLiteProviderEnrollmentStore,
)
from catalog.capabilities.provider_health import (
    FederatedProviderHealthService,
    ProviderHealthState,
    SQLiteProviderHealthStore,
)
from catalog.capabilities.provider_reports import ProviderResourceReport
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.relay.service import (
    RelayConfigurationError,
    RelayServer,
    _bounded_number,
    _build_parser,
    _payload_text,
    main as phase2_main,
)

LOGGER = logging.getLogger(__name__)

PROVIDER_ENROLLMENT_DATABASE_NAME = "provider_enrollment.sqlite3"
PROVIDER_HEALTH_DATABASE_NAME = "provider_health.sqlite3"


def provider_authority_paths(coordinator: SessionCoordinator) -> tuple[Path, Path]:
    """Return durable F8 sidecar paths beside the coordinator authority."""

    database = Path(coordinator.store.database)
    return (
        database.with_name(PROVIDER_ENROLLMENT_DATABASE_NAME),
        database.with_name(PROVIDER_HEALTH_DATABASE_NAME),
    )


def build_provider_authorities(
    coordinator: SessionCoordinator,
) -> tuple[FederatedProviderEnrollmentService, FederatedProviderHealthService]:
    enrollment_path, health_path = provider_authority_paths(coordinator)
    enrollment = FederatedProviderEnrollmentService(
        coordinator,
        SQLiteProviderEnrollmentStore(enrollment_path),
    )
    health = FederatedProviderHealthService(
        enrollment,
        SQLiteProviderHealthStore(health_path),
    )
    return enrollment, health


class ProviderAuthorityRelayServer(RelayServer):
    """RelayServer with authenticated F8.1/F8.2 product commands."""

    def __init__(self, coordinator: SessionCoordinator, **kwargs: Any) -> None:
        super().__init__(coordinator, **kwargs)
        self.provider_enrollment, self.provider_health = build_provider_authorities(
            coordinator
        )

    async def _accepted(
        self,
        record,
        request,
        *,
        message_type: str,
        payload: dict[str, Any],
        session_id: str,
    ) -> None:
        await self._send_live(
            record,
            self._response_envelope(
                request,
                message_type=message_type,
                payload=payload,
                authorization_context=self._membership_context(
                    session_id,
                    record.node_id,
                ),
            ),
        )

    async def _dispatch(self, record, request) -> None:
        message_type = request.message_type
        if not message_type.startswith("provider."):
            await super()._dispatch(record, request)
            return

        session_id = self._required_session(request)

        if message_type == "provider.enrollment.request":
            capability_id = _payload_text(request.payload, "capability_id")
            enrollment = self.provider_enrollment.request(
                session_id=session_id,
                capability_id=capability_id,
                actor_node_id=record.node_id,
                command_id=request.request_id,
            )
            await self._accepted(
                record,
                request,
                message_type="provider.enrollment.request.accepted",
                payload={"enrollment": enrollment.to_dict()},
                session_id=session_id,
            )
            return

        if message_type in {
            "provider.enrollment.approve",
            "provider.enrollment.suspend",
            "provider.enrollment.revoke",
            "provider.enrollment.reconcile",
        }:
            capability_id = _payload_text(request.payload, "capability_id")
            expected_revision = _bounded_number(
                request.payload.get("expected_revision"),
                field="expected_revision",
                minimum=1,
                maximum=2**63 - 1,
            )
            common = {
                "session_id": session_id,
                "capability_id": capability_id,
                "actor_node_id": record.node_id,
                "command_id": request.request_id,
                "expected_revision": expected_revision,
            }
            operation = message_type.rsplit(".", 1)[1]
            if operation == "approve":
                enrollment = self.provider_enrollment.approve(**common)
            elif operation == "suspend":
                enrollment = self.provider_enrollment.suspend(
                    **common,
                    reason_code=_payload_text(
                        request.payload,
                        "reason_code",
                        maximum=128,
                    ),
                )
            elif operation == "revoke":
                enrollment = self.provider_enrollment.revoke(
                    **common,
                    reason_code=_payload_text(
                        request.payload,
                        "reason_code",
                        maximum=128,
                    ),
                )
            else:
                enrollment = self.provider_enrollment.reconcile(**common)
            await self._accepted(
                record,
                request,
                message_type=f"{message_type}.accepted",
                payload={"enrollment": enrollment.to_dict()},
                session_id=session_id,
            )
            return

        if message_type == "provider.health.publish":
            report_value = request.payload.get("report")
            if not isinstance(report_value, dict):
                raise FederationValidationError(
                    "invalid-command-field",
                    "report",
                    "must contain a ProviderResourceReport object",
                )
            report = ProviderResourceReport.from_dict(report_value)
            if report.session_id != session_id:
                raise FederationValidationError(
                    "wrong-session",
                    "session_id",
                    "provider report and relay request must use the same session",
                )
            generation = _bounded_number(
                request.payload.get("provider_generation"),
                field="provider_generation",
                minimum=1,
                maximum=2**63 - 1,
            )
            health = self.provider_health.publish(
                report,
                actor_node_id=record.node_id,
                command_id=request.request_id,
                provider_generation=generation,
            )
            await self._accepted(
                record,
                request,
                message_type="provider.health.publish.accepted",
                payload={"health": health.to_dict()},
                session_id=session_id,
            )
            return

        if message_type == "provider.health.current":
            capability_type_value = request.payload.get("capability_type")
            capability_type = (
                None
                if capability_type_value is None
                else _payload_text(request.payload, "capability_type")
            )
            observations = {
                observation.capability_id: observation
                for observation in self.provider_health.observations(
                    session_id=session_id,
                    actor_node_id=record.node_id,
                    capability_type=capability_type,
                )
            }
            current = []
            for health in self.provider_health.store.list_records(
                session_id=session_id,
                capability_type=capability_type,
            ):
                observation = observations.get(health.capability_id)
                if (
                    observation is not None
                    and observation.state is ProviderHealthState.CURRENT
                ):
                    current.append(health.to_dict())
            await self._accepted(
                record,
                request,
                message_type="provider.health.current.accepted",
                payload={"health": current},
                session_id=session_id,
            )
            return

        raise FederationValidationError(
            "unsupported-message-type",
            "message_type",
            "provider authority message type is not supported",
        )


async def _serve_from_args(args: argparse.Namespace) -> None:
    coordinator = SessionCoordinator(args.database)
    relay = ProviderAuthorityRelayServer(
        coordinator,
        host=args.host,
        port=args.port,
        tls_cert=args.tls_cert,
        tls_key=args.tls_key,
        tls_key_password=None,
        unsafe_development_plaintext=args.unsafe_development_plaintext,
        auth_timeout_seconds=args.auth_timeout_seconds,
        send_timeout_seconds=args.send_timeout_seconds,
        heartbeat_timeout_seconds=args.heartbeat_timeout_seconds,
        sweep_interval_seconds=args.sweep_interval_seconds,
        max_connections=args.max_connections,
    )
    stop_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed_signals = []
    import signal

    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(shutdown_signal, stop_requested.set)
        except (NotImplementedError, RuntimeError):
            continue
        installed_signals.append(shutdown_signal)
    await relay.start()
    try:
        if installed_signals:
            await stop_requested.wait()
        elif relay._server is not None:
            await relay._server.serve_forever()
    finally:
        for shutdown_signal in installed_signals:
            with suppress(NotImplementedError, RuntimeError):
                loop.remove_signal_handler(shutdown_signal)
        await relay.stop()


def main(argv: Sequence[str] | None = None) -> int:
    values = list(sys.argv[1:] if argv is None else argv)
    if not values or values[0] != "serve":
        return phase2_main(values)
    parser = _build_parser()
    args = parser.parse_args(values)
    if args.tls_key_password_prompt:
        # Delegate the interactive encrypted-key case to the original service.
        # The Compose product path never uses this switch.
        return phase2_main(values)
    try:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        asyncio.run(_serve_from_args(args))
        return 0
    except KeyboardInterrupt:
        return 0
    except (
        FederationValidationError,
        FederationOperationError,
        RelayConfigurationError,
        OSError,
        sqlite3.Error,
    ) as error:
        code = getattr(error, "code", "relay-command-failed")
        print(f"relay command failed ({code})", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ProviderAuthorityRelayServer",
    "build_provider_authorities",
    "provider_authority_paths",
]
