"""Local headless Federation onboarding commands for operators and acceptance.

This module is intentionally a thin CLI over the installed Flask onboarding and
pairing services. It does not add a second Federation protocol or authority
source. The canonical host ``fcp`` CLI uses ``join-stdin`` so an automatically
obtained Tailscale pairing token never appears in a process argument or shell
history.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass
from typing import Any

from catalog.federation.errors import (
    AuthenticationError,
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)
from catalog.flask_app.app import create_app
from catalog.node.identity import NodeIdentityStateError

from .capability_onboarding_service import get_capability_onboarding_service
from .federation_pairing_service import PairingAwareCapabilityOnboardingService


@dataclass(frozen=True)
class HeadlessFederationResult:
    action: str
    state: str
    node_id: str | None
    federation_id: str | None
    session_id: str | None
    pairing_code: str | None = None

    def public_dict(self) -> dict[str, Any]:
        value = asdict(self)
        if self.action != "pairing-code":
            value.pop("pairing_code", None)
        return value


def _service() -> PairingAwareCapabilityOnboardingService:
    service = get_capability_onboarding_service()
    if not isinstance(service, PairingAwareCapabilityOnboardingService):
        raise FederationOperationError(
            "headless-pairing-service-unavailable",
            "the installed authenticated Federation pairing service is unavailable",
        )
    return service


def _result_from_binding(
    *,
    action: str,
    state: str,
    node_id: str,
    binding: Any,
    pairing_code: str | None = None,
) -> HeadlessFederationResult:
    return HeadlessFederationResult(
        action=action,
        state=state,
        node_id=node_id,
        federation_id=str(binding.federation_id),
        session_id=str(binding.internal_session_id),
        pairing_code=pairing_code,
    )


def create_first_federation() -> HeadlessFederationResult:
    service = _service()
    credentials = service.create_identity()
    existing = service.binding_or_none()
    if existing is not None:
        if service.remote_store.load() is not None:
            raise FederationOperationError(
                "headless-create-join-only-device",
                "this installation already joined a remote Federation; it cannot become a new first node",
                "binding",
            )
        context = service.authorized_context()
        if context is None:
            raise FederationOperationError(
                "headless-create-existing-binding-unavailable",
                "the existing local Federation binding could not be revalidated",
                "binding",
            )
        return _result_from_binding(
            action="create",
            state="already-connected",
            node_id=credentials.identity.node_id,
            binding=context.binding,
        )

    binding = service.connect(
        request_id=f"headless-create-{os.urandom(12).hex()}",
    )
    return _result_from_binding(
        action="create",
        state="created",
        node_id=credentials.identity.node_id,
        binding=binding,
    )


def join_existing_federation(code: str) -> HeadlessFederationResult:
    normalized = str(code or "").strip()
    if not normalized.startswith("FCP1-"):
        raise FederationValidationError(
            "headless-pairing-code-required",
            "pairing_code",
            "a signed FCP1 pairing token is required",
        )

    service = _service()
    existing = service.binding_or_none()
    if existing is not None:
        raise FederationOperationError(
            "headless-existing-binding-preserved",
            "this installation already has a Federation binding; join will not replace it",
            "binding",
        )

    binding = service.redeem_pairing_code(normalized)
    credentials = service.identity_or_none()
    if credentials is None:
        raise FederationOperationError(
            "headless-identity-missing-after-join",
            "the joined device identity could not be reloaded",
            "device_id",
        )
    return _result_from_binding(
        action="join",
        state="connected",
        node_id=credentials.identity.node_id,
        binding=binding,
    )


def create_pairing_code(relay_url: str) -> HeadlessFederationResult:
    service = _service()
    context = service.authorized_context()
    if context is None:
        raise FederationOperationError(
            "headless-pairing-federation-required",
            "start the first normal FCP Federation node before requesting a pairing code",
            "binding",
        )
    code = service.create_pairing_code(relay_url=relay_url)
    return _result_from_binding(
        action="pairing-code",
        state="issued",
        node_id=context.credentials.identity.node_id,
        binding=context.binding,
        pairing_code=code,
    )


def federation_status() -> HeadlessFederationResult:
    service = _service()
    credentials = service.identity_or_none()
    binding = service.binding_or_none()
    if credentials is None:
        return HeadlessFederationResult(
            action="status",
            state="identity-missing",
            node_id=None,
            federation_id=None,
            session_id=None,
        )
    if binding is None:
        return HeadlessFederationResult(
            action="status",
            state="not-connected",
            node_id=credentials.identity.node_id,
            federation_id=None,
            session_id=None,
        )
    context = service.authorized_context()
    if context is None:
        return _result_from_binding(
            action="status",
            state="reconnect-required",
            node_id=credentials.identity.node_id,
            binding=binding,
        )
    return _result_from_binding(
        action="status",
        state="connected",
        node_id=credentials.identity.node_id,
        binding=context.binding,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Headless local FCP Federation onboarding.",
        allow_abbrev=False,
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print one deterministic JSON object instead of human-readable lines.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("create", help="Create/reuse the first local Federation.")
    join = subparsers.add_parser(
        "join",
        help="Join with an explicitly supplied signed one-use FCP1 token.",
    )
    join.add_argument("pairing_code", metavar="FCP1-...")
    subparsers.add_parser(
        "join-stdin",
        help="Join with a signed one-use FCP1 token read from standard input.",
    )
    code = subparsers.add_parser(
        "pairing-code",
        help="Create a one-use pairing code from the local Federation authority.",
    )
    code.add_argument("--relay-url", required=True, help="Reachable ws:// or wss:// relay root.")
    subparsers.add_parser("status", help="Report the saved/revalidated Federation state.")
    return parser


def _emit(result: HeadlessFederationResult, *, as_json: bool) -> None:
    value = result.public_dict()
    if as_json:
        print(json.dumps(value, sort_keys=True, separators=(",", ":")))
        return
    print(f"Action:      {result.action}")
    print(f"State:       {result.state}")
    if result.node_id:
        print(f"Device:      {result.node_id}")
    if result.federation_id:
        print(f"Federation:  {result.federation_id}")
    if result.session_id:
        print(f"Session:     {result.session_id}")
    if result.action == "pairing-code" and result.pairing_code:
        print("Pairing code:")
        print(result.pairing_code)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    app = create_app()
    try:
        with app.app_context():
            if args.command == "create":
                result = create_first_federation()
            elif args.command == "join":
                result = join_existing_federation(args.pairing_code)
            elif args.command == "join-stdin":
                result = join_existing_federation(sys.stdin.readline())
            elif args.command == "pairing-code":
                result = create_pairing_code(args.relay_url)
            else:
                result = federation_status()
    except (
        AuthenticationError,
        AuthorizationError,
        FederationOperationError,
        FederationValidationError,
        ProtocolCompatibilityError,
        NodeIdentityStateError,
        OSError,
        RuntimeError,
        TimeoutError,
    ) as exc:
        code = str(getattr(exc, "code", type(exc).__name__))
        message = str(getattr(exc, "message", str(exc) or type(exc).__name__))
        if args.json:
            print(
                json.dumps(
                    {"accepted": False, "error": code, "message": message},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                file=sys.stderr,
            )
        else:
            print(f"Headless Federation command failed ({code}): {message}", file=sys.stderr)
        return 2

    _emit(result, as_json=args.json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
