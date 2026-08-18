"""Local CLI seams for one-time Federation initialization and trusted auto-join.

Human credentials belong to the Federation creator and are created exactly once.
They are never used as device-enrollment credentials.  Later devices redeem only
the existing short-lived pairing grant produced by the verified Tailscale host
responder.

The password accepted by ``initialize`` is read from stdin (or a hidden local
TTY prompt) and is never accepted as an argument or environment variable.
"""

from __future__ import annotations

import argparse
import getpass
import json
import sys
import uuid
from dataclasses import asdict

from flask_security import hash_password

from catalog.federation.errors import (
    AuthenticationError,
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)
from catalog.federation.tailnet_join_bridge import clear_grant, grant_path, load_grant
from catalog.node.identity import NodeIdentityStateError

from catalog.flask_app.app import create_app
from catalog.flask_app.auth.models import Role, User, db
from catalog.flask_app.auth.routes import (
    _commit_first_user,
    _normalized_email,
    _publish_user_best_effort,
)

from .capability_onboarding_service import get_capability_onboarding_service
from .federation_pairing_service import PairingAwareCapabilityOnboardingService
from .headless_federation_cli import (
    HeadlessFederationResult,
    create_first_federation,
    federation_status,
)


def _service() -> PairingAwareCapabilityOnboardingService:
    service = get_capability_onboarding_service()
    if not isinstance(service, PairingAwareCapabilityOnboardingService):
        raise FederationOperationError(
            "zero-touch-pairing-service-unavailable",
            "the installed authenticated Federation pairing service is unavailable",
        )
    return service


def _has_users() -> bool:
    return db.session.query(User.id).limit(1).first() is not None


def _has_active_admin() -> bool:
    return (
        db.session.query(User.id)
        .join(User.roles)
        .filter(User.active.is_(True), Role.name == "admin")
        .limit(1)
        .first()
        is not None
    )


def _read_password() -> str:
    if sys.stdin.isatty():
        return getpass.getpass("Federation administrator password: ")
    value = sys.stdin.readline()
    if value == "":
        raise FederationValidationError(
            "zero-touch-password-required",
            "password",
            "a password must be supplied on stdin",
        )
    return value.rstrip("\r\n")


def initialize_first_federation(
    *,
    email: str,
    password: str,
) -> HeadlessFederationResult:
    """Create the sole first human administrator, then the first Federation."""

    if _has_active_admin():
        # Idempotent retry after the administrator committed but before the
        # Federation creation result reached the host.
        return create_first_federation()
    if _has_users():
        raise FederationOperationError(
            "zero-touch-existing-users-without-admin",
            "human users already exist but no active administrator is available",
            "human_admin",
        )

    normalized = _normalized_email(email)
    if normalized is None:
        raise FederationValidationError(
            "zero-touch-invalid-email",
            "email",
            "enter a valid administrator email address",
        )
    if len(password) < 12:
        raise FederationValidationError(
            "zero-touch-password-too-short",
            "password",
            "the administrator password must contain at least 12 characters",
        )

    admin_role = db.session.query(Role).filter_by(name="admin").one_or_none()
    if admin_role is None:
        raise FederationOperationError(
            "zero-touch-admin-role-unavailable",
            "the seeded administrator role is unavailable",
            "human_admin",
        )

    user = User(
        email=normalized,
        password=hash_password(password),
        active=True,
        fs_uniquifier=uuid.uuid4().hex,
        roles=[admin_role],
    )
    if not _commit_first_user(user):
        if _has_active_admin():
            return create_first_federation()
        raise FederationOperationError(
            "zero-touch-admin-bootstrap-conflict",
            "the first-administrator bootstrap could not be committed safely",
            "human_admin",
        )

    _publish_user_best_effort(user)
    return create_first_federation()


def redeem_pending_tailnet_grant() -> HeadlessFederationResult:
    """Redeem the host-authorized tailnet grant through normal pairing authority."""

    service = _service()
    credentials = service.identity_or_none()
    remote = service.remote_store.load()
    if remote is not None:
        clear_grant(grant_path())
        status = federation_status()
        if status.state != "connected":
            raise FederationOperationError(
                "zero-touch-saved-membership-unavailable",
                "saved Federation membership could not be revalidated",
                "binding",
            )
        return status

    path = grant_path()
    code = load_grant(path)
    if not code:
        raise FederationOperationError(
            "zero-touch-auto-join-grant-required",
            "the verified Tailscale responder did not provide an enrollment grant",
            "binding",
        )

    # The grant is one-use.  Clear it before redemption so an exception, process
    # crash, or retry can never turn it into a reusable local credential.
    clear_grant(path)
    binding = service.redeem_pairing_code(code)
    credentials = service.identity_or_none() or credentials
    if credentials is None:
        raise FederationOperationError(
            "zero-touch-identity-missing-after-join",
            "the joined device identity could not be reloaded",
            "device_id",
        )
    return HeadlessFederationResult(
        action="auto-join",
        state="connected",
        node_id=credentials.identity.node_id,
        federation_id=str(binding.federation_id),
        session_id=str(binding.internal_session_id),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="One-time FCP Federation initialization and trusted auto-join.",
        allow_abbrev=False,
    )
    parser.add_argument("--json", action="store_true")
    commands = parser.add_subparsers(dest="command", required=True)

    initialize = commands.add_parser(
        "initialize",
        help="Create the first Federation administrator and initialize the first Federation.",
    )
    initialize.add_argument("--email", required=True)
    commands.add_parser(
        "redeem-auto-join",
        help="Redeem a verified Tailscale host grant through the existing pairing flow.",
    )
    commands.add_parser("status", help="Report current Federation binding state.")
    return parser


def _emit(result: HeadlessFederationResult, *, as_json: bool) -> None:
    value = result.public_dict()
    if as_json:
        print(json.dumps(value, sort_keys=True, separators=(",", ":")))
        return
    print(f"State:       {result.state}")
    if result.node_id:
        print(f"Device:      {result.node_id}")
    if result.federation_id:
        print(f"Federation:  {result.federation_id}")
    if result.session_id:
        print(f"Session:     {result.session_id}")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    app = create_app()
    password: str | None = None
    try:
        with app.app_context():
            if args.command == "initialize":
                password = _read_password()
                result = initialize_first_federation(
                    email=args.email,
                    password=password,
                )
            elif args.command == "redeem-auto-join":
                result = redeem_pending_tailnet_grant()
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
        ValueError,
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
            print(f"Zero-touch Federation setup failed ({code}): {message}", file=sys.stderr)
        return 2
    finally:
        password = None

    _emit(result, as_json=args.json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "initialize_first_federation",
    "redeem_pending_tailnet_grant",
    "main",
]
