"""Central human authorization policy for every FCP web endpoint."""

from __future__ import annotations

from collections.abc import Iterable

from flask import Flask, abort, current_app, redirect, request, url_for
from flask_security import current_user

from .federation import saved_remote_member
from .models import User, db

PERMISSIONS: dict[str, str] = {
    "dashboard.read": "View dashboards and runtime status",
    "data.read": "View data, analyses, playback, documentation, and system state",
    "data.upload": "Upload or import data",
    "analysis.run": "Run analyses and generate operator artifacts",
    "workflow.run": "Run workflows and change operator knowledge",
    "runtime.control": "Control normal runtime operations and source configuration",
    "recorder.control": "Control recorder operations",
    "federation.read": "View Federation status, devices, and capabilities",
    "federation.manage": "Administer Federation membership and providers",
    "pairing.manage": "Create and redeem Federation pairing grants",
    "software.update": "Check for and apply software updates",
    "users.manage": "Create, activate, deactivate, and assign roles to human users",
    "account.manage": "Change the authenticated human user's own password",
}

ROLE_PERMISSIONS: dict[str, frozenset[str]] = {
    "viewer": frozenset(
        {"dashboard.read", "data.read", "federation.read", "account.manage"}
    ),
    "operator": frozenset(
        {
            "dashboard.read",
            "data.read",
            "federation.read",
            "data.upload",
            "analysis.run",
            "workflow.run",
            "runtime.control",
            "recorder.control",
            "account.manage",
        }
    ),
    "admin": frozenset(PERMISSIONS),
}

#: One-line summaries for the user administration screen. Kept beside
#: ROLE_PERMISSIONS so the wording and the grants change together.
ROLE_SUMMARIES: dict[str, str] = {
    "viewer": "Read dashboards, data, and Federation status.",
    "operator": "Adds upload, analysis, workflow, runtime, and recorder control.",
    "admin": "Everything, including Federation administration, software updates, and user management.",
}

PUBLIC_ENDPOINTS = frozenset(
    {
        "security.login",
        "security.logout",
        "security.forgot_password",
        "security.reset_password",
        "security.verify",
        "security.static",
        # This route exists only while the local authority has zero human users.
        # Its handler closes itself as soon as the first administrator commits.
        "auth_users.bootstrap_user",
        # Host-side Tailscale discovery has no browser session. This endpoint
        # exposes only bounded public-safe metadata and never enrollment,
        # invitation, pairing, human-auth, or session authority.
        "federation_pairing_web.federation_discovery",
        # A member starts sign-in before it has a human session, and the signed
        # assertion returns to an anonymous browser. The authority endpoint is
        # intentionally not public: the leader must already authenticate the
        # human before it can issue an assertion.
        "federated_human_auth.start",
        "federated_human_auth.callback",
    }
)

# A fresh installation that is joining an existing Federation has no local human
# row yet. Permit only the browser endpoints needed to establish device identity,
# redeem a signed pairing grant, or run the login-first Tailscale enrollment
# handshake. This is conditional rather than public: it closes as soon as either
# a local human exists or a persisted remote Federation binding exists.
PRE_AUTH_FEDERATION_BOOTSTRAP_ENDPOINTS = frozenset(
    {
        "capability_onboarding_web.onboarding",
        "capability_onboarding_web.create_identity",
        "federation_pairing_web.pair_device",
        "federation_enrollment.start",
        "federation_enrollment.callback",
    }
)

ENDPOINT_PERMISSIONS: dict[str, str] = {
    # Keep both names while older/rebased branches may expose either endpoint.
    "federation_web.rename_device": "federation.manage",
    "federation_web.rename_this_device": "federation.manage",
    "federation_web.request_member_capabilities": "federation.manage",
    "federation_web.check_updates": "software.update",
    "federation_web.apply_updates": "software.update",
    "federation_pairing_web.create_pairing_code": "pairing.manage",
    "federation_pairing_web.pair_device": "pairing.manage",
    "federation_enrollment.start": "pairing.manage",
    "federation_enrollment.callback": "pairing.manage",
    "federation_enrollment.authorize": "pairing.manage",
    "auth_users.users": "users.manage",
    "auth_users.create_user": "users.manage",
    "auth_users.update_user": "users.manage",
    "security.change_password": "account.manage",
    "federated_human_auth.authorize": "account.manage",
}

READ_PERMISSIONS = {
    "docs_web": "data.read",
    "federation_web": "federation.read",
    "federation_recorder_web": "federation.read",
    "provider_federation_web": "federation.read",
    "capability_inspection_web": "federation.read",
    "capability_onboarding_web": "federation.read",
    "capability_contribution_web": "federation.read",
    "capability_benchmark_web": "federation.read",
    "capability_startup_transition_web": "federation.read",
    "data_upload_web": "data.read",
    "source_web": "data.read",
    "operator_strategy_web": "data.read",
    "operator_support_web": "data.read",
    "ai_web": "data.read",
    "server_setup_web": "dashboard.read",
    "web": "dashboard.read",
}

WRITE_PERMISSIONS = {
    "data_upload_web": "data.upload",
    "source_web": "runtime.control",
    "operator_strategy_web": "workflow.run",
    "operator_support_web": "workflow.run",
    "ai_web": "analysis.run",
    "server_setup_web": "runtime.control",
    "provider_federation_web": "federation.manage",
    "capability_inspection_web": "runtime.control",
    "capability_benchmark_web": "analysis.run",
    "capability_contribution_web": "federation.manage",
    "capability_onboarding_web": "federation.manage",
    "capability_startup_transition_web": "federation.manage",
    "federation_recorder_web": "recorder.control",
    "web": "runtime.control",
}


def permission_for(endpoint: str, method: str) -> str | None:
    if endpoint in PUBLIC_ENDPOINTS or endpoint == "static":
        return None
    exact = ENDPOINT_PERMISSIONS.get(endpoint)
    if exact:
        return exact
    blueprint = endpoint.partition(".")[0]
    if method in {"GET", "HEAD", "OPTIONS"}:
        return READ_PERMISSIONS.get(blueprint)
    return WRITE_PERMISSIONS.get(blueprint)


def pre_auth_federation_bootstrap_allowed(endpoint: str | None) -> bool:
    """Return whether a fresh device may use one bounded pairing endpoint."""

    if endpoint not in PRE_AUTH_FEDERATION_BOOTSTRAP_ENDPOINTS:
        return False
    try:
        if saved_remote_member():
            return False
        return db.session.query(User.id).limit(1).first() is None
    except Exception:  # noqa: BLE001 - authorization exceptions must fail closed
        return False


def enforce_human_authorization():
    if not current_app.config.get("FCP_AUTH_ENABLED", True):
        return None
    endpoint = request.endpoint
    if endpoint is None or endpoint in PUBLIC_ENDPOINTS or endpoint == "static":
        return None
    if pre_auth_federation_bootstrap_allowed(endpoint):
        return None
    if not current_user.is_authenticated or not current_user.is_active:
        if request.is_json or request.path.startswith("/api/"):
            abort(401)
        return redirect(url_for("security.login", next=request.full_path))
    permission = permission_for(endpoint, request.method)
    if permission is None or not current_user.has_permission(permission):
        abort(403)
    return None


def audit_route_policy(app: Flask) -> list[str]:
    """Return routes with no explicit policy; callers/tests must fail on any item."""
    missing: list[str] = []
    for rule in app.url_map.iter_rules():
        methods: Iterable[str] = rule.methods or ()
        for method in methods:
            if method in {"HEAD", "OPTIONS"}:
                continue
            if rule.endpoint in PUBLIC_ENDPOINTS or rule.endpoint == "static":
                continue
            if permission_for(rule.endpoint, method) is None:
                missing.append(f"{method} {rule.rule} ({rule.endpoint})")
    return sorted(missing)
