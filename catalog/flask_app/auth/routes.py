"""Small FCP-native human user administration surface."""

from __future__ import annotations

import uuid

from email_validator import EmailNotValidError, validate_email
from flask import (
    Blueprint,
    abort,
    current_app,
    flash,
    redirect,
    render_template,
    request,
    url_for,
)
from flask_security import current_user, hash_password

from .federation import get_federated_human_auth_service, saved_remote_member
from .models import Role, User, db
from .policy import ROLE_SUMMARIES

auth_users = Blueprint("auth_users", __name__, url_prefix="/admin/users")


def _active_admin_count() -> int:
    return (
        db.session.query(User)
        .join(User.roles)
        .filter(User.active.is_(True), Role.name == "admin")
        .distinct()
        .count()
    )


def _normalized_email(value: str) -> str | None:
    try:
        return validate_email(value.strip(), check_deliverability=False).normalized.lower()
    except EmailNotValidError:
        return None


def _member_admin_redirect():
    """Route member-side user administration to the password authority."""

    service = get_federated_human_auth_service()
    mode = service.mode(refresh=True)
    if not mode.is_member and not saved_remote_member():
        return None
    authority = mode.authority or service.authority(refresh=True)
    if authority is None:
        abort(503)
    return redirect(f"{authority.base_url}{url_for('auth_users.users')}")


def _require_local_user_authority() -> None:
    service = get_federated_human_auth_service()
    mode = service.mode()
    if mode.is_member or saved_remote_member():
        abort(403)


def _publish_user_best_effort(user: User) -> None:
    try:
        get_federated_human_auth_service().publish_user(user)
    except Exception as exc:  # noqa: BLE001 - retain local admin recovery path
        current_app.logger.warning(
            "Federation human-user publication unavailable (%s)",
            type(exc).__name__,
        )


@auth_users.get("")
def users():
    member_redirect = _member_admin_redirect()
    if member_redirect is not None:
        return member_redirect
    try:
        # This seeds accounts created before Federation SSO was enabled and is
        # idempotent when no user state changed.
        get_federated_human_auth_service().publish_all_users()
    except Exception as exc:  # noqa: BLE001 - user admin remains usable locally
        current_app.logger.warning(
            "Federation human-user reconciliation unavailable (%s)",
            type(exc).__name__,
        )
    return render_template(
        "auth/users.html",
        users=db.session.query(User).order_by(User.email).all(),
        roles=db.session.query(Role).order_by(Role.name).all(),
        role_summaries=ROLE_SUMMARIES,
        active_admin_count=_active_admin_count(),
    )


@auth_users.post("")
def create_user():
    _require_local_user_authority()
    email = _normalized_email(request.form.get("email", ""))
    password = request.form.get("password", "")
    role_names = set(request.form.getlist("roles"))
    if email is None or len(password) < 12:
        flash("Enter a valid email and a password of at least 12 characters.", "error")
        return redirect(url_for("auth_users.users"))
    if db.session.query(User).filter_by(email=email).first() is not None:
        flash("That user already exists.", "error")
        return redirect(url_for("auth_users.users"))
    roles = db.session.query(Role).filter(Role.name.in_(role_names)).all()
    user = User(
        email=email,
        password=hash_password(password),
        active=True,
        fs_uniquifier=uuid.uuid4().hex,
        roles=roles,
    )
    db.session.add(user)
    db.session.commit()
    _publish_user_best_effort(user)
    current_app.logger.info("Human user created by %s: %s", current_user.email, email)
    flash("User created.", "success")
    return redirect(url_for("auth_users.users"))


@auth_users.post("/<int:user_id>")
def update_user(user_id: int):
    _require_local_user_authority()
    user = db.session.get(User, user_id)
    if user is None:
        abort(404)
    active = request.form.get("active") == "1"
    role_names = set(request.form.getlist("roles"))
    removes_last_admin = (
        user.active
        and user.has_role("admin")
        and (not active or "admin" not in role_names)
    )
    if removes_last_admin and _active_admin_count() <= 1:
        flash(
            "The final active administrator cannot be deactivated or demoted.", "error"
        )
        return redirect(url_for("auth_users.users"))
    user.active = active
    user.roles = db.session.query(Role).filter(Role.name.in_(role_names)).all()
    db.session.commit()
    _publish_user_best_effort(user)
    current_app.logger.info(
        "Human user changed by %s: %s active=%s roles=%s",
        current_user.email,
        user.email,
        user.active,
        sorted(role_names),
    )
    flash("User updated.", "success")
    return redirect(url_for("auth_users.users"))
