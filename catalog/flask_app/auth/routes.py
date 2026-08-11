"""Small FCP-native human user administration surface."""

from __future__ import annotations

import re
import uuid

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

from .models import Role, User, db

auth_users = Blueprint("auth_users", __name__, url_prefix="/admin/users")


def _active_admin_count() -> int:
    return (
        db.session.query(User)
        .join(User.roles)
        .filter(User.active.is_(True), Role.name == "admin")
        .distinct()
        .count()
    )


@auth_users.get("")
def users():
    return render_template(
        "auth/users.html",
        users=db.session.query(User).order_by(User.email).all(),
        roles=db.session.query(Role).order_by(Role.name).all(),
    )


@auth_users.post("")
def create_user():
    email = request.form.get("email", "").strip().lower()
    password = request.form.get("password", "")
    role_names = set(request.form.getlist("roles"))
    if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email) or len(password) < 12:
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
    current_app.logger.info("Human user created by %s: %s", current_user.email, email)
    flash("User created.", "success")
    return redirect(url_for("auth_users.users"))


@auth_users.post("/<int:user_id>")
def update_user(user_id: int):
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
    current_app.logger.info(
        "Human user changed by %s: %s active=%s roles=%s",
        current_user.email,
        user.email,
        user.active,
        sorted(role_names),
    )
    flash("User updated.", "success")
    return redirect(url_for("auth_users.users"))
