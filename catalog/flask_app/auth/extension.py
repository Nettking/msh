"""Initialization, schema seeding, and bootstrap CLI for human authentication."""

from __future__ import annotations

import os
import re
import secrets
import uuid
from datetime import timedelta
from pathlib import Path

import click
from flask import Flask, request
from flask_security import SQLAlchemyUserDatastore, Security, hash_password
from flask_wtf.csrf import CSRFProtect

from .models import Role, User, db
from .policy import ROLE_PERMISSIONS, audit_route_policy, enforce_human_authorization
from .routes import auth_users

security = Security()
csrf = CSRFProtect()
_UNSAFE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
_HUMAN_CSRF_BLUEPRINTS = frozenset({"security", "auth_users"})
_LEGACY_PLACEHOLDERS = frozenset({"fcp-dev", "fcp-dev-change-me"})


def _boolean_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _database_uri(value: str) -> str:
    if value.startswith("sqlite:"):
        return value
    path = Path(value).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{path}"


def _secret_directory() -> Path:
    path = Path(os.getenv("FCP_AUTH_SECRET_DIR", "data/auth")).expanduser().resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


def _read_or_create_secret(path: Path) -> str:
    try:
        value = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        value = secrets.token_urlsafe(48)
        try:
            with path.open("x", encoding="utf-8") as handle:
                handle.write(value + "\n")
        except FileExistsError:
            value = path.read_text(encoding="utf-8").strip()
        try:
            path.chmod(0o600)
        except OSError:
            pass
    if len(value) < 32:
        raise RuntimeError(f"Persisted auth secret is invalid: {path}")
    return value


def _validated_secret(app: Flask) -> str:
    secret = os.getenv("FCP_FLASK_SECRET", "").strip()
    development = _boolean_env("FCP_DEVELOPMENT", False)
    if secret and secret not in _LEGACY_PLACEHOLDERS:
        if len(secret) < 32:
            raise RuntimeError("FCP_FLASK_SECRET must contain at least 32 characters")
        return secret
    if development:
        app.logger.warning("Using an ephemeral development-only Flask session secret")
        return uuid.uuid4().hex + uuid.uuid4().hex
    return _read_or_create_secret(_secret_directory() / "flask-secret")


def _validated_password_salt(app: Flask) -> str:
    salt = os.getenv("FCP_PASSWORD_SALT", "").strip()
    development = _boolean_env("FCP_DEVELOPMENT", False)
    if salt:
        if len(salt) < 32:
            raise RuntimeError("FCP_PASSWORD_SALT must contain at least 32 characters")
        return salt
    if development:
        app.logger.warning(
            "Using an ephemeral development-only password salt; human passwords will "
            "not survive a salt change"
        )
        return uuid.uuid4().hex + uuid.uuid4().hex
    return _read_or_create_secret(_secret_directory() / "password-salt")


def _seed_policy(datastore: SQLAlchemyUserDatastore) -> None:
    for role_name, names in ROLE_PERMISSIONS.items():
        role = datastore.find_role(role_name)
        if role is None:
            role = datastore.create_role(
                name=role_name, description=f"FCP {role_name} role"
            )
        role.permissions = sorted(names)
    db.session.commit()


def _register_cli(app: Flask, datastore: SQLAlchemyUserDatastore) -> None:
    @app.cli.group("fcp-user")
    def user_cli() -> None:
        """Manage human FCP accounts."""

    @user_cli.command("create-admin")
    @click.option("--email", prompt=True)
    @click.option("--password", prompt=True, hide_input=True, confirmation_prompt=True)
    def create_admin(email: str, password: str) -> None:
        """Create an administrator; safely refuse duplicates."""
        normalized = email.strip().lower()
        if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", normalized):
            raise click.ClickException("A valid email address is required")
        if len(password) < 12:
            raise click.ClickException("Password must contain at least 12 characters")
        if datastore.find_user(email=normalized) is not None:
            raise click.ClickException("That human user already exists")
        user = datastore.create_user(email=normalized, password=hash_password(password))
        datastore.add_role_to_user(user, "admin")
        db.session.commit()
        app.logger.info("Human administrator created: %s", normalized)
        click.echo(f"Created administrator {normalized}")


def init_human_auth(app: Flask) -> None:
    """Install human auth without changing Federation/device authentication."""
    enabled = not _boolean_env("FCP_AUTH_DISABLED", False)
    if not enabled and not (_boolean_env("FCP_DEVELOPMENT", False) or app.testing):
        raise RuntimeError("FCP_AUTH_DISABLED is permitted only with FCP_DEVELOPMENT=1")

    app.config.update(
        FCP_AUTH_ENABLED=enabled,
        SECRET_KEY=_validated_secret(app),
    )
    app.extensions["fcp_auth_route_audit"] = lambda: audit_route_policy(app)

    # Development/test bypass must preserve the legacy Flask request surface.
    # In particular, do not install Flask-Security response hooks or generic CSRF
    # processing when human auth is explicitly disabled.
    if not enabled:
        return

    secure_cookie = _boolean_env("FCP_HTTPS", False)
    session_minutes = int(os.getenv("FCP_SESSION_MINUTES", "480"))
    app.config.update(
        SQLALCHEMY_DATABASE_URI=_database_uri(
            os.getenv("FCP_AUTH_DATABASE", "data/auth/users.sqlite3")
        ),
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
        SECURITY_PASSWORD_SALT=_validated_password_salt(app),
        SECURITY_PASSWORD_HASH="argon2",
        SECURITY_REGISTERABLE=False,
        SECURITY_RECOVERABLE=False,
        SECURITY_CHANGEABLE=True,
        SECURITY_SEND_REGISTER_EMAIL=False,
        SECURITY_POST_LOGIN_VIEW="/",
        SECURITY_POST_LOGOUT_VIEW="/login",
        SECURITY_DEFAULT_REMEMBER_ME=False,
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
        SESSION_COOKIE_SECURE=secure_cookie,
        PERMANENT_SESSION_LIFETIME=timedelta(minutes=session_minutes),
        REMEMBER_COOKIE_HTTPONLY=True,
        REMEMBER_COOKIE_SAMESITE="Lax",
        REMEMBER_COOKIE_SECURE=secure_cookie,
        REMEMBER_COOKIE_DURATION=timedelta(days=7),
        WTF_CSRF_FIELD_NAME="csrf_token",
        WTF_CSRF_TIME_LIMIT=timedelta(hours=8),
        WTF_CSRF_CHECK_DEFAULT=False,
    )

    # Register before Flask-Security so this runs last (Flask after_request hooks
    # execute in reverse registration order) and preserves established FCP
    # no-store response contracts.
    @app.after_request
    def preserve_explicit_no_store(response):
        cache_control = response.headers.get("Cache-Control", "")
        if "no-store" in cache_control.lower():
            response.headers["Cache-Control"] = "no-store"
        return response

    db.init_app(app)
    datastore = SQLAlchemyUserDatastore(db, User, Role)
    security.init_app(app, datastore)
    csrf.init_app(app)
    app.register_blueprint(auth_users)

    @app.before_request
    def protect_human_auth_forms():
        if (
            request.method in _UNSAFE_METHODS
            and request.blueprint in _HUMAN_CSRF_BLUEPRINTS
        ):
            csrf.protect()
        return None

    app.before_request(enforce_human_authorization)
    with app.app_context():
        db.create_all()
        _seed_policy(datastore)
    _register_cli(app, datastore)
