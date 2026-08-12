from __future__ import annotations

import re

import pytest
from flask import Flask
from flask_security import hash_password

from catalog.flask_app.auth.extension import (
    _validated_password_salt,
    _validated_secret,
    init_human_auth,
)
from catalog.flask_app.auth.models import Role, User, db
from catalog.flask_app.auth.policy import ROLE_PERMISSIONS, audit_route_policy


@pytest.fixture()
def app(tmp_path, monkeypatch):
    monkeypatch.delenv("FCP_AUTH_DISABLED", raising=False)
    monkeypatch.setenv("FCP_FLASK_SECRET", "s" * 48)
    monkeypatch.setenv("FCP_PASSWORD_SALT", "p" * 48)
    monkeypatch.setenv("FCP_AUTH_DATABASE", str(tmp_path / "users.sqlite3"))
    application = Flask(__name__, template_folder="../templates")
    application.testing = True
    application.config["WTF_CSRF_ENABLED"] = False
    init_human_auth(application)

    def dashboard():
        return "dashboard"

    def operate():
        return "operated"

    application.add_url_rule("/", "web.dashboard", dashboard, methods=["GET"])
    application.add_url_rule("/operate", "web.operate", operate, methods=["POST"])
    with application.app_context():
        roles = {r.name: r for r in db.session.query(Role).all()}
        for name in ("viewer", "operator", "admin"):
            db.session.add(
                User(
                    email=f"{name}@example.com",
                    password=hash_password("correct horse battery staple"),
                    active=True,
                    fs_uniquifier=f"test-{name}",
                    roles=[roles[name]],
                )
            )
        db.session.add(
            User(
                email="inactive@example.com",
                password=hash_password("correct horse battery staple"),
                active=False,
                fs_uniquifier="test-inactive",
                roles=[roles["admin"]],
            )
        )
        db.session.commit()
    return application


def _csrf_token(client) -> str:
    body = client.get("/login").get_data(as_text=True)
    match = re.search(r'name="csrf_token"[^>]*value="([^"]+)"', body)
    return match.group(1) if match else ""


def _login(client, email: str, password: str = "correct horse battery staple"):
    return client.post("/login", data={"email": email, "password": password})


def test_authentication_login_logout_and_inactive_user(app):
    client = app.test_client()
    assert client.get("/").status_code == 302
    assert (
        _login(client, "viewer@example.com", "incorrect-password").status_code == 200
    )
    assert client.get("/").status_code == 302
    assert _login(client, "viewer@example.com").status_code == 302
    assert client.get("/").status_code == 200
    client.get("/logout")
    assert client.get("/").status_code == 302
    assert _login(client, "inactive@example.com").status_code == 200
    assert client.get("/").status_code == 302


def test_login_requires_csrf_when_enabled(app):
    app.config["WTF_CSRF_ENABLED"] = True
    client = app.test_client()
    assert _login(client, "viewer@example.com").status_code == 400


def test_login_succeeds_with_a_valid_csrf_token(app):
    """Cover the accept path, not only the reject path.

    ``WTF_CSRF_TIME_LIMIT`` reaches itsdangerous as ``max_age`` and is compared
    against an integer age. Configuring it as a timedelta raises TypeError while
    validating an otherwise correct token, so sign-in fails closed with a 500
    for every human — a failure a missing-token test cannot see.
    """

    app.config["WTF_CSRF_ENABLED"] = True
    client = app.test_client()
    token = _csrf_token(client)
    assert token

    response = client.post(
        "/login",
        data={
            "email": "viewer@example.com",
            "password": "correct horse battery staple",
            "csrf_token": token,
        },
    )

    assert response.status_code == 302


def test_csrf_time_limit_is_configured_in_seconds(app):
    limit = app.config["WTF_CSRF_TIME_LIMIT"]
    assert isinstance(limit, int) and not isinstance(limit, bool)
    assert limit == 8 * 60 * 60


@pytest.mark.parametrize(
    ("role", "write_status"), [("viewer", 403), ("operator", 200), ("admin", 200)]
)
def test_role_boundaries(app, role, write_status):
    client = app.test_client()
    _login(client, f"{role}@example.com")
    assert client.get("/").status_code == 200
    assert client.post("/operate").status_code == write_status


def test_permission_model_and_route_audit_are_central(app):
    assert (
        ROLE_PERMISSIONS["viewer"]
        < ROLE_PERMISSIONS["operator"]
        < ROLE_PERMISSIONS["admin"]
    )
    assert audit_route_policy(app) == []


def test_real_application_has_no_unclassified_routes(tmp_path, monkeypatch):
    monkeypatch.delenv("FCP_AUTH_DISABLED", raising=False)
    monkeypatch.setenv("FCP_FLASK_SECRET", "r" * 48)
    monkeypatch.setenv("FCP_PASSWORD_SALT", "q" * 48)
    monkeypatch.setenv("FCP_AUTH_DATABASE", str(tmp_path / "real-users.sqlite3"))
    from catalog.flask_app.app import create_app

    application = create_app()
    assert audit_route_policy(application) == []


def test_bootstrap_creates_admin_once(app):
    runner = app.test_cli_runner()
    args = [
        "fcp-user",
        "create-admin",
        "--email",
        "first@example.com",
        "--password",
        "a-secure-bootstrap-password",
    ]
    assert runner.invoke(args=args).exit_code == 0
    assert runner.invoke(args=args).exit_code != 0
    with app.app_context():
        assert (
            db.session.query(User)
            .filter_by(email="first@example.com")
            .one()
            .has_role("admin")
        )


def test_bootstrap_rejects_reserved_domain_email(app):
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "fcp-user",
            "create-admin",
            "--email",
            "first@example.test",
            "--password",
            "a-secure-bootstrap-password",
        ]
    )
    assert result.exit_code != 0
    assert "valid email address" in result.output


def test_production_rejects_explicit_short_secret(tmp_path, monkeypatch):
    monkeypatch.delenv("FCP_AUTH_DISABLED", raising=False)
    monkeypatch.setenv("FCP_FLASK_SECRET", "too-short")
    monkeypatch.setenv("FCP_PASSWORD_SALT", "p" * 48)
    monkeypatch.setenv("FCP_AUTH_DATABASE", str(tmp_path / "other.sqlite3"))
    monkeypatch.delenv("FCP_DEVELOPMENT", raising=False)
    with pytest.raises(RuntimeError, match="FCP_FLASK_SECRET"):
        init_human_auth(Flask("production"))


def test_production_rejects_explicit_short_password_salt(tmp_path, monkeypatch):
    monkeypatch.delenv("FCP_AUTH_DISABLED", raising=False)
    monkeypatch.setenv("FCP_FLASK_SECRET", "s" * 48)
    monkeypatch.setenv("FCP_PASSWORD_SALT", "too-short")
    monkeypatch.setenv("FCP_AUTH_DATABASE", str(tmp_path / "other.sqlite3"))
    monkeypatch.delenv("FCP_DEVELOPMENT", raising=False)
    with pytest.raises(RuntimeError, match="FCP_PASSWORD_SALT"):
        init_human_auth(Flask("production-salt"))


def test_production_secrets_are_independent_persistent_and_reused(tmp_path, monkeypatch):
    monkeypatch.delenv("FCP_AUTH_DISABLED", raising=False)
    monkeypatch.delenv("FCP_FLASK_SECRET", raising=False)
    monkeypatch.delenv("FCP_PASSWORD_SALT", raising=False)
    monkeypatch.delenv("FCP_DEVELOPMENT", raising=False)
    monkeypatch.setenv("FCP_AUTH_SECRET_DIR", str(tmp_path / "secrets"))

    first_app = Flask("first-production")
    first_secret = _validated_secret(first_app)
    first_salt = _validated_password_salt(first_app)
    second_app = Flask("second-production")

    assert _validated_secret(second_app) == first_secret
    assert _validated_password_salt(second_app) == first_salt
    assert first_secret != first_salt
    assert (tmp_path / "secrets" / "flask-secret").is_file()
    assert (tmp_path / "secrets" / "password-salt").is_file()
