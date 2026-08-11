from __future__ import annotations

import pytest
from flask import Flask
from flask_security import hash_password

from catalog.flask_app.auth.extension import init_human_auth
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
                    email=f"{name}@example.test",
                    password=hash_password("correct horse battery staple"),
                    active=True,
                    fs_uniquifier=f"test-{name}",
                    roles=[roles[name]],
                )
            )
        db.session.add(
            User(
                email="inactive@example.test",
                password=hash_password("correct horse battery staple"),
                active=False,
                fs_uniquifier="test-inactive",
                roles=[roles["admin"]],
            )
        )
        db.session.commit()
    return application


def _login(client, email: str, password: str = "correct horse battery staple"):
    return client.post("/login", data={"email": email, "password": password})


def test_authentication_login_logout_and_inactive_user(app):
    client = app.test_client()
    assert client.get("/").status_code == 302
    assert (
        _login(client, "viewer@example.test", "incorrect-password").status_code == 200
    )
    assert client.get("/").status_code == 302
    assert _login(client, "viewer@example.test").status_code == 302
    assert client.get("/").status_code == 200
    client.get("/logout")
    assert client.get("/").status_code == 302
    assert _login(client, "inactive@example.test").status_code == 200
    assert client.get("/").status_code == 302


@pytest.mark.parametrize(
    ("role", "write_status"), [("viewer", 403), ("operator", 200), ("admin", 200)]
)
def test_role_boundaries(app, role, write_status):
    client = app.test_client()
    _login(client, f"{role}@example.test")
    assert client.get("/").status_code == 200
    assert client.post("/operate").status_code == write_status


def test_permission_model_and_route_audit_are_central(app):
    assert (
        ROLE_PERMISSIONS["viewer"]
        < ROLE_PERMISSIONS["operator"]
        < ROLE_PERMISSIONS["admin"]
    )
    assert audit_route_policy(app) == []


def test_bootstrap_creates_admin_once(app):
    runner = app.test_cli_runner()
    args = [
        "fcp-user",
        "create-admin",
        "--email",
        "first@example.test",
        "--password",
        "a-secure-bootstrap-password",
    ]
    assert runner.invoke(args=args).exit_code == 0
    assert runner.invoke(args=args).exit_code != 0
    with app.app_context():
        assert (
            db.session.query(User)
            .filter_by(email="first@example.test")
            .one()
            .has_role("admin")
        )


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
