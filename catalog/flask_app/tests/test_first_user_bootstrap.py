from __future__ import annotations

from flask import Flask

from catalog.flask_app.auth.extension import init_human_auth
from catalog.flask_app.auth.models import User, db
from catalog.flask_app.auth.policy import audit_route_policy


def _empty_app(tmp_path, monkeypatch) -> Flask:
    monkeypatch.delenv("FCP_AUTH_DISABLED", raising=False)
    monkeypatch.setenv("FCP_FLASK_SECRET", "s" * 48)
    monkeypatch.setenv("FCP_PASSWORD_SALT", "p" * 48)
    monkeypatch.setenv("FCP_AUTH_DATABASE", str(tmp_path / "users.sqlite3"))
    app = Flask(__name__, template_folder="../templates")
    app.testing = True
    app.config["WTF_CSRF_ENABLED"] = False
    init_human_auth(app)
    app.add_url_rule("/", "web.dashboard", lambda: "dashboard", methods=["GET"])
    app.add_url_rule("/api/status", "web.api_status", lambda: {"ok": True}, methods=["GET"])
    return app


def test_empty_installation_meets_user_with_first_admin_setup(tmp_path, monkeypatch):
    app = _empty_app(tmp_path, monkeypatch)
    client = app.test_client()

    response = client.get("/")
    assert response.status_code == 302
    assert response.headers["Location"].endswith("/admin/users/bootstrap")

    page = client.get("/admin/users/bootstrap")
    assert page.status_code == 200
    body = page.get_data(as_text=True)
    assert "Create the first user" in body
    assert "Create administrator" in body

    created = client.post(
        "/admin/users/bootstrap",
        data={
            "email": "first@example.com",
            "password": "correct horse battery staple",
            "password_confirm": "correct horse battery staple",
        },
    )
    assert created.status_code == 302
    assert created.headers["Location"].endswith("/login")

    with app.app_context():
        user = db.session.query(User).filter_by(email="first@example.com").one()
        assert user.active is True
        assert user.has_role("admin")

    closed = client.get("/admin/users/bootstrap")
    assert closed.status_code == 302
    assert closed.headers["Location"].endswith("/login")

    normal = client.get("/")
    assert normal.status_code == 302
    assert "/login" in normal.headers["Location"]
    assert audit_route_policy(app) == []


def test_first_user_setup_validates_password_confirmation(tmp_path, monkeypatch):
    app = _empty_app(tmp_path, monkeypatch)
    client = app.test_client()

    response = client.post(
        "/admin/users/bootstrap",
        data={
            "email": "first@example.com",
            "password": "correct horse battery staple",
            "password_confirm": "different secure password",
        },
    )
    assert response.status_code == 302
    assert response.headers["Location"].endswith("/admin/users/bootstrap")
    with app.app_context():
        assert db.session.query(User).count() == 0


def test_empty_installation_api_fails_closed_instead_of_html_redirect(tmp_path, monkeypatch):
    app = _empty_app(tmp_path, monkeypatch)
    client = app.test_client()

    response = client.get("/api/status", headers={"Accept": "application/json"})
    assert response.status_code == 503
    with app.app_context():
        assert db.session.query(User).count() == 0


def test_stale_bootstrap_form_cannot_create_second_user(tmp_path, monkeypatch):
    app = _empty_app(tmp_path, monkeypatch)
    client = app.test_client()

    first = client.post(
        "/admin/users/bootstrap",
        data={
            "email": "first@example.com",
            "password": "correct horse battery staple",
            "password_confirm": "correct horse battery staple",
        },
    )
    assert first.status_code == 302

    second = client.post(
        "/admin/users/bootstrap",
        data={
            "email": "second@example.com",
            "password": "another correct horse battery staple",
            "password_confirm": "another correct horse battery staple",
        },
    )
    assert second.status_code == 302
    assert second.headers["Location"].endswith("/login")
    with app.app_context():
        assert db.session.query(User).count() == 1
        assert db.session.query(User).filter_by(email="second@example.com").one_or_none() is None
