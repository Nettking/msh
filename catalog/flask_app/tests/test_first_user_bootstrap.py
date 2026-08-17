from __future__ import annotations

from flask import Blueprint, Flask

from catalog.flask_app.auth import policy as auth_policy
from catalog.flask_app.auth import routes as auth_routes
from catalog.flask_app.auth.extension import init_human_auth
from catalog.flask_app.auth.models import FirstUserBootstrapClaim, User, db
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

    onboarding = Blueprint("capability_onboarding_web", __name__)
    onboarding.add_url_rule(
        "/onboarding",
        endpoint="onboarding",
        view_func=lambda: "onboarding",
        methods=["GET"],
    )
    onboarding.add_url_rule(
        "/onboarding/identity",
        endpoint="create_identity",
        view_func=lambda: "identity-created",
        methods=["POST"],
    )
    onboarding.add_url_rule(
        "/onboarding/federation",
        endpoint="federation_action",
        view_func=lambda: "federation-action",
        methods=["POST"],
    )
    app.register_blueprint(onboarding)

    pairing = Blueprint("federation_pairing_web", __name__)
    pairing.add_url_rule(
        "/onboarding/federation/pair",
        endpoint="pair_device",
        view_func=lambda: "paired",
        methods=["POST"],
    )
    pairing.add_url_rule(
        "/onboarding/federation/pairing-code",
        endpoint="create_pairing_code",
        view_func=lambda: "pairing-code",
        methods=["POST"],
    )
    app.register_blueprint(pairing)
    return app


def _discovered_candidate() -> tuple[dict[str, object], ...]:
    return (
        {
            "federation_label": "Existing Federation",
            "federation_fingerprint": "a" * 32,
            "device_name": "Federation PC",
            "tailscale_ip": "100.64.0.20",
            "web_port": 5000,
        },
    )


def test_empty_first_installation_meets_user_with_first_admin_setup(tmp_path, monkeypatch):
    app = _empty_app(tmp_path, monkeypatch)
    client = app.test_client()

    response = client.get("/")
    assert response.status_code == 302
    assert response.headers["Location"].endswith("/admin/users/bootstrap")

    page = client.get("/admin/users/bootstrap")
    assert page.status_code == 200
    body = page.get_data(as_text=True)
    assert "Create the first administrator" in body
    assert "first FCP device" in body
    assert "Use an existing Federation account" not in body
    assert "Create first administrator" in body

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

    login_page = client.get(created.headers["Location"])
    login_body = login_page.get_data(as_text=True)
    assert "Administrator created." not in login_body
    with client.session_transaction() as session:
        flashes = session.get("_flashes", [])
    assert ("success", "Administrator created.") in flashes
    assert all("Sign in to continue" not in message for _, message in flashes)

    with app.app_context():
        user = db.session.query(User).filter_by(email="first@example.com").one()
        assert user.active is True
        assert user.has_role("admin")
        assert db.session.query(FirstUserBootstrapClaim).count() == 1

    closed = client.get("/admin/users/bootstrap")
    assert closed.status_code == 302
    assert closed.headers["Location"].endswith("/login")

    normal = client.get("/")
    assert normal.status_code == 302
    assert "/login" in normal.headers["Location"]

    onboarding = client.get("/onboarding")
    assert onboarding.status_code == 302
    assert "/login" in onboarding.headers["Location"]
    assert audit_route_policy(app) == []


def test_discovered_federation_does_not_bypass_first_admin_bootstrap(
    tmp_path, monkeypatch
):
    # Discovery may already be running in the background, but it cannot become
    # human/Federation authority before this installation has its first admin.
    monkeypatch.setattr(
        "catalog.flask_app.auth.extension.fresh_discovered_federations",
        _discovered_candidate,
    )
    app = _empty_app(tmp_path, monkeypatch)
    client = app.test_client()

    response = client.get("/")
    assert response.status_code == 302
    assert response.headers["Location"].endswith("/admin/users/bootstrap")

    bootstrap = client.get(response.headers["Location"])
    assert bootstrap.status_code == 200
    body = bootstrap.get_data(as_text=True)
    assert "Create the first administrator" in body

    login = client.get("/login")
    assert login.status_code == 302
    assert login.headers["Location"].endswith("/admin/users/bootstrap")

    with app.app_context():
        assert db.session.query(User).count() == 0


def test_empty_installation_blocks_all_federation_onboarding_before_local_user(
    tmp_path, monkeypatch
):
    app = _empty_app(tmp_path, monkeypatch)
    client = app.test_client()

    for method, path in (
        (client.get, "/onboarding"),
        (client.post, "/onboarding/identity"),
        (client.post, "/onboarding/federation/pair"),
        (client.post, "/onboarding/federation"),
        (client.post, "/onboarding/federation/pairing-code"),
    ):
        response = method(path)
        assert response.status_code == 302
        assert response.headers["Location"].endswith("/admin/users/bootstrap")

    with app.app_context():
        assert db.session.query(User).count() == 0


def test_remote_member_closes_anonymous_pairing_before_shadow_user_exists(
    tmp_path, monkeypatch
):
    app = _empty_app(tmp_path, monkeypatch)
    monkeypatch.setattr(auth_routes, "saved_remote_member", lambda: True)
    monkeypatch.setattr(auth_policy, "saved_remote_member", lambda: True)
    client = app.test_client()

    response = client.get("/onboarding")
    assert response.status_code == 302
    assert "/login" in response.headers["Location"]
    assert "/admin/users/bootstrap" not in response.headers["Location"]

    pair = client.post("/onboarding/federation/pair")
    assert pair.status_code == 302
    assert "/login" in pair.headers["Location"]

    with app.app_context():
        assert db.session.query(User).count() == 0


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
    assert "/admin/users/bootstrap" in response.headers["Location"]
    assert "local=1" in response.headers["Location"]
    with app.app_context():
        assert db.session.query(User).count() == 0
        assert db.session.query(FirstUserBootstrapClaim).count() == 0


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
        assert db.session.query(FirstUserBootstrapClaim).count() == 1


def test_bootstrap_claim_fails_closed_if_claim_exists_without_user(tmp_path, monkeypatch):
    app = _empty_app(tmp_path, monkeypatch)
    client = app.test_client()

    with app.app_context():
        db.session.add(FirstUserBootstrapClaim(id=1))
        db.session.commit()
        assert db.session.query(User).count() == 0

    response = client.post(
        "/admin/users/bootstrap",
        data={
            "email": "blocked@example.com",
            "password": "correct horse battery staple",
            "password_confirm": "correct horse battery staple",
        },
    )
    assert response.status_code == 503
    with app.app_context():
        assert db.session.query(User).count() == 0
        assert db.session.query(FirstUserBootstrapClaim).count() == 1
