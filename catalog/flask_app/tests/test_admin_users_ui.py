from __future__ import annotations

import inspect
from pathlib import Path

from catalog.flask_app.auth import routes
from catalog.flask_app.auth.policy import ROLE_PERMISSIONS, ROLE_SUMMARIES


APP_ROOT = Path(__file__).resolve().parents[1]


def test_role_summaries_cover_every_human_role() -> None:
    assert set(ROLE_SUMMARIES) == set(ROLE_PERMISSIONS)
    assert all(summary.strip() for summary in ROLE_SUMMARIES.values())


def test_admin_users_route_supplies_ui_guard_context() -> None:
    source = inspect.getsource(routes.users)
    assert "role_summaries=ROLE_SUMMARIES" in source
    assert "active_admin_count=_active_admin_count()" in source


def test_admin_users_template_keeps_management_guards_visible() -> None:
    template = (APP_ROOT / "templates" / "auth" / "users.html").read_text(
        encoding="utf-8"
    )

    assert 'class="users-form"' in template
    assert 'class="role-option"' in template
    assert 'name="csrf_token"' in template
    assert 'minlength="12"' in template
    assert "active_admin_count <= 1" in template
    assert "only active administrator" in template.lower()
    assert "current_user.is_authenticated" in template


def test_admin_users_styles_are_scoped_and_loaded() -> None:
    base = (APP_ROOT / "templates" / "base.html").read_text(encoding="utf-8")
    css = (APP_ROOT / "static" / "css" / "admin-users.css").read_text(
        encoding="utf-8"
    )

    assert "css/admin-users.css" in base
    assert '.users-field input[type="email"]' in css
    assert '.users-field input[type="password"]' in css
    assert ".role-option" in css
    assert ".user-row" in css
    assert "@media (max-width: 640px)" in css
