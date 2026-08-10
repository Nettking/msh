from __future__ import annotations

from catalog.flask_app import app as app_module
from catalog.flask_app.app import create_app
from catalog.flask_app.services.capability_config_migration_service import (
    persist_capability_config_from_setup,
)


class _RuntimeManager:
    def mark_app_started(self) -> None:
        pass

    def requires_startup_choice(self) -> bool:
        return False


def test_product_app_installs_role_free_transition_saver(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(app_module, "get_runtime_manager", lambda: _RuntimeManager())

    app = create_app()

    assert (
        app.config["CAPABILITY_ONBOARDING_SETUP_SAVER"]
        is persist_capability_config_from_setup
    )
