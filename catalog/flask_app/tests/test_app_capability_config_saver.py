from __future__ import annotations

from catalog.flask_app import app as app_module
from catalog.flask_app.app import create_app


class _RuntimeManager:
    def mark_app_started(self) -> None:
        pass

    def requires_startup_choice(self) -> bool:
        return False


def test_product_app_has_no_retired_setup_saver(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(app_module, "get_runtime_manager", lambda: _RuntimeManager())

    app = create_app()

    assert "CAPABILITY_ONBOARDING_SETUP_SAVER" not in app.config
    assert "server_setup_web.save_language_model_capability_config" in app.view_functions
    assert "server_setup_web.save_recorder_capability_config" in app.view_functions
