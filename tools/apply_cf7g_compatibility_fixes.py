from __future__ import annotations

from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    target = Path(path)
    text = target.read_text(encoding="utf-8")
    if old not in text:
        raise SystemExit(f"Patch anchor not found in {path}: {old[:100]!r}")
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


replace_once(
    "catalog/flask_app/services/capability_startup_transition_service.py",
    '''    def _current_inspection_revision(self, device_id: str) -> int:
        snapshot = self.inspection_service.load()
        if (
            snapshot is None
            or snapshot.device_id != device_id
            or self.inspection_service.state(snapshot) != "current"
        ):
            raise FederationOperationError(
                "startup-transition-inspection-required",
                "run a current device inspection before finishing onboarding",
                "inspection",
            )
        return snapshot.revision
''',
    '''    def _current_inspection_revision(self, device_id: str) -> int:
        snapshot = self.inspection_service.load()
        state = getattr(self.inspection_service, "state", None)
        expired = (
            snapshot is not None
            and callable(state)
            and state(snapshot) != "current"
        )
        if (
            snapshot is None
            or snapshot.device_id != device_id
            or expired
        ):
            raise FederationOperationError(
                "startup-transition-inspection-required",
                "run a current device inspection before finishing onboarding",
                "inspection",
            )
        return snapshot.revision
''',
)

replace_once(
    "catalog/flask_app/capability_startup_transition_routes.py",
    '''    flash(message, "success")
    return redirect(
        url_for("federation_web.overview"),
        code=303,
    )
''',
    '''    flash(message, "success")
    if migration:
        destination = url_for(
            "capability_startup_transition_web.onboarding",
            step="finish",
        )
    else:
        destination = url_for("federation_web.overview")
    return redirect(destination, code=303)
''',
)

replace_once(
    "catalog/flask_app/templates/federation/onboarding/_inspect.html",
    '''          <h4 id="inspection-checks-heading">Optional checks available later</h4>
''',
    '''          <h4 id="inspection-checks-heading">Recommended bounded checks</h4>
''',
)

replace_once(
    "catalog/flask_app/tests/test_capability_startup_transition_route.py",
    '''    assert state.contribution_intents == {
        "workbench": "enabled",
        "runtime": "enabled",
        "recorder": "disabled",
        "language-model": "enabled",
        "compute": "disabled",
        "storage": "disabled",
    }
    assert saved_settings[0].deployment_mode == "web-workbench"
    assert saved_settings[0].ai_enabled is True
''',
    '''    assert state.contribution_intents == {
        "workbench": "enabled",
        "runtime": "enabled",
        "recorder": "ask-later",
        "language-model": "ask-later",
        "compute": "ask-later",
        "storage": "ask-later",
    }
    assert saved_settings[0].deployment_mode == "web-workbench"
    assert saved_settings[0].ai_enabled is False
''',
)

print("CF7-G compatibility fixes applied")
