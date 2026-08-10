from __future__ import annotations

from flask import Flask

from catalog.flask_app import app as app_module


class _Transition:
    def __init__(self, *, completed: bool, runtime: bool = True) -> None:
        self.completed = completed
        self.runtime = runtime
        self.calls = 0

    def capability_flags(self) -> dict[str, bool]:
        self.calls += 1
        return {
            "completed": self.completed,
            "runtime": self.runtime,
        }


class _Runtime:
    def __init__(self, *, pending: bool) -> None:
        self.pending = pending
        self.choices: list[str] = []

    def requires_startup_choice(self) -> bool:
        return self.pending

    def choose_startup_mode(self, mode: str) -> tuple[bool, str]:
        self.choices.append(mode)
        self.pending = False
        return True, "started"


def test_completed_capability_setup_continues_existing_runtime(
    monkeypatch,
) -> None:
    app = Flask(__name__)
    transition = _Transition(completed=True)
    runtime = _Runtime(pending=True)
    background_starts: list[str] = []

    monkeypatch.setattr(
        app_module,
        "get_capability_startup_transition_service",
        lambda: transition,
    )
    monkeypatch.setattr(app_module, "get_runtime_manager", lambda: runtime)
    monkeypatch.setattr(
        app_module,
        "start_runtime_background",
        lambda: background_starts.append("started"),
    )

    state = app_module._start_runtime_from_capability_state(app)

    assert state == "started"
    assert transition.calls == 1
    assert runtime.choices == ["continue_existing"]
    assert background_starts == []
    assert runtime.requires_startup_choice() is False


def test_runtime_handoff_has_no_legacy_setup_argument(monkeypatch) -> None:
    app = Flask(__name__)
    transition = _Transition(completed=False, runtime=False)
    runtime = _Runtime(pending=True)

    monkeypatch.setattr(
        app_module,
        "get_capability_startup_transition_service",
        lambda: transition,
    )
    monkeypatch.setattr(app_module, "get_runtime_manager", lambda: runtime)

    state = app_module._start_runtime_from_capability_state(app)

    assert state == "disabled"
    assert transition.calls == 1
    assert runtime.choices == []


def test_incomplete_legacy_preview_keeps_explicit_runtime_choice(
    monkeypatch,
) -> None:
    app = Flask(__name__)
    transition = _Transition(completed=False)
    runtime = _Runtime(pending=True)

    monkeypatch.setattr(
        app_module,
        "get_capability_startup_transition_service",
        lambda: transition,
    )
    monkeypatch.setattr(app_module, "get_runtime_manager", lambda: runtime)

    state = app_module._start_runtime_from_capability_state(app)

    assert state == "legacy-choice-required"
    assert runtime.choices == []


def test_resolved_capability_runtime_starts_existing_background_entrypoint(
    monkeypatch,
) -> None:
    app = Flask(__name__)
    transition = _Transition(completed=True)
    runtime = _Runtime(pending=False)
    background_starts: list[str] = []

    monkeypatch.setattr(
        app_module,
        "get_capability_startup_transition_service",
        lambda: transition,
    )
    monkeypatch.setattr(app_module, "get_runtime_manager", lambda: runtime)
    monkeypatch.setattr(
        app_module,
        "start_runtime_background",
        lambda: background_starts.append("started"),
    )

    state = app_module._start_runtime_from_capability_state(app)

    assert state == "started"
    assert transition.calls == 1
    assert runtime.choices == []
    assert background_starts == ["started"]
