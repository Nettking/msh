from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from catalog.flask_app import app as app_module
from catalog.flask_app import capability_product_routes
from catalog.flask_app import capability_startup_transition_routes as transition_routes
from catalog.flask_app import routes as routes_module
from catalog.flask_app import server_setup_routes as setup_routes_module
from catalog.flask_app.app import create_app
from catalog.flask_app.services.capability_config_service import (
    CapabilityConfig,
    default_capability_config,
)


class FakeRuntimeManager:
    def __init__(self, *, requires_choice: bool = False) -> None:
        self._requires_choice = requires_choice

    def mark_app_started(self) -> None:
        pass

    def requires_startup_choice(self) -> bool:
        return self._requires_choice

    def startup_decision_snapshot(self) -> dict[str, bool]:
        return {"requires_choice": self._requires_choice}

    def state_snapshot(self) -> dict[str, object]:
        return {
            "current_processing_phase": "runtime_not_started",
            "startup_mode": "continue_existing",
            "last_failure": "",
            "update_running": False,
            "view_contracts": {},
        }


class FakeStartupTransition:
    def __init__(
        self,
        *,
        completed: bool,
        enabled: frozenset[str],
    ) -> None:
        self.completed = completed
        self.enabled = enabled

    def capability_flags(self) -> dict[str, object]:
        def is_enabled(capability: str) -> bool:
            return self.completed and capability in self.enabled

        return {
            "workbench": is_enabled("workbench"),
            "runtime": is_enabled("runtime"),
            "recorder": is_enabled("recorder"),
            "language_model": is_enabled("language_model"),
            "compute": False,
            "storage": False,
            "completed": self.completed,
            "needs_migration": False,
            "source": "capability-first" if self.completed else "unconfigured",
            "state": object() if self.completed else None,
        }


def _patch_runtime(monkeypatch, *, requires_choice: bool = False) -> None:
    manager = FakeRuntimeManager(requires_choice=requires_choice)
    monkeypatch.setattr(app_module, "get_runtime_manager", lambda: manager)
    monkeypatch.setattr(routes_module, "get_runtime_manager", lambda: manager)
    monkeypatch.setattr(
        capability_product_routes,
        "get_runtime_manager",
        lambda: manager,
    )


def _patch_setup(
    monkeypatch,
    config: CapabilityConfig | None = None,
    *,
    completed: bool = True,
    enabled: frozenset[str] = frozenset(
        {"workbench", "runtime", "recorder", "language_model"}
    ),
    recorder_active: bool = False,
) -> None:
    configured = config or default_capability_config()
    transition = FakeStartupTransition(completed=completed, enabled=enabled)
    monkeypatch.setattr(
        app_module,
        "get_capability_startup_transition_service",
        lambda: transition,
    )
    monkeypatch.setattr(
        transition_routes,
        "get_capability_startup_transition_service",
        lambda: transition,
    )
    monkeypatch.setattr(
        capability_product_routes,
        "get_capability_startup_transition_service",
        lambda: transition,
    )
    monkeypatch.setattr(
        capability_product_routes,
        "_load_product_config",
        lambda: (configured, ""),
    )
    monkeypatch.setattr(
        capability_product_routes,
        "_active_recorder_contribution",
        lambda: recorder_active,
    )


def _open_returning_device(client) -> None:
    landing = client.get("/")
    assert landing.status_code == 302
    assert landing.location == "/federation"


def test_main_navigation_pages_load(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    _patch_setup(monkeypatch)

    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()
    _open_returning_device(client)

    pages = [
        ("/", "Overview"),
        ("/guide", "How to use FCP"),
        ("/get-started", "What do you want to do first?"),
        ("/sources/", "Sources"),
        ("/status", "Diagnostics"),
        ("/operator-strategies", "Knowledge workspace"),
        ("/operator-strategies/capture", "Capture"),
        ("/operator-strategies/review", "Review Notes"),
        ("/strategy-comparison", "Strategies"),
        ("/strategies", "Intervention Logic"),
        ("/osl-export", "OSL to SysML export"),
        ("/assist", "Assist"),
    ]

    for path, expected_text in pages:
        response = client.get(path)
        assert response.status_code == 200, path
        assert expected_text in response.get_data(as_text=True), path


def test_get_started_is_a_focused_task_handoff(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    _patch_setup(monkeypatch)

    app = create_app()
    app.config.update(TESTING=True)
    html = app.test_client().get("/get-started").get_data(as_text=True)

    assert '<body class="setup-focus">' in html
    assert "site-nav--primary" not in html
    assert "Artifact scan:" not in html
    assert "Rescan" not in html
    assert "Capture operator knowledge" in html
    assert 'href="/operator-strategies/capture"' in html
    assert 'href="/sources/"' in html
    assert "Full workbench" in html


def test_main_pages_include_mobile_navigation_and_retired_startup_redirects(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    _patch_setup(monkeypatch)

    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()
    _open_returning_device(client)

    overview = client.get("/").get_data(as_text=True)
    startup_response = client.get("/startup?legacy=1")

    assert 'data-mobile-navigation' in overview
    assert 'aria-label="Mobile primary sections"' in overview
    assert "Rescan now" in overview
    assert startup_response.status_code == 302
    assert startup_response.location == "/"


def test_knowledge_navigation_opens_a_choice_page(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    _patch_setup(monkeypatch)

    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()
    _open_returning_device(client)

    overview_html = client.get("/").get_data(as_text=True)
    knowledge_response = client.get("/operator-strategies")
    knowledge_html = knowledge_response.get_data(as_text=True)

    assert knowledge_response.status_code == 200
    assert 'href="/operator-strategies"' in overview_html
    assert "Capture now or review later" in knowledge_html
    assert 'href="/operator-strategies/capture"' in knowledge_html
    assert 'href="/operator-strategies/review"' in knowledge_html
    assert 'href="/strategy-comparison"' in knowledge_html
    assert 'href="/strategies"' in knowledge_html
    assert 'href="/osl-export"' in knowledge_html


def test_recorder_configuration_does_not_scope_product_navigation(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch, requires_choice=True)
    config = replace(
        default_capability_config(),
        recorder_sources="M8015RW221N=http://192.168.200.101:5000",
    )
    _patch_setup(monkeypatch, config)

    def fail_if_ollama_is_contacted(*_args, **_kwargs):
        raise AssertionError("Diagnostics must not probe Ollama implicitly.")

    monkeypatch.setattr(
        setup_routes_module,
        "ollama_status",
        fail_if_ollama_is_contacted,
    )

    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()

    status_response = client.get("/status")
    status_html = status_response.get_data(as_text=True)
    first_workbench_response = client.get("/")
    workbench_response = client.get("/")
    guide_response = client.get("/guide")

    assert status_response.status_code == 200
    assert "Diagnostics" in status_html
    assert 'data-recorder-dashboard' in status_html
    assert "Records this run" in status_html
    assert "Add or rescan machines" in status_html
    assert "data-source-state" in status_html
    assert "data-next-sequence" in status_html
    assert first_workbench_response.status_code == 302
    assert first_workbench_response.location == "/federation"
    assert workbench_response.status_code == 200
    assert "Overview" in workbench_response.get_data(as_text=True)
    assert guide_response.status_code == 200


def test_configured_recorder_is_visible_before_runtime_choice(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch, requires_choice=True)
    config = replace(
        default_capability_config(),
        recorder_sources="MAZAK-001=http://192.168.200.249:5000",
    )
    _patch_setup(monkeypatch, config)

    app = create_app()
    app.config.update(TESTING=True)

    response = app.test_client().get("/status")

    assert response.status_code == 200
    assert "data-recorder-dashboard" in response.get_data(as_text=True)


def test_recorder_live_status_endpoint_is_small_fresh_and_authority_scoped(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch, requires_choice=True)
    config = replace(
        default_capability_config(),
        recorder_sources="M8015RW221N=http://192.168.200.101:5000",
    )
    _patch_setup(monkeypatch, config, recorder_active=True)

    class FakeRecorder:
        calls = 0

        def web_status(self, _config):
            self.calls += 1
            return {
                "schema": "fcp.recorder.web_status.v1",
                "generated_at": "2026-07-28T12:45:10Z",
                "poll_after_ms": 2000,
                "ready": True,
                "requested_enabled": True,
                "running": True,
                "worker_alive": True,
                "state": "recording",
                "message": "Recording one MTConnect source.",
                "heartbeat_at": "2026-07-28T12:45:10Z",
                "records_written": 15021 + self.calls,
                "records_buffered": 0,
                "last_flush_at": "2026-07-28T12:45:10Z",
                "sources": [
                    {
                        "source_name": "M8015RW221N",
                        "machine_id": "M8015RW221N",
                        "base_url": "http://192.168.200.101:5000",
                        "last_success_at": "2026-07-28T12:45:10Z",
                        "next_sequence": 10932 + self.calls,
                        "agent_last_sequence": 10931 + self.calls,
                        "caught_up": True,
                        "last_error": "",
                        "state": "caught_up",
                    }
                ],
            }

    fake_recorder = FakeRecorder()
    monkeypatch.setattr(
        capability_product_routes,
        "get_recorder_control_service",
        lambda: fake_recorder,
    )

    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()

    first = client.get(
        "/status/recorder.json",
        headers={"Accept": "application/json"},
    )
    second = client.get(
        "/status/recorder.json",
        headers={"Accept": "application/json"},
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.is_json
    assert first.headers["Cache-Control"] == "no-store, max-age=0"
    assert first.headers["X-Content-Type-Options"] == "nosniff"
    assert first.get_json()["records_written"] == 15022
    assert second.get_json()["records_written"] == 15023
    assert second.get_json()["sources"][0]["next_sequence"] == 10934
    assert "log_tail" not in second.get_json()
    assert "status_path" not in second.get_json()


def test_recorder_live_script_polls_without_unsafe_html() -> None:
    script = (
        Path(__file__).parent / "static" / "js" / "recorder-status.js"
    ).read_text(encoding="utf-8")

    assert 'cache: "no-store"' in script
    assert "AbortController" in script
    assert "document.hidden" in script
    assert "pageshow" in script
    assert "requestTimeoutMs" in script
    assert "setTimeout" in script
    assert "textContent" in script
    assert "innerHTML" not in script


def test_recorder_live_status_endpoint_rejects_inactive_contribution(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    config = replace(
        default_capability_config(),
        recorder_sources="M8015RW221N=http://192.168.200.101:5000",
    )
    _patch_setup(monkeypatch, config, recorder_active=False)

    app = create_app()
    app.config.update(TESTING=True)
    response = app.test_client().get(
        "/status/recorder.json",
        headers={"Accept": "application/json"},
    )

    assert response.status_code == 409
    assert response.is_json
    assert response.get_json()["error"] == "recorder_not_enabled"


def test_recorder_live_status_endpoint_returns_json_when_setup_is_incomplete(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    _patch_setup(monkeypatch, completed=False)

    app = create_app()
    app.config.update(TESTING=True)
    response = app.test_client().get(
        "/status/recorder.json",
        headers={"Accept": "application/json"},
    )

    assert response.status_code == 409
    assert response.is_json
    assert response.headers["Cache-Control"] == "no-store, max-age=0"
    assert response.get_json()["error"] == "setup_required"
