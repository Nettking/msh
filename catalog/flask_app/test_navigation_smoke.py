from __future__ import annotations

from dataclasses import replace

from catalog.flask_app import app as app_module
from catalog.flask_app import routes as routes_module
from catalog.flask_app.app import create_app
from catalog.flask_app.services.server_setup_service import default_settings


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


class FakeSetupDiscovery:
    def last_scan(self) -> dict[str, object]:
        def result(source_id, source_name, host, display_name, machine_id):
            return {
                "source_id": source_id,
                "source_name": source_name,
                "display_name": display_name,
                "base_url": f"http://{host}:5000",
                "machines": [
                    {
                        "display_name": display_name,
                        "reported_name": "Mazak",
                        "machine_id": machine_id,
                        "identity_kind": "uuid",
                    }
                ],
            }

        return {
            "state": "complete",
            "cidr": "192.168.200.0/24",
            "port": 5000,
            "agents_found": 2,
            "machines_found": 2,
            "results": [
                result(
                    "source-a",
                    "M8015RW221N",
                    "192.168.200.101",
                    "Mazak [M8015RW221N]",
                    "M8015RW221N",
                ),
                result(
                    "source-b",
                    "MAZAK-M7ZDA13010Z",
                    "192.168.200.249",
                    "Mazak M7ZDA13010Z",
                    "MAZAK-M7ZDA13010Z",
                ),
            ],
        }

    def recommended_cidr(self, _settings) -> str:
        return "192.168.200.0/24"


def _patch_runtime(monkeypatch, *, requires_choice: bool = False) -> None:
    manager = FakeRuntimeManager(requires_choice=requires_choice)
    monkeypatch.setattr(app_module, "get_runtime_manager", lambda: manager)
    monkeypatch.setattr(routes_module, "get_runtime_manager", lambda: manager)


def _patch_setup(monkeypatch, settings=None) -> None:
    configured_settings = settings or default_settings(configured=True)

    def load_configured_settings():
        return configured_settings

    monkeypatch.setattr(app_module, "load_settings", load_configured_settings)
    monkeypatch.setattr(routes_module, "load_settings", load_configured_settings)
    monkeypatch.setattr("catalog.flask_app.server_setup_routes.load_settings", load_configured_settings)


def test_main_navigation_pages_load(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    _patch_setup(monkeypatch)

    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()

    pages = [
        ("/", "Overview"),
        ("/guide", "How to use MSH"),
        ("/get-started", "What do you want to do first?"),
        ("/startup", "MSH is ready"),
        ("/startup?edit=1&step=ai", "Language-model capability"),
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


def test_get_started_is_a_focused_task_handoff(monkeypatch, tmp_path):
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


def test_main_pages_include_mobile_navigation_but_setup_does_not(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    _patch_setup(monkeypatch)

    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()

    overview = client.get("/").get_data(as_text=True)
    setup = client.get("/startup?edit=1").get_data(as_text=True)

    assert 'data-mobile-navigation' in overview
    assert 'aria-label="Mobile primary sections"' in overview
    assert "Rescan now" in overview
    assert 'data-mobile-navigation' not in setup


def test_knowledge_navigation_opens_a_choice_page(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    _patch_setup(monkeypatch)

    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()

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


def test_recorder_setup_contains_inline_discovery_and_role_scoped_saved_view(
    monkeypatch,
    tmp_path,
):
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch, requires_choice=True)
    settings = replace(
        default_settings(configured=True),
        deployment_mode="recorder-only",
        ai_enabled=False,
        recorder_sources="M8015RW221N=http://192.168.200.101:5000",
    )
    _patch_setup(monkeypatch, settings)
    monkeypatch.setattr(
        routes_module,
        "get_mtconnect_discovery_service",
        FakeSetupDiscovery,
    )

    def fail_if_ollama_is_contacted(*_args, **_kwargs):
        raise AssertionError("Recorder-only pages must not contact Ollama.")

    monkeypatch.setattr(app_module, "ollama_status", fail_if_ollama_is_contacted)

    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()

    edit_html = client.get("/startup?edit=1&step=recorder").get_data(
        as_text=True
    )
    saved_html = client.get("/startup").get_data(as_text=True)
    status_html = client.get("/status").get_data(as_text=True)
    workbench_response = client.get("/")
    guide_response = client.get("/guide")

    assert 'id="setupDiscoveryButton"' in edit_html
    assert 'id="setupDiscoveryResults"' in edit_html
    assert edit_html.count('name="source_id"') == 2
    assert "Mazak [M8015RW221N]" in edit_html
    assert "Mazak M7ZDA13010Z" in edit_html
    assert "MAZAK-M7ZDA13010Z" in edit_html
    assert "Checked MTConnect Agents will be added" in edit_html
    assert "Scan for MTConnect machines" not in edit_html
    assert "Open recorder status" in saved_html
    assert "<h4>Recorder</h4>" in saved_html
    assert "<h4>Language-model provider</h4>" not in saved_html
    assert "AI Explainer" not in saved_html
    assert ">Monitor</a>" not in saved_html
    assert "Recorder status" in status_html
    assert "MTConnect network discovery" in status_html
    assert "Open source inventory" not in status_html
    assert "Open control" not in status_html
    assert workbench_response.status_code == 302
    assert workbench_response.location == "/status"
    assert guide_response.status_code == 200


def test_malformed_saved_discovery_results_do_not_break_setup(
    monkeypatch,
    tmp_path,
):
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    _patch_setup(monkeypatch, default_settings(configured=True))

    class MalformedDiscovery(FakeSetupDiscovery):
        def last_scan(self) -> dict[str, object]:
            return {
                "state": "complete",
                "results": [{"base_url": "invalid", "machines": None}],
            }

    monkeypatch.setattr(
        routes_module,
        "get_mtconnect_discovery_service",
        MalformedDiscovery,
    )
    app = create_app()
    app.config.update(TESTING=True)

    response = app.test_client().get("/startup?edit=1&step=recorder")

    assert response.status_code == 200


def test_web_workbench_hides_recorder_only_saved_options(
    monkeypatch,
    tmp_path,
):
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    _patch_setup(monkeypatch, default_settings(configured=True))

    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()

    saved_html = client.get("/startup").get_data(as_text=True)
    status_html = client.get("/status").get_data(as_text=True)

    assert "<h4>Language-model provider</h4>" in saved_html
    assert "<h4>Recorder</h4>" not in saved_html
    assert "MTConnect network discovery" not in status_html
    assert "Recorder station</h3>" not in status_html


def test_web_ui_only_does_not_require_a_runtime_start_choice(
    monkeypatch,
    tmp_path,
):
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch, requires_choice=True)
    settings = replace(
        default_settings(configured=True),
        deployment_mode="web-ui-only",
    )
    _patch_setup(monkeypatch, settings)

    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()

    overview_response = client.get("/")
    saved_html = client.get("/startup").get_data(as_text=True)

    assert overview_response.status_code == 200
    assert "Resume session" not in saved_html
    assert "Open overview" in saved_html


def test_full_server_can_open_recorder_status_before_runtime_choice(
    monkeypatch,
    tmp_path,
):
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch, requires_choice=True)
    settings = replace(
        default_settings(configured=True),
        deployment_mode="full-server",
        recorder_sources="MAZAK-001=http://192.168.200.249:5000",
    )
    _patch_setup(monkeypatch, settings)

    app = create_app()
    app.config.update(TESTING=True)

    response = app.test_client().get("/status")

    assert response.status_code == 200
    assert "Recorder station" in response.get_data(as_text=True)
