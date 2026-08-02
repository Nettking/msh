from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from jinja2 import ChoiceLoader, DictLoader, Environment, FileSystemLoader, StrictUndefined, select_autoescape

HERE = Path(__file__).resolve().parent
FLASK_APP = HERE.parent
TEMPLATES = FLASK_APP / "templates"
STATIC = FLASK_APP / "static"
FIXTURE_FILES = (
    HERE / "fixtures" / "cf5_onboarding_view_models.json",
    HERE / "fixtures" / "cf5_overview_view_models.json",
)


@pytest.fixture(scope="module")
def fixtures() -> dict[str, object]:
    models: dict[str, object] = {}
    for fixture_file in FIXTURE_FILES:
        models.update(json.loads(fixture_file.read_text(encoding="utf-8")))
    return models


@pytest.fixture(scope="module")
def environment() -> Environment:
    loader = ChoiceLoader(
        [
            DictLoader({"base.html": "<!doctype html><main>{% block body %}{% endblock %}</main>"}),
            FileSystemLoader(TEMPLATES),
        ]
    )
    env = Environment(
        loader=loader,
        autoescape=select_autoescape(("html", "xml")),
        undefined=StrictUndefined,
    )
    env.globals["url_for"] = lambda endpoint, **values: (
        f"/static/{values['filename']}" if endpoint == "static" else f"/{endpoint}"
    )
    return env


def merged(base: dict[str, object], overlay: dict[str, object]) -> dict[str, object]:
    result = copy.deepcopy(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merged(result[key], value)  # type: ignore[arg-type]
        else:
            result[key] = copy.deepcopy(value)
    return result


def render(environment: Environment, template: str, view_model: dict[str, object]) -> str:
    return environment.get_template(template).render(view_model=view_model)


def test_onboarding_renders_six_capability_first_steps(
    environment: Environment,
    fixtures: dict[str, object],
) -> None:
    html = render(environment, "onboarding.html", fixtures["onboarding_fresh"])

    for step in ("Identity", "Federation", "Inspect", "Benchmarks", "Contributions", "Finish"):
        assert f">{step}<" in html
    assert html.count("data-step-panel=") == 6
    assert "What should this computer do?" not in html
    assert "deployment_mode" not in html
    assert "provider enrollment" not in html.lower()
    assert "primary/replica" not in html.lower()
    assert "handler terminology" not in html.lower()
    assert "/docs/capability-first-onboarding" in html


def test_migration_fixture_preserves_behavior_without_private_values(
    environment: Environment,
    fixtures: dict[str, object],
) -> None:
    vm = merged(
        fixtures["onboarding_fresh"],
        fixtures["onboarding_migration"],
    )
    html = render(environment, "onboarding.html", vm)

    assert "Existing installation found" in html
    assert "Web workbench" in html
    assert "Storage and compute stay disabled" in html
    assert "Private connection details remain local" in html
    assert "192.168." not in html
    assert "ollama_base_url" not in html
    assert "recorder_sources" not in html
    assert "fixture-csrf-token" in html


def test_skipped_benchmark_does_not_enable_storage_contribution(
    environment: Environment,
    fixtures: dict[str, object],
) -> None:
    html = render(environment, "onboarding.html", fixtures["onboarding_fresh"])

    assert "This optional benchmark was skipped. Storage remains disabled." in html
    assert 'data-benchmark-card="storage-roundtrip-v1"' in html
    assert 'data-contribution-card="candidate-storage"' in html
    storage_card = html.split('data-contribution-card="candidate-storage"', maxsplit=1)[1]
    storage_card = storage_card.split("</article>", maxsplit=1)[0]
    assert 'value="disabled"' in storage_card
    assert 'value="disabled"\n        data-contribution-choice\n        checked' in storage_card
    assert "disabled" in storage_card.split("<fieldset", maxsplit=1)[1].split(">", maxsplit=1)[0]


def test_benchmark_and_contribution_controls_are_post_forms(
    environment: Environment,
    fixtures: dict[str, object],
) -> None:
    html = render(environment, "onboarding.html", fixtures["onboarding_fresh"])

    assert html.count("data-benchmark-form") == 3
    assert 'action="/onboarding/benchmarks/run"' in html
    assert 'action="/onboarding/benchmarks/skip"' in html
    assert 'action="/onboarding/contributions"' in html
    assert html.count("data-contribution-choice") == 9
    assert "Passing a benchmark does not enable or authorize a contribution." in html


def test_overview_exposes_all_federation_sections_and_safe_details(
    environment: Environment,
    fixtures: dict[str, object],
) -> None:
    html = render(environment, "federation_overview.html", fixtures["overview_connected"])

    for section in (
        "Overview",
        "This device",
        "Devices",
        "Services",
        "Benchmarks",
        "Storage",
        "Jobs",
        "Activity",
        "Settings",
    ):
        assert f">{section}<" in html
    assert "Recommended next action" in html
    assert "Private addresses, credentials, and local provider configuration" in html
    assert "http://192.168" not in html
    assert "503" not in html
    assert "internal_session_id" not in html


def test_degraded_overview_has_guided_repair_instead_of_raw_error(
    environment: Environment,
    fixtures: dict[str, object],
) -> None:
    vm = merged(fixtures["overview_connected"], fixtures["overview_degraded"])
    html = render(environment, "federation_overview.html", vm)

    assert "This device could not reconnect" in html
    assert "Your saved identity is intact" in html
    assert "Open guided repair" in html
    assert "/onboarding?repair=1" in html
    assert "No active contributions" in html
    assert "No benchmark results yet" in html


def test_static_behavior_persists_navigation_and_never_fakes_authority() -> None:
    javascript = (STATIC / "js" / "onboarding.js").read_text(encoding="utf-8")

    assert "window.localStorage" in javascript
    assert "data.progressRevision" not in javascript
    assert "stored.revision === revision" in javascript
    assert "Starting bounded check" in javascript
    assert "preventDefault" not in javascript
    assert "storageRemove(storageKey)" in javascript
    assert "enabledCount" in javascript


def test_css_includes_mobile_focus_and_reduced_motion_support() -> None:
    css = (STATIC / "css" / "federation.css").read_text(encoding="utf-8")

    assert "@media (max-width: 720px)" in css
    assert "@media (prefers-reduced-motion: reduce)" in css
    assert ":focus-visible" in css
    assert ".onboarding-shell.is-enhanced .onboarding-panel.is-active" in css
    assert ".federation-section-nav" in css
