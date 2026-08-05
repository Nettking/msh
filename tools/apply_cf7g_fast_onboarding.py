from __future__ import annotations

from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    target = Path(path)
    text = target.read_text(encoding="utf-8")
    if old not in text:
        raise SystemExit(f"Expected patch anchor not found in {path}: {old[:80]!r}")
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


service = "catalog/flask_app/services/capability_startup_transition_service.py"

replace_once(
    service,
    '''    @staticmethod
    def _combine_desired(states: Sequence[ContributionDesiredState]) -> str:
        if ContributionDesiredState.ENABLED in states:
            return ContributionDesiredState.ENABLED.value
        if states and all(
            state is ContributionDesiredState.DISABLED for state in states
        ):
            return ContributionDesiredState.DISABLED.value
        return ContributionDesiredState.ASK_LATER.value

    def _current_intents(self) -> dict[str, str]:
''',
    '''    @staticmethod
    def _combine_desired(states: Sequence[ContributionDesiredState]) -> str:
        if ContributionDesiredState.ENABLED in states:
            return ContributionDesiredState.ENABLED.value
        if states and all(
            state is ContributionDesiredState.DISABLED for state in states
        ):
            return ContributionDesiredState.DISABLED.value
        return ContributionDesiredState.ASK_LATER.value

    @staticmethod
    def _fast_start_intents() -> dict[str, str]:
        """Start the workbench without granting optional contribution authority."""

        intents = {
            key: ContributionDesiredState.ASK_LATER.value
            for key in CONTRIBUTION_KEYS
        }
        intents["workbench"] = ContributionDesiredState.ENABLED.value
        intents["runtime"] = ContributionDesiredState.ENABLED.value
        return intents

    def _current_inspection_revision(self, device_id: str) -> int:
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

    def _current_intents(self) -> dict[str, str]:
''',
)

replace_once(
    service,
    '''    def complete_current(self) -> CapabilityStartupState:
        current = self.load()
        if current is not None:
            return current
        context = self._connected_context()
        intents = self._current_intents()
        self._write_compatibility_settings(intents)
        return self.store.save(
            CapabilityStartupState(
                device_id=context.credentials.identity.node_id,
                federation_id=context.binding.federation_id,
                internal_session_id=context.binding.internal_session_id,
                federation_state=context.binding.state,
                inspection_revision=self._inspection_revision(
                    context.credentials.identity.node_id
                ),
                contribution_intents=intents,
                completed=True,
                source_kind="capability-first",
                source_schema=None,
                source_mode=None,
                source_revision=1,
                updated_at=self._clock(),
            )
        )
''',
    '''    def complete_current(self) -> CapabilityStartupState:
        current = self.load()
        if current is not None:
            return current
        context = self._connected_context()
        inspection_revision = self._current_inspection_revision(
            context.credentials.identity.node_id
        )
        intents = self._fast_start_intents()
        self._write_compatibility_settings(intents)
        return self.store.save(
            CapabilityStartupState(
                device_id=context.credentials.identity.node_id,
                federation_id=context.binding.federation_id,
                internal_session_id=context.binding.internal_session_id,
                federation_state=context.binding.state,
                inspection_revision=inspection_revision,
                contribution_intents=intents,
                completed=True,
                source_kind="capability-first",
                source_schema=None,
                source_mode=None,
                source_revision=1,
                updated_at=self._clock(),
            )
        )
''',
)

replace_once(
    service,
    '''        finish_available = isinstance(steps, list) and any(
            isinstance(step, dict)
            and step.get("key") == "finish"
            and bool(step.get("available"))
            for step in steps
        )
        if state is not None and state.completed:
''',
    '''        finish_available = isinstance(steps, list) and any(
            isinstance(step, dict)
            and step.get("key") == "finish"
            and bool(step.get("available"))
            for step in steps
        )
        inspection = view_model.get("inspection")
        fast_finish_available = (
            isinstance(inspection, Mapping)
            and inspection.get("state") == "current"
        )
        if fast_finish_available and isinstance(steps, list):
            for step in steps:
                if isinstance(step, dict) and step.get("key") == "finish":
                    step.update(
                        {
                            "available": True,
                            "summary": "Ready after inspection",
                        }
                    )
        if state is not None and state.completed:
''',
)

replace_once(
    service,
    '''        elif isinstance(finish, dict) and finish_available:
            finish.update(
                {
                    "state": "pending",
                    "state_label": "Ready to finish",
                    "transition_action": {
                        "kind": "finish",
                        "label": "Finish capability-first setup",
                        "url": "/onboarding/finish",
                    },
                }
            )
''',
    '''        elif isinstance(finish, dict) and (
            finish_available or fast_finish_available
        ):
            finish.update(
                {
                    "title": "Open the Federation workbench",
                    "message": (
                        "Benchmarks and service contributions are optional. "
                        "Finish now and review them later from the Federation page."
                    ),
                    "state": "pending",
                    "state_label": "Ready after inspection",
                    "transition_action": {
                        "kind": "finish",
                        "label": "Finish setup and open Federation",
                        "url": "/onboarding/finish",
                    },
                }
            )
''',
)

routes = "catalog/flask_app/capability_startup_transition_routes.py"
replace_once(
    routes,
    '''        "startup-transition-benchmarks-required": (
            "Review the current benchmark evidence before finishing setup."
        ),
''',
    '''        "startup-transition-inspection-required": (
            "Inspect this device before finishing setup."
        ),
        "startup-transition-benchmarks-required": (
            "Review the current benchmark evidence before finishing setup."
        ),
''',
)
replace_once(
    routes,
    '''    flash(message, "success")
    return redirect(
        url_for(
            "capability_startup_transition_web.onboarding",
            step="finish",
        ),
        code=303,
    )
''',
    '''    flash(message, "success")
    return redirect(
        url_for("federation_web.overview"),
        code=303,
    )
''',
)

Path("catalog/flask_app/services/onboarding_view_normalizer.py").write_text(
    '''"""Final presentation normalization for the integrated onboarding flow.

The capability services own evidence and authority. This module presents a fast
three-step first-run flow while keeping benchmarks and contribution choices
available as explicit optional follow-up work.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

_FAST_STEPS = frozenset({"identity", "federation", "inspect"})
_OPTIONAL_STEPS = frozenset({"benchmarks", "contributions", "finish"})


def normalize_onboarding_view_model(
    value: Mapping[str, Any],
    requested_step: str | None = None,
) -> dict[str, Any]:
    """Return one consistent browser model without changing persisted state."""

    view_model = deepcopy(dict(value))
    migration = view_model.get("migration")
    setup_complete = (
        isinstance(migration, Mapping)
        and bool(migration.get("persisted"))
    )
    explicitly_optional = requested_step in _OPTIONAL_STEPS
    inspection = view_model.get("inspection")
    inspection_current = (
        isinstance(inspection, Mapping)
        and inspection.get("state") == "current"
    )

    if not setup_complete and not explicitly_optional:
        steps = view_model.get("steps")
        if isinstance(steps, list):
            view_model["steps"] = [
                step
                for step in steps
                if isinstance(step, dict) and step.get("key") in _FAST_STEPS
            ]
        completed = view_model.get("completed_steps")
        if isinstance(completed, list):
            view_model["completed_steps"] = [
                key for key in completed if key in _FAST_STEPS
            ]
        if requested_step in _FAST_STEPS:
            view_model["current_step"] = requested_step
        elif view_model.get("current_step") not in _FAST_STEPS:
            view_model["current_step"] = (
                "inspect" if inspection_current else "federation"
            )
        return view_model

    contribution_summary = view_model.get("contribution_summary")
    contribution_complete = (
        isinstance(contribution_summary, Mapping)
        and contribution_summary.get("state") == "complete"
    )
    if (
        contribution_complete
        and requested_step is None
        and view_model.get("current_step") == "contributions"
    ):
        view_model["current_step"] = "finish"
    return view_model


__all__ = ["normalize_onboarding_view_model"]
''',
    encoding="utf-8",
)

Path("catalog/flask_app/templates/federation/onboarding/_inspect.html").write_text(
    '''      <section class="onboarding-panel" data-step-panel="inspect" aria-labelledby="inspect-heading">
        <header class="onboarding-panel__header">
          <div>
            <span class="onboarding-panel__step">Step 3 of 3</span>
            <h3 id="inspect-heading">Inspect this device</h3>
            <p>Review supported local services and data sources. Benchmarks and contribution choices can be completed later.</p>
          </div>
          {{ status_badge(vm.inspection.state, vm.inspection.state_label, True) }}
        </header>

        {% if vm.inspection.observations %}
        <dl class="inspection-facts">
          {% for fact in vm.inspection.observations %}<div><dt>{{ fact.label }}</dt><dd>{{ fact.value }}</dd></div>{% endfor %}
        </dl>
        {% else %}
        <p>No inspection evidence has been recorded for this device yet.</p>
        {% endif %}

        <div class="inspection-columns">
          <section>
            <h4>Supported services</h4>
            {% if vm.inspection.services %}<ul class="federation-list">{% for service in vm.inspection.services %}<li><strong>{{ service.label }}</strong><span>{{ service.summary }}</span></li>{% endfor %}</ul>{% else %}<p>No supported local service was detected.</p>{% endif %}
          </section>
          <section>
            <h4>Data sources</h4>
            {% if vm.inspection.data_sources %}<ul class="federation-list">{% for source in vm.inspection.data_sources %}<li><strong>{{ source.label }}</strong><span>{{ source.summary }}</span></li>{% endfor %}</ul>{% else %}<p>No supported data source was detected.</p>{% endif %}
          </section>
        </div>

        {% set recommended_checks = vm.inspection.get('recommended_checks', ()) %}
        {% if recommended_checks %}
        <section aria-labelledby="inspection-checks-heading">
          <h4 id="inspection-checks-heading">Optional checks available later</h4>
          <ul class="federation-list">
            {% for check in recommended_checks %}<li><strong>{{ check.label }}</strong><span>{{ check.summary }}</span></li>{% endfor %}
          </ul>
          <p>These recommendations do not block setup and do not activate a contribution.</p>
        </section>
        {% endif %}

        {% if vm.inspection.warnings %}
        <div class="federation-warning" role="alert"><strong>Inspection notes</strong><ul>{% for warning in vm.inspection.warnings %}<li>{{ warning }}</li>{% endfor %}</ul></div>
        {% endif %}

        {% if vm.inspection.get('can_run', True) %}
        <form method="post" action="{{ vm.actions.inspect_url }}" class="onboarding-inline-actions">
          {% if vm.csrf_token %}<input type="hidden" name="_csrf_token" value="{{ vm.csrf_token }}">{% endif %}
          <button class="federation-button federation-button--secondary" type="submit">{% if vm.inspection.revision %}Run inspection again{% else %}Inspect this device{% endif %}</button>
          <span>Inspection revision {{ vm.inspection.revision }}</span>
        </form>
        {% else %}
        <p>Connect this device to a trusted federation before inspection.</p>
        {% endif %}

        <div class="onboarding-actions">
          <button class="federation-button federation-button--ghost" type="button" data-prev-step>Back</button>
          {% if vm.inspection.state == 'current' %}
          <form method="post" action="{{ vm.actions.finish_url }}">
            {% if vm.csrf_token %}<input type="hidden" name="_csrf_token" value="{{ vm.csrf_token }}">{% endif %}
            {% if vm.command_id %}<input type="hidden" name="command_id" value="{{ vm.command_id }}">{% endif %}
            <button class="federation-button federation-button--primary" type="submit">Finish setup and open Federation</button>
          </form>
          {% else %}
          <button class="federation-button federation-button--primary" type="button" disabled aria-disabled="true">Inspect before finishing</button>
          {% endif %}
        </div>
      </section>''',
    encoding="utf-8",
)

federation_template = Path("catalog/flask_app/templates/federation_overview.html")
text = federation_template.read_text(encoding="utf-8")
anchor = '''  {% if vm.recommended_action %}
  <section class="recommended-action">
    <div><span class="federation-eyebrow">Recommended next action</span><h3>{{ vm.recommended_action.title }}</h3><p>{{ vm.recommended_action.message }}</p></div>
    <a class="federation-button federation-button--primary" href="{{ vm.recommended_action.url }}">{{ vm.recommended_action.label }}</a>
  </section>
  {% endif %}
'''
addition = anchor + '''
  {% if capability_startup_flags.completed|default(false) %}
  <section class="recommended-action">
    <div>
      <span class="federation-eyebrow">Optional device setup</span>
      <h3>Benchmark and contribution review</h3>
      <p>Run checks and choose which services this device may contribute whenever it is convenient. These choices do not block Federation access.</p>
    </div>
    <a class="federation-button federation-button--secondary" href="{{ url_for('capability_startup_transition_web.onboarding', step='benchmarks') }}">Review optional capabilities</a>
  </section>
  {% endif %}
'''
if anchor not in text:
    raise SystemExit("Federation overview insertion anchor not found")
federation_template.write_text(text.replace(anchor, addition, 1), encoding="utf-8")

Path("catalog/flask_app/tests/test_functional_onboarding_progress.py").write_text(
    '''from __future__ import annotations

from copy import deepcopy

from catalog.flask_app.services.onboarding_view_normalizer import (
    normalize_onboarding_view_model,
)


def _view_model() -> dict[str, object]:
    return {
        "current_step": "benchmarks",
        "completed_steps": ["identity", "federation", "inspect"],
        "steps": [
            {"key": "identity", "available": True, "summary": "This device"},
            {"key": "federation", "available": True, "summary": "Connected"},
            {"key": "inspect", "available": True, "summary": "Current"},
            {"key": "benchmarks", "available": True, "summary": "Optional"},
            {"key": "contributions", "available": False, "summary": "Optional"},
            {"key": "finish", "available": True, "summary": "Ready"},
        ],
        "migration": {"persisted": False},
        "inspection": {"state": "current"},
        "benchmark_summary": {
            "state": "pending",
            "label": "Checks available",
            "can_skip": False,
        },
        "benchmarks": [],
        "contribution_summary": {
            "state": "blocked",
            "label": "Review benchmarks first",
        },
    }


def test_empty_benchmark_plan_does_not_block_fast_setup() -> None:
    normalized = normalize_onboarding_view_model(_view_model())

    assert normalized["current_step"] == "inspect"
    assert normalized["completed_steps"] == ["identity", "federation", "inspect"]
    assert [item["key"] for item in normalized["steps"]] == [
        "identity",
        "federation",
        "inspect",
    ]
    assert normalized["benchmark_summary"]["state"] == "pending"


def test_explicit_optional_benchmark_page_remains_available() -> None:
    normalized = normalize_onboarding_view_model(
        _view_model(),
        requested_step="benchmarks",
    )

    assert normalized["current_step"] == "benchmarks"
    assert [item["key"] for item in normalized["steps"]] == [
        "identity",
        "federation",
        "inspect",
        "benchmarks",
        "contributions",
        "finish",
    ]


def test_completed_setup_keeps_full_device_management_flow() -> None:
    value = _view_model()
    value["migration"] = {"persisted": True}
    value["current_step"] = "contributions"
    value["contribution_summary"] = {
        "state": "complete",
        "label": "Choices saved",
    }

    normalized = normalize_onboarding_view_model(value)

    assert normalized["current_step"] == "finish"
    assert len(normalized["steps"]) == 6


def test_normalization_does_not_mutate_service_view_model() -> None:
    value = _view_model()
    original = deepcopy(value)

    normalize_onboarding_view_model(value)

    assert value == original
''',
    encoding="utf-8",
)

Path("catalog/flask_app/tests/test_fast_federation_onboarding.py").write_text(
    '''from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from catalog.federation.errors import FederationOperationError
from catalog.federation.onboarding_models import (
    ContributionDesiredState,
    FederationConnectionState,
)
from catalog.flask_app.services.capability_startup_transition_service import (
    CapabilityStartupTransitionService,
)
from catalog.flask_app.services.server_setup_service import default_settings

NOW = datetime(2026, 8, 5, 15, 30, tzinfo=timezone.utc)


class OnboardingStub:
    def __init__(self) -> None:
        identity = SimpleNamespace(node_id="device-fast-one")
        credentials = SimpleNamespace(identity=identity)
        binding = SimpleNamespace(
            federation_id="federation-fast-one",
            internal_session_id="session-fast-one",
            state=FederationConnectionState.CONNECTED,
        )
        self.context = SimpleNamespace(credentials=credentials, binding=binding)

    def identity_or_none(self):
        return self.context.credentials

    def authorized_context(self):
        return self.context

    def binding_or_none(self):
        return self.context.binding


class InspectionStub:
    def __init__(self, *, current: bool = True) -> None:
        self.snapshot = SimpleNamespace(device_id="device-fast-one", revision=7)
        self.current = current

    def load(self):
        return self.snapshot

    def state(self, snapshot):
        assert snapshot is self.snapshot
        return "current" if self.current else "expired"


class ContributionsMustNotRun:
    def __getattr__(self, name: str):
        raise AssertionError(f"Fast setup must not call contribution service: {name}")


def _service(tmp_path, *, inspection_current: bool = True):
    saved_settings = []
    service = CapabilityStartupTransitionService(
        onboarding_service=OnboardingStub(),
        inspection_service=InspectionStub(current=inspection_current),
        contribution_service=ContributionsMustNotRun(),
        state_database=tmp_path / "onboarding.sqlite3",
        setup_loader=lambda: default_settings(configured=False),
        setup_saver=saved_settings.append,
        clock=lambda: NOW,
    )
    return service, saved_settings


def test_fast_setup_completes_after_inspection_without_benchmarks(tmp_path) -> None:
    service, saved_settings = _service(tmp_path)

    state = service.complete_current()

    assert state.completed is True
    assert state.inspection_revision == 7
    assert state.contribution_intents["workbench"] == (
        ContributionDesiredState.ENABLED.value
    )
    assert state.contribution_intents["runtime"] == (
        ContributionDesiredState.ENABLED.value
    )
    for capability in ("recorder", "language-model", "compute", "storage"):
        assert state.contribution_intents[capability] == (
            ContributionDesiredState.ASK_LATER.value
        )
    assert len(saved_settings) == 1
    assert saved_settings[0].configured is True
    assert saved_settings[0].user_setup_complete is True
    assert saved_settings[0].deployment_mode == "web-workbench"
    assert saved_settings[0].ai_enabled is False


def test_fast_setup_still_requires_current_inspection(tmp_path) -> None:
    service, saved_settings = _service(tmp_path, inspection_current=False)

    with pytest.raises(FederationOperationError) as error:
        service.complete_current()

    assert error.value.code == "startup-transition-inspection-required"
    assert saved_settings == []


def test_fast_setup_templates_expose_finish_and_optional_follow_up() -> None:
    inspect_template = (
        __import__("pathlib").Path(
            "catalog/flask_app/templates/federation/onboarding/_inspect.html"
        ).read_text(encoding="utf-8")
    )
    overview_template = (
        __import__("pathlib").Path(
            "catalog/flask_app/templates/federation_overview.html"
        ).read_text(encoding="utf-8")
    )

    assert "Step 3 of 3" in inspect_template
    assert "Finish setup and open Federation" in inspect_template
    assert "Review optional capabilities" in overview_template
''',
    encoding="utf-8",
)

print("CF7-G fast onboarding patch applied")
