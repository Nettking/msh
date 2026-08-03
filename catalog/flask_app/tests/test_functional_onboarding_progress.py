from __future__ import annotations

from copy import deepcopy

from catalog.flask_app.services.onboarding_view_normalizer import (
    normalize_onboarding_view_model,
)


def _view_model() -> dict[str, object]:
    return {
        "current_step": "contributions",
        "completed_steps": [
            "identity",
            "federation",
            "inspect",
            "benchmarks",
            "contributions",
            "finish",
        ],
        "steps": [
            {"key": "identity", "available": True, "summary": "This device"},
            {"key": "federation", "available": True, "summary": "Connected"},
            {"key": "inspect", "available": True, "summary": "Current"},
            {"key": "benchmarks", "available": True, "summary": "Complete"},
            {"key": "contributions", "available": True, "summary": "Complete"},
            {"key": "finish", "available": True, "summary": "Ready"},
        ],
        "inspection": {"state": "current"},
        "benchmark_summary": {
            "state": "complete",
            "label": "All recommended checks reviewed",
            "can_skip": False,
        },
        "benchmarks": [],
        "contribution_summary": {
            "state": "complete",
            "label": "No supported contributions found",
        },
    }


def test_empty_benchmark_plan_cannot_complete_later_steps() -> None:
    value = _view_model()

    normalized = normalize_onboarding_view_model(value)

    assert normalized["current_step"] == "benchmarks"
    assert normalized["benchmark_summary"] == {
        "state": "blocked",
        "label": "No supported checks configured",
        "can_skip": False,
        "setup_url": "/startup",
    }
    assert normalized["completed_steps"] == ["identity", "federation", "inspect"]
    steps = {item["key"]: item for item in normalized["steps"]}
    assert steps["benchmarks"]["available"] is True
    assert steps["benchmarks"]["summary"] == "Configure a capability first"
    assert steps["contributions"]["available"] is False
    assert steps["finish"]["available"] is False


def test_complete_contribution_step_defaults_to_finish() -> None:
    value = _view_model()
    value["benchmarks"] = [{"id": "check-one"}]

    normalized = normalize_onboarding_view_model(value)

    assert normalized["current_step"] == "finish"


def test_explicit_contribution_step_is_respected() -> None:
    value = _view_model()
    value["benchmarks"] = [{"id": "check-one"}]

    normalized = normalize_onboarding_view_model(
        value,
        requested_step="contributions",
    )

    assert normalized["current_step"] == "contributions"


def test_normalization_does_not_mutate_service_view_model() -> None:
    value = _view_model()
    original = deepcopy(value)

    normalize_onboarding_view_model(value)

    assert value == original
