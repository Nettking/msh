from __future__ import annotations

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
