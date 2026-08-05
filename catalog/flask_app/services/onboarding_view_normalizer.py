"""Final presentation normalization for the integrated onboarding flow.

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
