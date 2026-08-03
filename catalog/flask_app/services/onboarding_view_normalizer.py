"""Final presentation normalization for the integrated onboarding flow.

The individual CFI services intentionally own their evidence and authority. This
module only prevents the composed browser view from claiming progress that the
combined product cannot actually perform.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping


def _step(view_model: dict[str, Any], key: str) -> dict[str, Any] | None:
    steps = view_model.get("steps")
    if not isinstance(steps, list):
        return None
    for item in steps:
        if isinstance(item, dict) and item.get("key") == key:
            return item
    return None


def _remove_completed(view_model: dict[str, Any], *keys: str) -> None:
    completed = view_model.get("completed_steps")
    if not isinstance(completed, list):
        return
    blocked = set(keys)
    completed[:] = [value for value in completed if value not in blocked]


def normalize_onboarding_view_model(
    value: Mapping[str, Any],
    requested_step: str | None = None,
) -> dict[str, Any]:
    """Return one consistent browser model without changing persisted state."""

    view_model = deepcopy(dict(value))
    benchmarks = view_model.get("benchmarks")
    benchmark_items = benchmarks if isinstance(benchmarks, list) else []
    inspection = view_model.get("inspection")
    inspection_current = (
        isinstance(inspection, Mapping)
        and inspection.get("state") == "current"
    )

    if inspection_current and not benchmark_items:
        summary = view_model.get("benchmark_summary")
        if not isinstance(summary, dict):
            summary = {}
            view_model["benchmark_summary"] = summary
        summary.update(
            {
                "state": "blocked",
                "label": "No supported checks configured",
                "can_skip": False,
                "setup_url": "/startup",
            }
        )
        _remove_completed(view_model, "benchmarks", "contributions", "finish")
        benchmark_step = _step(view_model, "benchmarks")
        if benchmark_step is not None:
            benchmark_step.update(
                {
                    "available": True,
                    "summary": "Configure a capability first",
                }
            )
        for key in ("contributions", "finish"):
            item = _step(view_model, key)
            if item is not None:
                item["available"] = False
        if requested_step not in {"identity", "federation", "inspect"}:
            view_model["current_step"] = "benchmarks"
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
