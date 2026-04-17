"""Compatibility facade for shared state/situation inference helpers.

The implementation was split into focused modules to avoid this area becoming
another catch-all utility surface:
- :mod:`catalog.common.state_models` for state heuristics
- :mod:`catalog.common.state_events` for event grouping/interval shaping

Existing imports from ``catalog.common.state_inference`` are kept stable.
"""

from catalog.common.state_events import (
    build_fired_rules,
    extract_intervention_candidates,
    group_boolean_events,
    rows_to_state_intervals,
)
from catalog.common.state_models import StateInferenceConfig, infer_states_for_machine

__all__ = [
    "StateInferenceConfig",
    "build_fired_rules",
    "extract_intervention_candidates",
    "group_boolean_events",
    "infer_states_for_machine",
    "rows_to_state_intervals",
]
