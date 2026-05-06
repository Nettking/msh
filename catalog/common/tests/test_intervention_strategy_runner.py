from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from catalog.common.intervention_strategy_runner import (
    CANDIDATE_EVENT_COLUMNS,
    StrategyConfigError,
    build_strategy_summary,
    load_label_config,
    load_strategy_config,
    intervention_strategy_config_signature,
    run_intervention_strategies,
    validate_strategies,
    write_strategy_outputs,
)


def _write_yaml(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


def _labels(path: Path) -> Path:
    return _write_yaml(
        path,
        """
labels:
  operator_override_change:
    description: Override changed.
  spindle_load_collapse:
    description: Load collapsed.
  tool_change:
    description: Tool changed.
  unknown:
    description: Needs review.
""".strip()
        + "\n",
    )


def _telemetry() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01 00:00:00", periods=4, freq="10s"),
            "machine_id": ["M1", "M1", "M1", "M1"],
            "Sovr": [100, 95, 80, 82],
            "Sload": [80, 78, 30, 31],
            "Srpm": [1200, 1200, 1190, 1180],
            "Tool_number": [1, 1, 2, 2],
            "Tool_group": ["A", "A", "B", "B"],
        }
    )


def test_disabled_strategies_are_ignored(tmp_path: Path) -> None:
    labels = _labels(tmp_path / "labels.yaml")
    strategies = _write_yaml(
        tmp_path / "strategies.yaml",
        """
strategies:
  - id: disabled_drop
    enabled: false
    type: delta_threshold
    suggested_label: operator_override_change
    signal: Sovr
    threshold: -1
    window_seconds: 30
    description: Disabled rule.
""".strip()
        + "\n",
    )

    candidates = run_intervention_strategies(_telemetry(), strategies_path=strategies, labels_path=labels)

    assert candidates.empty
    assert list(candidates.columns) == CANDIDATE_EVENT_COLUMNS


def test_delta_threshold_detects_override_drop(tmp_path: Path) -> None:
    labels = _labels(tmp_path / "labels.yaml")
    strategies = _write_yaml(
        tmp_path / "strategies.yaml",
        """
strategies:
  - id: override_drop
    enabled: true
    type: delta_threshold
    suggested_label: operator_override_change
    signal: Sovr
    threshold: -10
    window_seconds: 30
    description: Override drop.
""".strip()
        + "\n",
    )

    candidates = run_intervention_strategies(_telemetry(), strategies_path=strategies, labels_path=labels)

    assert len(candidates) == 1
    row = candidates.iloc[0]
    assert row["strategy_id"] == "override_drop"
    assert row["event_score"] == 15
    assert "delta" in row["evidence"]


def test_ratio_drop_detects_load_collapse_with_companion_evidence(tmp_path: Path) -> None:
    labels = _labels(tmp_path / "labels.yaml")
    strategies = _write_yaml(
        tmp_path / "strategies.yaml",
        """
strategies:
  - id: spindle_load_collapse
    enabled: true
    type: ratio_drop
    suggested_label: spindle_load_collapse
    signal: Sload
    companion_signal: Srpm
    ratio_threshold: 0.5
    window_seconds: 30
    description: Load collapse.
""".strip()
        + "\n",
    )

    candidates = run_intervention_strategies(_telemetry(), strategies_path=strategies, labels_path=labels)

    assert len(candidates) == 1
    row = candidates.iloc[0]
    assert row["strategy_id"] == "spindle_load_collapse"
    assert pytest.approx(row["event_score"], rel=1e-6) == 1 - (30 / 78)
    assert "Srpm" in row["evidence"]


def test_value_change_detects_tool_changes(tmp_path: Path) -> None:
    labels = _labels(tmp_path / "labels.yaml")
    strategies = _write_yaml(
        tmp_path / "strategies.yaml",
        """
strategies:
  - id: tool_number_change
    enabled: true
    type: value_change
    suggested_label: tool_change
    signal: Tool_number
    window_seconds: 30
    description: Tool number change.
  - id: tool_group_change
    enabled: true
    type: value_change
    suggested_label: tool_change
    signal: Tool_group
    window_seconds: 30
    description: Tool group change.
""".strip()
        + "\n",
    )

    candidates = run_intervention_strategies(_telemetry(), strategies_path=strategies, labels_path=labels)

    assert set(candidates["strategy_id"]) == {"tool_number_change", "tool_group_change"}
    assert set(candidates["event_score"]) == {1.0}


def test_output_schema_includes_review_fields_and_summary_outputs(tmp_path: Path) -> None:
    labels = _labels(tmp_path / "labels.yaml")
    strategies = _write_yaml(
        tmp_path / "strategies.yaml",
        """
strategies:
  - id: override_drop
    enabled: true
    type: delta_threshold
    suggested_label: operator_override_change
    signal: Sovr
    threshold: -10
    window_seconds: 30
    description: Override drop.
""".strip()
        + "\n",
    )

    paths = write_strategy_outputs(_telemetry(), tmp_path / "outputs", strategies_path=strategies, labels_path=labels)
    candidates = pd.read_csv(paths["candidate_events"])
    summary = pd.read_csv(paths["strategy_summary"])

    assert list(candidates.columns) == CANDIDATE_EVENT_COLUMNS
    assert candidates.loc[0, "review_status"] == "unreviewed"
    assert pd.isna(candidates.loc[0, "human_label"])
    assert pd.isna(candidates.loc[0, "notes"])
    assert summary.to_dict("records") == [
        {
            "strategy_id": "override_drop",
            "suggested_label": "operator_override_change",
            "candidate_count": 1,
            "mean_score": 15.0,
        }
    ]
    used_text = paths["strategies_used"].read_text(encoding="utf-8")
    assert "strategy_config_signature:" in used_text
    assert "strategies:" in used_text


def test_strategy_signature_changes_when_active_config_changes(tmp_path: Path) -> None:
    labels = _labels(tmp_path / "labels.yaml")
    strategies = tmp_path / "strategies.yaml"
    _write_yaml(
        strategies,
        """
strategies:
  - id: override_drop
    enabled: true
    type: delta_threshold
    suggested_label: operator_override_change
    signal: Sovr
    threshold: -10
    window_seconds: 30
    description: Override drop.
""".strip()
        + "\n",
    )
    initial = intervention_strategy_config_signature(strategies_path=strategies, labels_path=labels)

    _write_yaml(
        strategies,
        """
strategies:
  - id: override_drop
    enabled: true
    type: delta_threshold
    suggested_label: operator_override_change
    signal: Sovr
    threshold: -20
    window_seconds: 30
    description: Override drop.
""".strip()
        + "\n",
    )
    threshold_changed = intervention_strategy_config_signature(strategies_path=strategies, labels_path=labels)

    _write_yaml(
        strategies,
        """
strategies:
  - id: override_drop
    enabled: false
    type: delta_threshold
    suggested_label: operator_override_change
    signal: Sovr
    threshold: -20
    window_seconds: 30
    description: Override drop.
""".strip()
        + "\n",
    )
    disabled = intervention_strategy_config_signature(strategies_path=strategies, labels_path=labels)

    assert initial != threshold_changed
    assert threshold_changed != disabled


def test_unknown_labels_warn_by_default_and_can_be_rejected(tmp_path: Path) -> None:
    labels = _labels(tmp_path / "labels.yaml")
    strategies = _write_yaml(
        tmp_path / "strategies.yaml",
        """
strategies:
  - id: unknown_label_rule
    enabled: true
    type: value_change
    suggested_label: not_in_vocab
    signal: Tool_number
    window_seconds: 30
    description: Bad label.
""".strip()
        + "\n",
    )

    with pytest.warns(UserWarning, match="unknown suggested_label"):
        validated = validate_strategies(load_strategy_config(strategies), load_label_config(labels))
    assert validated[0]["suggested_label"] == "not_in_vocab"

    with pytest.raises(StrategyConfigError, match="unknown suggested_label"):
        validate_strategies(load_strategy_config(strategies), load_label_config(labels), warn_unknown_labels=False)


def test_malformed_strategy_configs_fail_safely(tmp_path: Path) -> None:
    labels = _labels(tmp_path / "labels.yaml")
    strategies = _write_yaml(
        tmp_path / "strategies.yaml",
        """
strategies:
  - id: broken
    enabled: true
    type: delta_threshold
    suggested_label: operator_override_change
    signal: Sovr
    window_seconds: 30
    description: Missing threshold.
""".strip()
        + "\n",
    )

    with pytest.raises(StrategyConfigError, match="numeric threshold"):
        run_intervention_strategies(_telemetry(), strategies_path=strategies, labels_path=labels)


def test_build_strategy_summary_includes_zero_count_enabled_strategy() -> None:
    strategies = [
        {"id": "empty", "suggested_label": "unknown"},
    ]
    summary = build_strategy_summary(pd.DataFrame(columns=CANDIDATE_EVENT_COLUMNS), strategies)

    assert summary.to_dict("records") == [
        {"strategy_id": "empty", "suggested_label": "unknown", "candidate_count": 0, "mean_score": 0.0}
    ]
