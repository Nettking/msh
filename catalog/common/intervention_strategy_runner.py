"""Config-driven intervention candidate detection strategies.

Strategies suggest candidate events for validation review. They do not assign
truth labels; review fields are intentionally present but empty by default.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
import hashlib
import json
import warnings

import pandas as pd

DEFAULT_LABELS_PATH = Path(__file__).with_name("intervention_labels.yaml")
DEFAULT_STRATEGIES_PATH = Path(__file__).with_name("intervention_strategies.yaml")
SUPPORTED_STRATEGY_TYPES = {"delta_threshold", "ratio_drop", "value_change"}
CANDIDATE_EVENT_COLUMNS = [
    "timestamp",
    "machine_id",
    "strategy_id",
    "strategy_type",
    "suggested_label",
    "event_score",
    "fired_rule",
    "evidence",
    "window_start",
    "window_end",
    "review_status",
    "human_label",
    "notes",
]
SUMMARY_COLUMNS = ["strategy_id", "suggested_label", "candidate_count", "mean_score"]


def _coerce_yaml_scalar(value: str) -> Any:
    text = value.strip()
    if text == "":
        return ""
    lowered = text.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered in {"null", "none", "~"}:
        return None
    if (text.startswith('"') and text.endswith('"')) or (text.startswith("'") and text.endswith("'")):
        return text[1:-1]
    try:
        if "." in text:
            return float(text)
        return int(text)
    except ValueError:
        return text


def _parse_simple_yaml(text: str) -> dict[str, Any]:
    """Parse the small YAML subset used by intervention config files."""
    lines = [line.rstrip() for line in text.splitlines() if line.strip() and not line.lstrip().startswith("#")]
    if not lines:
        return {}

    root_index = 0
    root_key = lines[root_index].strip().rstrip(":")
    if root_key not in {"strategies", "labels"}:
        for index, line in enumerate(lines):
            stripped = line.strip()
            if not line.startswith(" ") and stripped in {"strategies:", "labels:"}:
                root_index = index
                root_key = stripped.rstrip(":")
                break
    if root_key == "strategies":
        strategies: list[dict[str, Any]] = []
        current: dict[str, Any] | None = None
        for line in lines[root_index + 1:]:
            stripped = line.strip()
            if stripped.startswith("- "):
                if current is not None:
                    strategies.append(current)
                current = {}
                remainder = stripped[2:].strip()
                if remainder:
                    key, value = remainder.split(":", 1)
                    current[key.strip()] = _coerce_yaml_scalar(value)
            elif current is not None and ":" in stripped:
                key, value = stripped.split(":", 1)
                current[key.strip()] = _coerce_yaml_scalar(value)
        if current is not None:
            strategies.append(current)
        return {"strategies": strategies}
    if root_key == "labels":
        labels: dict[str, dict[str, Any]] = {}
        current_label: str | None = None
        for line in lines[root_index + 1:]:
            indent = len(line) - len(line.lstrip(" "))
            stripped = line.strip()
            if indent == 2 and stripped.endswith(":"):
                current_label = stripped[:-1]
                labels[current_label] = {}
            elif indent >= 4 and current_label is not None and ":" in stripped:
                key, value = stripped.split(":", 1)
                labels[current_label][key.strip()] = _coerce_yaml_scalar(value)
        return {"labels": labels}
    raise StrategyConfigError(f"unsupported YAML root: {root_key}")


def _quote_yaml_string(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace('"', '\\"')
    return f'"{text}"'


def _dump_strategies_yaml(strategies: list[dict[str, Any]]) -> str:
    lines = ["strategies:"]
    for strategy in strategies:
        first = True
        for key, value in strategy.items():
            prefix = "  -" if first else "   "
            lines.append(f"{prefix} {key}: {_quote_yaml_string(value)}")
            first = False
    return "\n".join(lines) + "\n"


class StrategyConfigError(ValueError):
    """Raised when an intervention strategy config is unsafe to run."""


def _load_yaml_mapping(path: str | Path) -> dict[str, Any]:
    source = Path(path)
    data = _parse_simple_yaml(source.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise StrategyConfigError(f"YAML root must be a mapping: {source}")
    return data


def load_label_config(path: str | Path = DEFAULT_LABELS_PATH) -> dict[str, dict[str, str]]:
    """Load and validate the intervention label vocabulary."""
    data = _load_yaml_mapping(path)
    labels = data.get("labels")
    if not isinstance(labels, dict) or not labels:
        raise StrategyConfigError("label config must contain a non-empty 'labels' mapping")

    normalized: dict[str, dict[str, str]] = {}
    for label_id, payload in labels.items():
        if not isinstance(label_id, str) or not label_id.strip():
            raise StrategyConfigError("label ids must be non-empty strings")
        if not isinstance(payload, dict):
            raise StrategyConfigError(f"label '{label_id}' must be a mapping")
        description = payload.get("description")
        if not isinstance(description, str) or not description.strip():
            raise StrategyConfigError(f"label '{label_id}' must include a description")
        normalized[label_id] = {"description": description.strip()}
    return normalized


def load_strategy_config(path: str | Path = DEFAULT_STRATEGIES_PATH) -> list[dict[str, Any]]:
    """Load raw strategy definitions from YAML."""
    data = _load_yaml_mapping(path)
    strategies = data.get("strategies")
    if not isinstance(strategies, list):
        raise StrategyConfigError("strategy config must contain a 'strategies' list")
    return strategies


def validate_strategies(
    strategies: list[dict[str, Any]],
    labels: dict[str, dict[str, str]],
    *,
    warn_unknown_labels: bool = True,
) -> list[dict[str, Any]]:
    """Validate enabled strategies conservatively and return runnable entries."""
    validated: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for index, strategy in enumerate(strategies):
        if not isinstance(strategy, dict):
            raise StrategyConfigError(f"strategy at index {index} must be a mapping")
        if not bool(strategy.get("enabled", False)):
            continue

        strategy_id = strategy.get("id")
        if not isinstance(strategy_id, str) or not strategy_id.strip():
            raise StrategyConfigError(f"enabled strategy at index {index} must include a non-empty id")
        if strategy_id in seen_ids:
            raise StrategyConfigError(f"duplicate enabled strategy id: {strategy_id}")
        seen_ids.add(strategy_id)

        strategy_type = strategy.get("type")
        if strategy_type not in SUPPORTED_STRATEGY_TYPES:
            raise StrategyConfigError(f"strategy '{strategy_id}' has unsupported type: {strategy_type}")

        label = strategy.get("suggested_label")
        if not isinstance(label, str) or not label.strip():
            raise StrategyConfigError(f"strategy '{strategy_id}' must include suggested_label")
        if label not in labels:
            message = f"strategy '{strategy_id}' references unknown suggested_label: {label}"
            if warn_unknown_labels:
                warnings.warn(message, UserWarning, stacklevel=2)
            else:
                raise StrategyConfigError(message)

        signal = strategy.get("signal")
        if not isinstance(signal, str) or not signal.strip():
            raise StrategyConfigError(f"strategy '{strategy_id}' must include a signal")

        window_seconds = strategy.get("window_seconds")
        if not isinstance(window_seconds, (int, float)) or window_seconds < 0:
            raise StrategyConfigError(f"strategy '{strategy_id}' must include non-negative window_seconds")

        if strategy_type == "delta_threshold":
            threshold = strategy.get("threshold")
            if not isinstance(threshold, (int, float)):
                raise StrategyConfigError(f"strategy '{strategy_id}' must include numeric threshold")
        elif strategy_type == "ratio_drop":
            ratio_threshold = strategy.get("ratio_threshold")
            if not isinstance(ratio_threshold, (int, float)) or not 0 <= ratio_threshold <= 1:
                raise StrategyConfigError(f"strategy '{strategy_id}' must include ratio_threshold between 0 and 1")
            companion = strategy.get("companion_signal")
            if companion is not None and (not isinstance(companion, str) or not companion.strip()):
                raise StrategyConfigError(f"strategy '{strategy_id}' companion_signal must be a non-empty string")

        description = strategy.get("description")
        if not isinstance(description, str) or not description.strip():
            raise StrategyConfigError(f"strategy '{strategy_id}' must include description")

        validated.append(dict(strategy))
    return validated


def _empty_candidates() -> pd.DataFrame:
    return pd.DataFrame(columns=CANDIDATE_EVENT_COLUMNS)


def _machine_series(df: pd.DataFrame, machine_col: str) -> pd.Series:
    if machine_col in df.columns:
        return df[machine_col].astype("string").fillna("unknown")
    return pd.Series(["unknown"] * len(df), index=df.index, dtype="string")


def _window_start(timestamps: pd.Series, previous_timestamps: pd.Series, window_seconds: float) -> pd.Series:
    fallback = timestamps - pd.to_timedelta(window_seconds, unit="s")
    return previous_timestamps.where(previous_timestamps.notna(), fallback)


def _event_rows(
    df: pd.DataFrame,
    mask: pd.Series,
    strategy: dict[str, Any],
    scores: pd.Series,
    evidence_fields: dict[str, pd.Series],
    *,
    time_col: str,
    machine_col: str,
    previous_timestamps: pd.Series,
) -> pd.DataFrame:
    if not bool(mask.any()):
        return _empty_candidates()

    selected = df.loc[mask].copy()
    timestamps = pd.to_datetime(selected[time_col], errors="coerce")
    machine_ids = _machine_series(selected, machine_col)
    window_seconds = float(strategy["window_seconds"])
    starts = _window_start(timestamps, previous_timestamps.loc[mask], window_seconds)

    evidence_rows: list[str] = []
    for idx in selected.index:
        evidence = {key: series.loc[idx] for key, series in evidence_fields.items()}
        evidence["description"] = strategy["description"]
        evidence_rows.append(json.dumps(evidence, default=str, sort_keys=True))

    out = pd.DataFrame(
        {
            "timestamp": timestamps,
            "machine_id": machine_ids.to_numpy(),
            "strategy_id": strategy["id"],
            "strategy_type": strategy["type"],
            "suggested_label": strategy["suggested_label"],
            "event_score": scores.loc[mask].astype(float).to_numpy(),
            "fired_rule": strategy["id"],
            "evidence": evidence_rows,
            "window_start": starts,
            "window_end": timestamps,
            "review_status": "unreviewed",
            "human_label": "",
            "notes": "",
        }
    )
    return out[CANDIDATE_EVENT_COLUMNS]


def _run_delta_threshold(df: pd.DataFrame, strategy: dict[str, Any], *, time_col: str, machine_col: str) -> pd.DataFrame:
    signal = strategy["signal"]
    if signal not in df.columns:
        return _empty_candidates()
    values = pd.to_numeric(df[signal], errors="coerce")
    previous = values.shift(1)
    delta = values - previous
    threshold = float(strategy["threshold"])
    mask = delta <= threshold if threshold < 0 else delta >= threshold
    mask = mask.fillna(False)
    return _event_rows(
        df,
        mask,
        strategy,
        delta.abs(),
        {"signal": pd.Series([signal] * len(df), index=df.index), "previous_value": previous, "current_value": values, "delta": delta},
        time_col=time_col,
        machine_col=machine_col,
        previous_timestamps=df[time_col].shift(1),
    )


def _run_ratio_drop(df: pd.DataFrame, strategy: dict[str, Any], *, time_col: str, machine_col: str) -> pd.DataFrame:
    signal = strategy["signal"]
    if signal not in df.columns:
        return _empty_candidates()
    values = pd.to_numeric(df[signal], errors="coerce")
    previous = values.shift(1)
    ratio = values / previous
    ratio = ratio.where(previous > 0)
    mask = (ratio <= float(strategy["ratio_threshold"])).fillna(False)
    evidence = {
        "signal": pd.Series([signal] * len(df), index=df.index),
        "previous_value": previous,
        "current_value": values,
        "ratio": ratio,
    }
    companion = strategy.get("companion_signal")
    if companion and companion in df.columns:
        evidence["companion_signal"] = pd.Series([companion] * len(df), index=df.index)
        evidence["companion_value"] = df[companion]
    return _event_rows(
        df,
        mask,
        strategy,
        1 - ratio,
        evidence,
        time_col=time_col,
        machine_col=machine_col,
        previous_timestamps=df[time_col].shift(1),
    )


def _run_value_change(df: pd.DataFrame, strategy: dict[str, Any], *, time_col: str, machine_col: str) -> pd.DataFrame:
    signal = strategy["signal"]
    if signal not in df.columns:
        return _empty_candidates()
    values = df[signal]
    previous = values.shift(1)
    mask = values.notna() & previous.notna() & (values.astype("string") != previous.astype("string"))
    return _event_rows(
        df,
        mask,
        strategy,
        pd.Series([1.0] * len(df), index=df.index),
        {"signal": pd.Series([signal] * len(df), index=df.index), "previous_value": previous, "current_value": values},
        time_col=time_col,
        machine_col=machine_col,
        previous_timestamps=df[time_col].shift(1),
    )


def strategy_config_signature_for_definitions(strategies: list[dict[str, Any]]) -> str:
    """Return a stable signature for the active, validated strategy definitions."""
    payload = {"signature_version": 1, "strategies": strategies}
    canonical = json.dumps(payload, default=str, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def intervention_strategy_config_signature(
    *,
    strategies_path: str | Path = DEFAULT_STRATEGIES_PATH,
    labels_path: str | Path = DEFAULT_LABELS_PATH,
    warn_unknown_labels: bool = True,
) -> str:
    """Load, validate, and hash the currently active intervention strategy config."""
    labels = load_label_config(labels_path)
    strategies = validate_strategies(load_strategy_config(strategies_path), labels, warn_unknown_labels=warn_unknown_labels)
    return strategy_config_signature_for_definitions(strategies)


def run_intervention_strategies(
    df: pd.DataFrame,
    *,
    strategies_path: str | Path = DEFAULT_STRATEGIES_PATH,
    labels_path: str | Path = DEFAULT_LABELS_PATH,
    time_col: str = "timestamp",
    machine_col: str = "machine_id",
    warn_unknown_labels: bool = True,
) -> pd.DataFrame:
    """Run enabled intervention strategies and return combined candidate events."""
    labels = load_label_config(labels_path)
    strategies = validate_strategies(load_strategy_config(strategies_path), labels, warn_unknown_labels=warn_unknown_labels)
    return run_strategy_definitions(df, strategies, time_col=time_col, machine_col=machine_col)


def run_strategy_definitions(
    df: pd.DataFrame,
    strategies: list[dict[str, Any]],
    *,
    time_col: str = "timestamp",
    machine_col: str = "machine_id",
) -> pd.DataFrame:
    """Run already-validated strategy definitions."""
    if df.empty or time_col not in df.columns:
        return _empty_candidates()
    working = df.copy()
    working[time_col] = pd.to_datetime(working[time_col], errors="coerce")
    working = working[working[time_col].notna()].sort_values([machine_col, time_col] if machine_col in working.columns else [time_col])
    if working.empty:
        return _empty_candidates()

    frames: list[pd.DataFrame] = []
    group_keys = [machine_col] if machine_col in working.columns else [lambda _: "unknown"]
    for _, group in working.groupby(group_keys, sort=True, dropna=False):
        for strategy in strategies:
            if strategy["type"] == "delta_threshold":
                frames.append(_run_delta_threshold(group, strategy, time_col=time_col, machine_col=machine_col))
            elif strategy["type"] == "ratio_drop":
                frames.append(_run_ratio_drop(group, strategy, time_col=time_col, machine_col=machine_col))
            elif strategy["type"] == "value_change":
                frames.append(_run_value_change(group, strategy, time_col=time_col, machine_col=machine_col))

    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return _empty_candidates()
    candidates = pd.concat(non_empty, ignore_index=True)
    return candidates.sort_values(["timestamp", "machine_id", "strategy_id"]).reset_index(drop=True)[CANDIDATE_EVENT_COLUMNS]


def build_strategy_summary(candidates: pd.DataFrame, strategies: list[dict[str, Any]]) -> pd.DataFrame:
    """Summarize candidate counts and mean scores for each enabled strategy."""
    rows = []
    for strategy in strategies:
        subset = candidates[candidates["strategy_id"] == strategy["id"]] if not candidates.empty else pd.DataFrame()
        rows.append(
            {
                "strategy_id": strategy["id"],
                "suggested_label": strategy["suggested_label"],
                "candidate_count": int(len(subset)),
                "mean_score": float(subset["event_score"].mean()) if not subset.empty else 0.0,
            }
        )
    return pd.DataFrame(rows, columns=SUMMARY_COLUMNS)


def write_strategy_outputs(
    df: pd.DataFrame,
    output_dir: str | Path,
    *,
    strategies_path: str | Path = DEFAULT_STRATEGIES_PATH,
    labels_path: str | Path = DEFAULT_LABELS_PATH,
    time_col: str = "timestamp",
    machine_col: str = "machine_id",
) -> dict[str, Path]:
    """Run strategies and write candidate, summary, and used-config outputs."""
    labels = load_label_config(labels_path)
    strategies = validate_strategies(load_strategy_config(strategies_path), labels)
    strategy_signature = strategy_config_signature_for_definitions(strategies)
    candidates = run_strategy_definitions(df, strategies, time_col=time_col, machine_col=machine_col)
    summary = build_strategy_summary(candidates, strategies)

    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    candidate_path = target_dir / "candidate_events.csv"
    summary_path = target_dir / "strategy_summary.csv"
    used_path = target_dir / "strategies_used.yaml"
    candidates.to_csv(candidate_path, index=False)
    summary.to_csv(summary_path, index=False)
    used_path.write_text(
        f"strategy_config_signature: {strategy_signature}\n" + _dump_strategies_yaml(strategies),
        encoding="utf-8",
    )
    return {"candidate_events": candidate_path, "strategy_summary": summary_path, "strategies_used": used_path}
