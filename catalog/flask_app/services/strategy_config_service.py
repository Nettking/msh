"""Thin Flask UI service for editing intervention strategy YAML."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from werkzeug.datastructures import MultiDict

from catalog.common.intervention_strategy_runner import (
    DEFAULT_LABELS_PATH,
    DEFAULT_STRATEGIES_PATH,
    SUPPORTED_STRATEGY_TYPES,
    StrategyConfigError,
    _dump_strategies_yaml,
    intervention_strategy_config_signature,
    load_label_config,
    load_strategy_config,
    strategy_config_signature_for_definitions,
    validate_strategies,
)

STRATEGY_FIELD_ORDER = [
    "id",
    "enabled",
    "type",
    "suggested_label",
    "signal",
    "companion_signal",
    "threshold",
    "ratio_threshold",
    "window_seconds",
    "description",
]


@dataclass(frozen=True)
class StrategyValidationResult:
    strategies: list[dict[str, Any]]
    errors: list[str]
    warnings: list[str]
    signature: str | None = None


@dataclass(frozen=True)
class StrategyConfigPage:
    strategies: list[dict[str, Any]]
    labels: dict[str, dict[str, str]]
    signature: str | None
    validation_errors: list[str]
    validation_warnings: list[str]
    summary: dict[str, Any]
    supported_types: list[str]
    strategies_path: Path
    labels_path: Path


class StrategyConfigService:
    """Load, validate, and save the intervention strategy config for Flask forms."""

    def __init__(
        self,
        *,
        strategies_path: str | Path | None = DEFAULT_STRATEGIES_PATH,
        labels_path: str | Path | None = DEFAULT_LABELS_PATH,
    ) -> None:
        self.strategies_path = Path(strategies_path or DEFAULT_STRATEGIES_PATH)
        self.labels_path = Path(labels_path or DEFAULT_LABELS_PATH)

    def page_model(self) -> StrategyConfigPage:
        errors: list[str] = []
        warnings: list[str] = []
        labels: dict[str, dict[str, str]] = {}
        strategies: list[dict[str, Any]] = []
        signature: str | None = None

        try:
            labels = load_label_config(self.labels_path)
        except StrategyConfigError as exc:
            errors.append(str(exc))

        try:
            strategies = self._normalize_for_form(
                load_strategy_config(self.strategies_path)
            )
        except StrategyConfigError as exc:
            errors.append(str(exc))

        if labels and strategies:
            validation = self.validate(strategies, labels=labels)
            errors.extend(validation.errors)
            warnings.extend(validation.warnings)
            signature = validation.signature
        elif labels and not strategies:
            signature = strategy_config_signature_for_definitions([])

        return StrategyConfigPage(
            strategies=strategies,
            labels=labels,
            signature=signature,
            validation_errors=errors,
            validation_warnings=warnings,
            summary=self.summary(strategies, signature),
            supported_types=sorted(SUPPORTED_STRATEGY_TYPES),
            strategies_path=self.strategies_path,
            labels_path=self.labels_path,
        )

    def parse_form(self, form: MultiDict[str, str]) -> list[dict[str, Any]]:
        raw_indices = form.get("strategy_indices", "")
        indices = [item for item in raw_indices.split(",") if item != ""]
        strategies: list[dict[str, Any]] = []
        for index in indices:
            prefix = f"strategies-{index}-"
            if form.get(prefix + "delete") == "1":
                continue
            raw = {
                "id": form.get(prefix + "id", "").strip(),
                "enabled": form.get(prefix + "enabled") == "1",
                "type": form.get(prefix + "type", "").strip(),
                "suggested_label": form.get(prefix + "suggested_label", "").strip(),
                "signal": form.get(prefix + "signal", "").strip(),
                "companion_signal": form.get(prefix + "companion_signal", "").strip(),
                "threshold": form.get(prefix + "threshold", "").strip(),
                "ratio_threshold": form.get(prefix + "ratio_threshold", "").strip(),
                "window_seconds": form.get(prefix + "window_seconds", "").strip(),
                "description": form.get(prefix + "description", "").strip(),
            }
            if (
                index == "new"
                and not raw["enabled"]
                and not any(
                    raw[field]
                    for field in (
                        "id",
                        "suggested_label",
                        "signal",
                        "companion_signal",
                        "threshold",
                        "ratio_threshold",
                        "description",
                    )
                )
            ):
                continue
            strategies.append(self._coerce_form_strategy(raw))
        return strategies

    def validate(
        self,
        strategies: list[dict[str, Any]],
        *,
        labels: dict[str, dict[str, str]] | None = None,
    ) -> StrategyValidationResult:
        labels = labels if labels is not None else load_label_config(self.labels_path)
        errors: list[str] = []
        warnings: list[str] = []
        enabled_ids: set[str] = set()

        for index, strategy in enumerate(strategies):
            label = self._strategy_label(strategy, index)
            strategy_id = str(strategy.get("id", "")).strip()
            strategy_type = str(strategy.get("type", "")).strip()

            if not strategy_id:
                errors.append(f"Strategy {index + 1}: id is required.")
            elif bool(strategy.get("enabled", False)):
                if strategy_id in enabled_ids:
                    errors.append(f"Duplicate enabled strategy id: {strategy_id}.")
                enabled_ids.add(strategy_id)

            if strategy_type not in SUPPORTED_STRATEGY_TYPES:
                errors.append(
                    f"{label}: unsupported type '{strategy_type or '(empty)'}'."
                )

            suggested_label = str(strategy.get("suggested_label", "")).strip()
            if not suggested_label:
                errors.append(f"{label}: suggested_label is required.")
            elif suggested_label not in labels:
                errors.append(f"{label}: unknown suggested_label '{suggested_label}'.")

            if not str(strategy.get("signal", "")).strip():
                errors.append(f"{label}: signal is required.")

            if strategy_type == "delta_threshold" and not isinstance(
                strategy.get("threshold"), (int, float)
            ):
                errors.append(
                    f"{label}: threshold is required and must be numeric for delta_threshold."
                )
            if strategy_type == "ratio_drop":
                ratio_threshold = strategy.get("ratio_threshold")
                if (
                    not isinstance(ratio_threshold, (int, float))
                    or not 0 <= ratio_threshold <= 1
                ):
                    errors.append(
                        f"{label}: ratio_threshold is required and must be numeric between 0 and 1 for ratio_drop."
                    )
                companion = strategy.get("companion_signal")
                if companion is not None and not str(companion).strip():
                    errors.append(
                        f"{label}: companion_signal must be non-empty when provided."
                    )

            if (
                not isinstance(strategy.get("window_seconds"), (int, float))
                or strategy.get("window_seconds", -1) < 0
            ):
                errors.append(
                    f"{label}: window_seconds is required, numeric, and non-negative."
                )

            if not str(strategy.get("description", "")).strip():
                errors.append(f"{label}: description is required.")

        validated: list[dict[str, Any]] = []
        signature: str | None = None
        if not errors:
            try:
                validated = validate_strategies(
                    strategies, labels, warn_unknown_labels=False
                )
                signature = strategy_config_signature_for_definitions(validated)
            except StrategyConfigError as exc:
                errors.append(str(exc))

        for strategy in strategies:
            strategy_type = strategy.get("type")
            if strategy_type != "ratio_drop" and strategy.get("companion_signal"):
                warnings.append(
                    f"{strategy.get('id', '(new)')}: companion_signal is ignored unless type is ratio_drop."
                )
            if (
                strategy_type != "delta_threshold"
                and strategy.get("threshold") is not None
            ):
                warnings.append(
                    f"{strategy.get('id', '(new)')}: threshold is ignored unless type is delta_threshold."
                )
            if (
                strategy_type != "ratio_drop"
                and strategy.get("ratio_threshold") is not None
            ):
                warnings.append(
                    f"{strategy.get('id', '(new)')}: ratio_threshold is ignored unless type is ratio_drop."
                )

        return StrategyValidationResult(
            strategies=strategies, errors=errors, warnings=warnings, signature=signature
        )

    def save(self, strategies: list[dict[str, Any]]) -> str:
        labels = load_label_config(self.labels_path)
        validation = self.validate(strategies, labels=labels)
        if validation.errors:
            raise StrategyConfigError("; ".join(validation.errors))
        text = _dump_strategies_yaml(
            [self._ordered_strategy(strategy) for strategy in strategies]
        )
        self.strategies_path.write_text(text, encoding="utf-8")
        return intervention_strategy_config_signature(
            strategies_path=self.strategies_path,
            labels_path=self.labels_path,
            warn_unknown_labels=False,
        )

    def _normalize_for_form(
        self, strategies: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        return [self._ordered_strategy(dict(strategy)) for strategy in strategies]

    def _ordered_strategy(self, strategy: dict[str, Any]) -> dict[str, Any]:
        ordered: dict[str, Any] = {}
        for key in STRATEGY_FIELD_ORDER:
            if key in strategy and strategy[key] is not None:
                ordered[key] = strategy[key]
        for key, value in strategy.items():
            if key not in ordered and value is not None:
                ordered[key] = value
        return ordered

    def summary(
        self, strategies: list[dict[str, Any]], signature: str | None
    ) -> dict[str, Any]:
        enabled = [
            strategy for strategy in strategies if bool(strategy.get("enabled", False))
        ]
        disabled = [
            strategy
            for strategy in strategies
            if not bool(strategy.get("enabled", False))
        ]
        return {
            "enabled_count": len(enabled),
            "disabled_count": len(disabled),
            "labels_in_use": sorted(
                {
                    str(strategy.get("suggested_label"))
                    for strategy in strategies
                    if strategy.get("suggested_label")
                }
            ),
            "signature": signature,
        }

    def _coerce_form_strategy(self, raw: dict[str, Any]) -> dict[str, Any]:
        strategy: dict[str, Any] = {
            "id": raw["id"],
            "enabled": bool(raw["enabled"]),
            "type": raw["type"],
            "suggested_label": raw["suggested_label"],
            "signal": raw["signal"],
        }
        if raw["companion_signal"]:
            strategy["companion_signal"] = raw["companion_signal"]
        if raw["threshold"]:
            strategy["threshold"] = self._coerce_number(raw["threshold"])
        if raw["ratio_threshold"]:
            strategy["ratio_threshold"] = self._coerce_number(raw["ratio_threshold"])
        if raw["window_seconds"]:
            strategy["window_seconds"] = self._coerce_number(raw["window_seconds"])
        strategy["description"] = raw["description"]
        return self._ordered_strategy(strategy)

    def _coerce_number(self, value: str) -> int | float | str:
        try:
            number = float(value)
        except ValueError:
            return value
        return int(number) if number.is_integer() else number

    def _strategy_label(self, strategy: dict[str, Any], index: int) -> str:
        strategy_id = str(strategy.get("id", "")).strip()
        return f"Strategy '{strategy_id}'" if strategy_id else f"Strategy {index + 1}"
