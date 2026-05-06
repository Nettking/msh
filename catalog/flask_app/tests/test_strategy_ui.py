from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(".").resolve()))

from flask import Flask

from catalog.common.intervention_strategy_runner import (
    intervention_strategy_config_signature,
    load_strategy_config,
)
from catalog.flask_app.routes import web


class _FakeRuntime:
    def requires_startup_choice(self) -> bool:
        return False


class _FakeCatalog:
    pass


def _write_labels(path: Path) -> Path:
    path.write_text(
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
        encoding="utf-8",
    )
    return path


def _write_strategies(path: Path, *, threshold: int = -10) -> Path:
    path.write_text(
        f"""
strategies:
  - id: override_drop
    enabled: true
    type: delta_threshold
    suggested_label: operator_override_change
    signal: Sovr
    threshold: {threshold}
    window_seconds: 30
    description: Override drop.
  - id: tool_number_change
    enabled: false
    type: value_change
    suggested_label: tool_change
    signal: Tool_number
    window_seconds: 30
    description: Tool changed.
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return path


def _app(tmp_path: Path, monkeypatch) -> tuple[Flask, Path, Path]:
    labels_path = _write_labels(tmp_path / "intervention_labels.yaml")
    strategies_path = _write_strategies(tmp_path / "intervention_strategies.yaml")
    monkeypatch.setattr(
        "catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime()
    )
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    app.secret_key = "test"
    app.config["ARTIFACT_CATALOG"] = _FakeCatalog()
    app.config["INTERVENTION_LABELS_PATH"] = labels_path
    app.config["INTERVENTION_STRATEGIES_PATH"] = strategies_path
    app.register_blueprint(web)
    return app, strategies_path, labels_path


def _valid_form(
    *, threshold: str = "-12", label: str = "operator_override_change"
) -> dict[str, str]:
    return {
        "strategy_indices": "0,1,new",
        "strategies-0-enabled": "1",
        "strategies-0-id": "override_drop",
        "strategies-0-type": "delta_threshold",
        "strategies-0-suggested_label": label,
        "strategies-0-signal": "Sovr",
        "strategies-0-companion_signal": "",
        "strategies-0-threshold": threshold,
        "strategies-0-ratio_threshold": "",
        "strategies-0-window_seconds": "30",
        "strategies-0-description": "Updated override drop.",
        "strategies-1-id": "tool_number_change",
        "strategies-1-type": "value_change",
        "strategies-1-suggested_label": "tool_change",
        "strategies-1-signal": "Tool_number",
        "strategies-1-companion_signal": "",
        "strategies-1-threshold": "",
        "strategies-1-ratio_threshold": "",
        "strategies-1-window_seconds": "30",
        "strategies-1-description": "Tool changed.",
        "strategies-new-id": "",
        "strategies-new-type": "delta_threshold",
        "strategies-new-suggested_label": "",
        "strategies-new-signal": "",
        "strategies-new-companion_signal": "",
        "strategies-new-threshold": "",
        "strategies-new-ratio_threshold": "",
        "strategies-new-window_seconds": "30",
        "strategies-new-description": "",
    }


def test_strategies_page_loads_successfully(tmp_path: Path, monkeypatch) -> None:
    app, _, _ = _app(tmp_path, monkeypatch)

    response = app.test_client().get("/strategies")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "Intervention strategies" in body
    assert "override_drop" in body
    assert "operator_override_change" in body
    assert "Active strategy signature" in body


def test_valid_strategy_edits_save_to_yaml(tmp_path: Path, monkeypatch) -> None:
    app, strategies_path, _ = _app(tmp_path, monkeypatch)

    response = app.test_client().post(
        "/strategies/save", data=_valid_form(), follow_redirects=True
    )

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "Strategy config saved" in body
    saved = load_strategy_config(strategies_path)
    assert saved[0]["threshold"] == -12
    assert saved[0]["description"] == "Updated override drop."


def test_invalid_strategy_edits_do_not_save_and_show_validation_errors(
    tmp_path: Path, monkeypatch
) -> None:
    app, strategies_path, _ = _app(tmp_path, monkeypatch)
    before = strategies_path.read_text(encoding="utf-8")
    form = _valid_form(threshold="not-a-number")

    response = app.test_client().post("/strategies/save", data=form)

    assert response.status_code == 400
    body = response.get_data(as_text=True)
    assert "threshold is required and must be numeric" in body
    assert strategies_path.read_text(encoding="utf-8") == before


def test_duplicate_enabled_strategy_ids_are_rejected(
    tmp_path: Path, monkeypatch
) -> None:
    app, strategies_path, _ = _app(tmp_path, monkeypatch)
    before = strategies_path.read_text(encoding="utf-8")
    form = _valid_form()
    form["strategies-1-enabled"] = "1"
    form["strategies-1-id"] = "override_drop"

    response = app.test_client().post("/strategies/save", data=form)

    assert response.status_code == 400
    assert "Duplicate enabled strategy id: override_drop" in response.get_data(
        as_text=True
    )
    assert strategies_path.read_text(encoding="utf-8") == before


def test_unknown_labels_are_rejected(tmp_path: Path, monkeypatch) -> None:
    app, strategies_path, _ = _app(tmp_path, monkeypatch)
    before = strategies_path.read_text(encoding="utf-8")

    response = app.test_client().post(
        "/strategies/save", data=_valid_form(label="not_in_vocab")
    )

    assert response.status_code == 400
    assert "unknown suggested_label &#39;not_in_vocab&#39;" in response.get_data(
        as_text=True
    )
    assert strategies_path.read_text(encoding="utf-8") == before


def test_saving_threshold_change_changes_strategy_signature(
    tmp_path: Path, monkeypatch
) -> None:
    app, strategies_path, labels_path = _app(tmp_path, monkeypatch)
    initial = intervention_strategy_config_signature(
        strategies_path=strategies_path, labels_path=labels_path
    )

    response = app.test_client().post(
        "/strategies/save", data=_valid_form(threshold="-25"), follow_redirects=True
    )

    assert response.status_code == 200
    changed = intervention_strategy_config_signature(
        strategies_path=strategies_path, labels_path=labels_path
    )
    assert changed != initial
    assert changed in response.get_data(as_text=True)


def test_strategies_page_renders_single_form_and_collapsible_sections(
    tmp_path: Path, monkeypatch
) -> None:
    app, _, _ = _app(tmp_path, monkeypatch)

    response = app.test_client().get("/strategies")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert body.count('<form method="post"') == 1
    assert "<details" in body
    assert "Add new strategy" in body


def test_strategies_page_renders_each_strategy_zero_field_once(
    tmp_path: Path, monkeypatch
) -> None:
    app, _, _ = _app(tmp_path, monkeypatch)

    response = app.test_client().get("/strategies")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    for field_id in (
        "strategies-0-enabled",
        "strategies-0-id",
        "strategies-0-type",
        "strategies-0-signal",
        "strategies-0-companion_signal",
        "strategies-0-threshold",
        "strategies-0-ratio_threshold",
        "strategies-0-window_seconds",
        "strategies-0-suggested_label",
        "strategies-0-description",
    ):
        assert body.count(f'id="{field_id}"') == 1


def test_strategy_card_template_includes_fields_template_once() -> None:
    source = Path("catalog/flask_app/templates/strategies_card.html").read_text(
        encoding="utf-8"
    )

    assert source.count("{% include 'strategies_fields.html' %}") == 1
