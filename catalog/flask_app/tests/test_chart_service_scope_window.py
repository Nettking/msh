from pathlib import Path
import importlib.util

import pandas as pd


_MODULE_PATH = Path("catalog/flask_app/services/chart_service.py")
_SPEC = importlib.util.spec_from_file_location("chart_service", _MODULE_PATH)
assert _SPEC and _SPEC.loader
_MOD = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MOD)
line_or_scatter_data = _MOD.line_or_scatter_data


def test_line_window_includes_full_end_day_for_date_only_bounds() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": [
                "2026-03-10T10:00:00Z",
                "2026-03-11T11:00:00Z",
                "2026-03-12T13:30:00Z",
            ],
            "value": [1.0, 2.0, 3.0],
        }
    )

    payload = line_or_scatter_data(
        frame,
        ["value"],
        mode="line",
        window_start="2026-03-10",
        window_end="2026-03-12",
        aggregation="raw",
    )

    values = payload["datasets"][0]["data"]
    assert len(values) == 3
    assert values[-1] == 3.0
    assert bool(payload["x_is_time"]) is True


def test_minute_aggregation_buckets_by_minute_without_deprecated_alias() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": [
                "2026-03-12T13:30:01Z",
                "2026-03-12T13:30:45Z",
                "2026-03-12T13:31:10Z",
            ],
            "value": [1.0, 3.0, 5.0],
        }
    )

    payload = line_or_scatter_data(
        frame,
        ["value"],
        mode="line",
        aggregation="minute",
    )

    assert payload["labels"] == ["2026-03-12 13:30:00", "2026-03-12 13:31:00"]
    assert payload["datasets"][0]["data"] == [2.0, 5.0]
