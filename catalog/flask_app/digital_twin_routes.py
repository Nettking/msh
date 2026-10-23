"""Authenticated, read-only digital twin and what-if page."""

from __future__ import annotations

import math

from flask import Blueprint, current_app, render_template, request

from .services.digital_twin_service import (
    DigitalTwinDataError,
    DigitalTwinInputError,
    get_digital_twin_service,
)

digital_twin_web = Blueprint("digital_twin_web", __name__)


def _empty_model() -> dict[str, object]:
    return {
        "machines": [],
        "machine_id": None,
        "machine_info": None,
        "start_date": "",
        "end_date": "",
        "source_file_count": 0,
        "record_count": 0,
        "has_data": False,
        "sufficient_data": False,
        "recovery_percent": 10.0,
        "active_seconds": 0.0,
        "non_active_seconds": 0.0,
        "observed_seconds": 0.0,
        "simulated_active_seconds": 0.0,
        "recovered_seconds": 0.0,
        "observed_active_share": None,
        "simulated_active_share": None,
        "active_share_change_pp": None,
        "positive_intervals": 0,
        "usable_intervals": 0,
        "classified_intervals": 0,
        "long_gaps": 0,
        "continuity": None,
        "state_breakdown": [],
    }


@digital_twin_web.get("/digital-twin")
def index():
    """Render observed state and a bounded recovery scenario."""

    service = get_digital_twin_service(current_app.config.get("FCP_DATA_ROOT", "data"))
    requested_recovery = request.args.get("recovery_percent", "10")
    error = None
    data_unavailable = False
    status_code = 200

    try:
        recovery_percent = float(requested_recovery)
        if not math.isfinite(recovery_percent):
            raise ValueError
        analysis = service.analyze(
            machine_id=request.args.get("machine_id"),
            start_date=request.args.get("start_date"),
            end_date=request.args.get("end_date"),
            recovery_percent=recovery_percent,
        )
    except DigitalTwinDataError as exc:
        error = str(exc)
        data_unavailable = True
        status_code = 503
        analysis = _empty_model()
    except (DigitalTwinInputError, TypeError, ValueError) as exc:
        error = (
            str(exc)
            if isinstance(exc, DigitalTwinInputError)
            else "Recovery must be a number from 0 to 50 percent."
        )
        status_code = 400
        try:
            analysis = service.analyze()
        except Exception:  # noqa: BLE001 - render a useful page if local data cannot be read
            analysis = _empty_model()

    return render_template(
        "digital_twin.html",
        analysis=analysis,
        error=error,
        data_unavailable=data_unavailable,
        requested_machine=request.args.get(
            "machine_id", analysis.get("machine_id") or ""
        ),
        requested_start=request.args.get(
            "start_date", analysis.get("start_date") or ""
        ),
        requested_end=request.args.get("end_date", analysis.get("end_date") or ""),
        requested_recovery=requested_recovery,
    ), status_code
