"""Diagnostic-only error observation without changed calls, guards or returns."""
import json
import os
import pathlib
import secrets
import sys
import threading
import time
import traceback

import pytest

events = []


def record(stage, **details):
    events.append({"stage": stage, "monotonic": time.monotonic(),
                   "wall_time": time.time(), **details})


@pytest.fixture(autouse=True)
def observe_inspection(request, monkeypatch):
    if request.node.name != "test_three_device_federation_keeps_ai_compute_and_storage_authority_separate":
        return
    from flask import session
    from catalog.flask_app import capability_inspection_routes as routes
    from catalog.flask_app.capability_onboarding_routes import _CSRF_SESSION_KEY

    original_error = routes._safe_error_response
    original_validate = routes._validate_server_bound_request

    def error_response(code, status):
        record("inspection_response", code=code, status=status)
        return original_error(code, status)

    def validate(payload, **kwargs):
        supplied = payload.get("_csrf_token")
        expected = session.get(_CSRF_SESSION_KEY)
        typed = isinstance(supplied, str) and isinstance(expected, str)
        record("inspection_validation", supplied_present=isinstance(supplied, str),
               expected_present=isinstance(expected, str),
               equal=secrets.compare_digest(supplied, expected) if typed else False)
        try:
            return original_validate(payload, **kwargs)
        except Exception as error:
            record("inspection_validation_exception", exception=type(error).__name__,
                   code=getattr(error, "code", None))
            raise

    monkeypatch.setattr(routes, "_safe_error_response", error_response)
    monkeypatch.setattr(routes, "_validate_server_bound_request", validate)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    details = {"test": item.nodeid, "outcome": report.outcome,
               "duration": report.duration,
               "threads": [{"name": t.name, "daemon": t.daemon} for t in threading.enumerate()]}
    if call.excinfo and report.when == "call":
        details["exception_type"] = call.excinfo.typename
        for frame in call.excinfo.traceback:
            if frame.name == "test_skip_serializes_with_concurrent_benchmark_completion":
                local = frame.frame.f_locals
                details["run_errors"] = [{"type": type(e).__name__, "code": getattr(e, "code", None),
                                          "text": str(e)} for e in local.get("run_errors", [])]
                thread = local.get("run_thread")
                details["run_thread_alive"] = thread.is_alive() if thread else None
                running_frame = sys._current_frames().get(thread.ident) if thread else None
                if running_frame:
                    details["run_thread_stack"] = [{"file": f.filename, "function": f.name, "line": f.lineno}
                                                   for f in traceback.extract_stack(running_frame)]
    record("pytest_" + report.when, **details)


def pytest_sessionfinish(session, exitstatus):
    pathlib.Path(os.environ["PR475_CONTEXT_OBSERVER"]).write_text(
        json.dumps({"diagnostic_only": True, "exitstatus": int(exitstatus), "events": events}, indent=2)
        + "\n", encoding="utf-8")
