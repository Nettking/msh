"""Regression coverage for test-owned real Flask monitor/catalog lifetimes."""

from __future__ import annotations

import threading

import pytest

from catalog.common import artifact_refresh
from catalog.flask_app import app as app_module
from catalog.flask_app.conftest import owned_app_workers
from catalog.flask_app.services.recorder_artifact_refresh import (
    RECORDER_ARTIFACT_REFRESH_EXTENSION,
)


def _callback():
    with artifact_refresh._lock:
        return artifact_refresh._refresh_callback


def _start_monitor(app):
    monitor = app.extensions[RECORDER_ARTIFACT_REFRESH_EXTENSION]
    # A real request exercises the lazy startup hook, without supplying a
    # recorder checkpoint or disabling the monitor's ordinary polling.
    app.test_client().get("/static/ownership-regression-missing.txt")
    with monitor._lock:
        thread = monitor._thread
    assert thread is not None
    assert thread.is_alive()
    assert monitor.snapshot().running is True
    return monitor, thread


def test_nested_app_ownership_stops_two_monitors_and_preserves_outer_owner(
    tmp_path, monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    outer_app = app_module.create_app()
    outer_monitor, outer_thread = _start_monitor(outer_app)
    previous_callback = _callback()
    assert previous_callback is not None
    previous_install = app_module.install_recorder_artifact_refresh

    with owned_app_workers() as ownership:
        first = app_module.create_app()
        second = app_module.create_app()
        first_monitor, first_thread = _start_monitor(first)
        second_monitor, second_thread = _start_monitor(second)
        assert first_thread is not second_thread
        assert first_thread is not outer_thread
        assert second_thread is not outer_thread
        assert ownership.apps == [first, second]
        # These are the actual startup scan workers, retained even if a scan
        # completed before the first request. No thread census or GC heuristic.
        catalog_threads = tuple(ownership.catalog_threads)
        assert len(catalog_threads) >= 2
        assert all(thread.ident is not None for thread in catalog_threads)
        assert _callback() is not previous_callback

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert first_monitor.snapshot().status == "stopped"
    assert second_monitor.snapshot().status == "stopped"
    assert all(not thread.is_alive() for thread in catalog_threads)
    assert _callback() is previous_callback
    assert app_module.install_recorder_artifact_refresh is previous_install
    assert outer_thread.is_alive()
    assert outer_monitor.snapshot().running is True
    # The surrounding autouse owner is responsible for outer_app after this
    # test; the nested owner must not stop it early.


def test_partial_app_startup_failure_drains_real_workers_and_restores_callback(
    tmp_path, monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    previous_callback = _callback()
    stop_unrelated = threading.Event()
    unrelated = threading.Thread(
        target=stop_unrelated.wait,
        name="ownership-regression-unrelated",
        daemon=True,
    )
    unrelated.start()
    monitor_handles = []

    class StartupFailure(RuntimeError):
        pass

    def fail_after_catalog_startup(_supplier):
        # create_app has installed the monitor, registered its callback, and
        # launched the real startup catalog scan before reaching this seam.
        app = ownership.apps[-1]
        monitor = app.extensions[RECORDER_ARTIFACT_REFRESH_EXTENSION]
        assert monitor.start() is True
        with monitor._lock:
            thread = monitor._thread
        assert thread is not None and thread.is_alive()
        monitor_handles.append((monitor, thread))
        assert _callback() is not previous_callback
        raise StartupFailure("failure after real monitor installation")

    try:
        with (
            pytest.raises(StartupFailure, match="after real monitor installation"),
            owned_app_workers() as ownership,
        ):
            first = app_module.create_app()
            monitor_handles.append(_start_monitor(first))
            with monkeypatch.context() as fault:
                fault.setattr(app_module, "register_identity_supplier", fail_after_catalog_startup)
                app_module.create_app()
        assert len(ownership.apps) == 2
        assert len(monitor_handles) == 2
        assert all(not thread.is_alive() for _monitor, thread in monitor_handles)
        assert all(monitor.snapshot().status == "stopped" for monitor, _thread in monitor_handles)
        assert len(ownership.catalog_threads) >= 2
        assert all(not thread.is_alive() for thread in ownership.catalog_threads)
        assert _callback() is previous_callback
        assert unrelated.is_alive()
    finally:
        stop_unrelated.set()
        unrelated.join()
    assert not unrelated.is_alive()
