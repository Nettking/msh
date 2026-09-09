"""Own the real background workers created by Flask application tests."""

from __future__ import annotations

import builtins
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field

import pytest


@dataclass
class AppWorkerOwnership:
    """Keep exact handles available after cleanup, including partial startup."""

    apps: list = field(default_factory=list)
    catalog_threads: list[threading.Thread] = field(default_factory=list)


@contextmanager
def owned_app_workers():
    """Finish this scope's real app workers and restore its prior callback.

    Imports happen at fixture setup, not conftest collection. Nested scopes
    restore the immediate outer scope's patches and callback; they never stop
    a monitor or catalog worker that existed before entering the inner scope.
    """
    from catalog.common import artifact_refresh
    from catalog.flask_app import app as app_module
    from catalog.flask_app.services import catalog_service
    from catalog.flask_app.services.recorder_artifact_refresh import (
        RECORDER_ARTIFACT_REFRESH_EXTENSION,
    )

    original_catalog = app_module.ArtifactCatalog
    original_install = app_module.install_recorder_artifact_refresh
    original_threading = catalog_service.threading
    ownership = AppWorkerOwnership()
    owned_catalogs = set()
    ownership_lock = threading.Lock()
    with artifact_refresh._lock:
        previous_refresh = artifact_refresh._refresh_callback

    def make_catalog(*args, **kwargs):
        catalog = original_catalog(*args, **kwargs)
        with ownership_lock:
            owned_catalogs.add(catalog)
        return catalog

    def install_monitor(app, *args, **kwargs):
        # Retain the app before installation or any later factory step can fail.
        # The real installer records its monitor in app.extensions.
        ownership.apps.append(app)
        return original_install(app, *args, **kwargs)

    class CatalogThreading:
        """Observe this module's owned catalog workers, never global Thread."""

        def __getattr__(self, name):
            return getattr(original_threading, name)

        def Thread(self, *args, **kwargs):
            thread = original_threading.Thread(*args, **kwargs)
            target = kwargs.get("target", args[1] if len(args) > 1 else None)
            code = getattr(target, "__code__", None)
            closure = getattr(target, "__closure__", None)
            if code is not None and closure is not None and "self" in code.co_freevars:
                owner = closure[code.co_freevars.index("self")].cell_contents
                with ownership_lock:
                    if owner in owned_catalogs:
                        ownership.catalog_threads.append(thread)
            return thread

    with pytest.MonkeyPatch.context() as patches:
        patches.setattr(app_module, "ArtifactCatalog", make_catalog)
        patches.setattr(app_module, "install_recorder_artifact_refresh", install_monitor)
        patches.setattr(catalog_service, "threading", CatalogThreading())
        try:
            yield ownership
        finally:
            failures = []
            monitor_threads = []

            def finish(operation, *args):
                try:
                    operation(*args)
                except BaseException as error:  # noqa: BLE001 - finish all owned cleanup before reporting
                    failures.append(error)

            for app in ownership.apps:
                monitor = app.extensions.get(RECORDER_ARTIFACT_REFRESH_EXTENSION)
                if monitor is None:
                    continue
                with monitor._lock:
                    thread = monitor._thread
                if thread is not None:
                    monitor_threads.append(thread)
                finish(monitor.stop)

            def restore_refresh():
                with artifact_refresh._lock:
                    artifact_refresh._refresh_callback = previous_refresh

            finish(restore_refresh)
            # stop() truthfully reports "stopping" if its bounded join expires.
            # Drain those exact handles before any cwd/environment restoration.
            for thread in monitor_threads:
                if thread.ident is not None:
                    finish(thread.join)
            # Only after monitor completion can the owned catalog worker set no
            # longer grow from a last recorder checkpoint notification.
            with ownership_lock:
                catalog_threads = tuple(ownership.catalog_threads)
            for thread in catalog_threads:
                if thread.ident is not None:
                    finish(thread.join)
            live_workers = [
                thread.name for thread in (*monitor_threads, *catalog_threads)
                if thread.is_alive()
            ]
            if live_workers:
                failures.append(AssertionError(f"owned app workers survived cleanup: {live_workers}"))
            if failures:
                raise builtins.BaseExceptionGroup("Flask app cleanup failed", failures)


@pytest.fixture(autouse=True)
def owned_flask_apps(monkeypatch):
    """Run before callers and finish before monkeypatch restores test state."""
    with owned_app_workers() as ownership:
        yield ownership
