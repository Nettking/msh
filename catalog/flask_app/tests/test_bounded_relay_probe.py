from __future__ import annotations

import threading
import time

from catalog.flask_app.services.bounded_relay_probe import BoundedRelayListenerProbe


def test_relay_name_resolution_timeout_fails_closed_without_blocking_request() -> None:
    release = threading.Event()

    def blocked_resolver(*_args, **_kwargs):
        release.wait(timeout=1.0)
        return []

    probe = BoundedRelayListenerProbe(
        resolver=blocked_resolver,
        resolution_timeout=0.01,
        total_timeout=0.02,
    )

    started = time.monotonic()
    try:
        assert probe("relay", 8765) is False
        elapsed = time.monotonic() - started
        assert elapsed < 0.25

        # The first resolver is still occupied. At most one later lookup may be
        # queued; callers still fail closed on their own bounded deadline rather
        # than creating another resolver thread per request.
        started_again = time.monotonic()
        assert probe("relay", 8765) is False
        assert time.monotonic() - started_again < 0.25
    finally:
        release.set()
