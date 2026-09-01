from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from catalog.federation.authoritative_replay import AuthoritativeReplayIncomplete
from catalog.flask_app.services import federation_capability_requests as capability_module
from catalog.flask_app.services import federation_recorder_control_service as recorder_module
from catalog.flask_app.services import federation_software_version_service as version_module
from catalog.flask_app.services import federation_update_service as update_module
from catalog.flask_app.services.federation_capability_requests import (
    FederationCapabilityRequestService,
)
from catalog.flask_app.services.federation_recorder_control_service import (
    FederationRecorderControlService,
)
from catalog.flask_app.services.federation_software_version_service import (
    FederationSoftwareVersionService,
)
from catalog.flask_app.services.federation_update_events import CHECK_REPORT_EVENT
from catalog.flask_app.services.federation_update_service import FederationUpdateService


class _PagedCoordinator:
    def __init__(self, count: int) -> None:
        self.events = tuple(
            SimpleNamespace(
                revision=revision,
                event_type="capability.activity.recorded",
                actor_node_id="node-remote",
                payload={"revision": revision},
            )
            for revision in range(1, count + 1)
        )

    def replay_page(
        self,
        *,
        last_applied_revision: int,
        limit: int,
        **_kwargs: Any,
    ) -> tuple[tuple[Any, ...], int]:
        remaining = tuple(
            event
            for event in self.events
            if event.revision > last_applied_revision
        )
        return remaining[:limit], len(self.events)


def _context(count: int) -> Any:
    return SimpleNamespace(
        coordinator=_PagedCoordinator(count),
        binding=SimpleNamespace(internal_session_id="session-b09"),
        credentials=SimpleNamespace(identity=SimpleNamespace(node_id="node-leader")),
    )


class _UpdateLocal:
    apply_calls = 0

    def inspect(self, *, target=None, fetch=True):  # pragma: no cover - not used
        raise AssertionError("unexpected inspect")

    def apply(self, target, *, request_id=None):  # pragma: no cover - not used
        self.apply_calls += 1
        raise AssertionError("incomplete replay must refuse before host apply")

    def latest_result(self):
        return None


class _VersionLocal:
    def approved_branches(self):
        return ()


def test_update_reports_do_not_return_a_prefix_at_the_existing_page_ceiling(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(update_module, "_REPORT_REPLAY_PAGE_EVENTS", 1)
    context = _context(update_module._MAX_REPORT_REPLAY_PAGES + 1)
    service = FederationUpdateService(_UpdateLocal(), tmp_path / "update.json")

    with pytest.raises(AuthoritativeReplayIncomplete):
        service._reports(
            context,
            "node-leader",
            event_type=CHECK_REPORT_EVENT,
            request_id="check-one",
            target="1" * 40,
        )


def test_software_version_reports_do_not_return_a_prefix_at_the_existing_page_ceiling(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(version_module, "_REPORT_REPLAY_PAGE_EVENTS", 1)
    context = _context(version_module._MAX_REPORT_REPLAY_PAGES + 1)
    service = FederationSoftwareVersionService(
        _VersionLocal(), tmp_path / "software-version.json"
    )

    with pytest.raises(AuthoritativeReplayIncomplete):
        service._reports(context, "node-leader", request_id="trial-one")


def test_capability_reports_do_not_return_a_prefix_at_the_existing_page_ceiling(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(capability_module, "_REPORT_REPLAY_PAGE_EVENTS", 1)
    context = _context(capability_module._MAX_REPORT_REPLAY_PAGES + 1)
    service = FederationCapabilityRequestService(tmp_path / "capability.json")

    with pytest.raises(AuthoritativeReplayIncomplete):
        service._reports(context, "node-leader", request_id="capability-one")


def test_recorder_control_reports_do_not_return_a_prefix_at_the_existing_page_ceiling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(recorder_module, "_REPORT_REPLAY_PAGE_EVENTS", 1)
    context = _context(recorder_module._MAX_REPORT_REPLAY_PAGES + 1)

    with pytest.raises(AuthoritativeReplayIncomplete):
        FederationRecorderControlService._events(context, "node-leader")


@pytest.mark.parametrize("kind", ["update", "version", "capability"])
def test_passive_status_does_not_hide_authoritative_replay_incomplete(
    kind: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    failure = AuthoritativeReplayIncomplete("bounded report replay stopped early")
    context = _context(0)

    if kind == "update":
        service = FederationUpdateService(_UpdateLocal(), tmp_path / "update.json")
    elif kind == "version":
        service = FederationSoftwareVersionService(
            _VersionLocal(), tmp_path / "software-version.json"
        )
    else:
        service = FederationCapabilityRequestService(tmp_path / "capability.json")

    monkeypatch.setattr(service, "_context", lambda: (context, "node-leader"))

    def fail_refresh(*_args: Any, **_kwargs: Any) -> Any:
        raise failure

    monkeypatch.setattr(service, "_refresh", fail_refresh)

    with pytest.raises(AuthoritativeReplayIncomplete):
        service.snapshot()
