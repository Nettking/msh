from __future__ import annotations

from types import SimpleNamespace

import pytest

from catalog.federation.authoritative_replay import (
    AUTHORITATIVE_REPLAY_INCOMPLETE,
    AuthoritativeReplayIncomplete,
    replay_authoritative_history,
)
from catalog.federation.errors import FederationOperationError, RevisionGapError
from catalog.flask_app.services import federation_software_version_service as version_module
from catalog.flask_app.services.federation_software_version_service import (
    FederationSoftwareVersionService,
)


def _discard(_events: tuple[object, ...]) -> None:
    return None


def test_local_revision_gap_is_normalized_to_incomplete_authoritative_replay() -> None:
    def read_page(_last_revision: int):
        raise RevisionGapError("replay-window-too-large", "history is incomplete")

    with pytest.raises(AuthoritativeReplayIncomplete) as failure:
        replay_authoritative_history(read_page, apply_page=_discard, max_pages=1)

    assert failure.value.code == AUTHORITATIVE_REPLAY_INCOMPLETE
    assert isinstance(failure.value.__cause__, RevisionGapError)


def test_remote_revision_gap_is_normalized_to_incomplete_authoritative_replay() -> None:
    remote_gap = FederationOperationError("revision-gap", "remote history is incomplete")

    def read_page(_last_revision: int):
        raise remote_gap

    with pytest.raises(AuthoritativeReplayIncomplete) as failure:
        replay_authoritative_history(read_page, apply_page=_discard, max_pages=1)

    assert failure.value.code == AUTHORITATIVE_REPLAY_INCOMPLETE
    assert failure.value.__cause__ is remote_gap


def test_non_gap_federation_failure_keeps_its_original_semantics() -> None:
    unavailable = FederationOperationError(
        "pairing-relay-disconnected", "the paired relay is not connected"
    )

    def read_page(_last_revision: int):
        raise unavailable

    with pytest.raises(FederationOperationError) as failure:
        replay_authoritative_history(read_page, apply_page=_discard, max_pages=1)

    assert failure.value is unavailable


def test_software_version_snapshot_propagates_incomplete_authority_lookup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    service = FederationSoftwareVersionService(SimpleNamespace(), tmp_path / "version.json")

    def incomplete_context():
        raise AuthoritativeReplayIncomplete("leader history is incomplete")

    monkeypatch.setattr(service, "_context", incomplete_context)

    with pytest.raises(AuthoritativeReplayIncomplete) as failure:
        service.snapshot()

    assert failure.value.code == AUTHORITATIVE_REPLAY_INCOMPLETE


def test_software_version_update_rows_propagate_incomplete_update_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def incomplete_snapshot():
        raise AuthoritativeReplayIncomplete("update history is incomplete")

    monkeypatch.setattr(
        version_module,
        "get_active_update_service",
        lambda: SimpleNamespace(snapshot=incomplete_snapshot),
    )

    with pytest.raises(AuthoritativeReplayIncomplete) as failure:
        FederationSoftwareVersionService._update_rows()

    assert failure.value.code == AUTHORITATIVE_REPLAY_INCOMPLETE
