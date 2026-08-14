"""An unready recorder must tell the operator what to do next.

`start-tailscale-recorder.cmd` promises fail-closed sharing, so every refusal
reaches a person standing at a machine. Two of the refusal states are not
recorder faults: the logical-storage authority is a separate leader-side
process, so a normally started leader advertises none and a correct recorder
refuses to start. Without a remedy in the message that reads as a broken
recorder.
"""

from __future__ import annotations

import pytest

from catalog.federation.recorder_storage_relay import (
    STORAGE_CONTROL_CAPABILITY_PROTOCOL,
    STORAGE_CONTROL_CAPABILITY_TYPE,
)
from catalog.mtconnect_recorder.federation_node import (
    SHARING_STATE_REMEDIES,
    StorageAuthoritySelection,
    select_storage_authority,
    sharing_state_detail,
)

SESSION = "session-machine-4"
OWNER = "node-leader"


def _ready_authority(*group_ids: str) -> dict[str, object]:
    return {
        "session_id": SESSION,
        "node_id": OWNER,
        "type": STORAGE_CONTROL_CAPABILITY_TYPE,
        "protocol": STORAGE_CONTROL_CAPABILITY_PROTOCOL,
        "status": "ready",
        "properties": {"group_ids": list(group_ids)},
    }


def _status(*, capabilities: list[dict[str, object]]) -> dict[str, object]:
    return {
        "sessions": [{"session_id": SESSION, "created_by_node_id": OWNER}],
        "capabilities": capabilities,
    }


def test_a_leader_without_a_storage_authority_names_the_missing_process():
    selection = select_storage_authority(
        _status(capabilities=[]), session_id=SESSION, requested_group=None
    )

    assert selection.state == "authority-unavailable"

    detail = sharing_state_detail(selection.state)

    assert "FCP_FEDERATION_STORAGE_AUTHORITY_ENABLED" in detail
    assert "created the Federation" in detail


@pytest.mark.parametrize("state", sorted(SHARING_STATE_REMEDIES))
def test_every_known_unready_state_keeps_its_code_and_gains_a_remedy(state):
    detail = sharing_state_detail(state)

    assert detail.startswith(f"{state};")
    assert len(detail) > len(state) + 2


def test_the_group_choice_remedy_survived_the_move_off_the_inline_branch():
    detail = sharing_state_detail("storage-group-required")

    assert "--storage-group" in detail


def test_an_unknown_state_is_reported_verbatim_without_invented_advice():
    assert sharing_state_detail("some-future-state") == "some-future-state"
    assert sharing_state_detail(None) == "unknown"
    assert sharing_state_detail("") == "unknown"


def test_every_selection_state_the_selector_can_return_has_a_remedy():
    # A new refusal reason must not reach an operator bare.
    reachable = {
        select_storage_authority(
            {"sessions": None, "capabilities": None},
            session_id=SESSION,
            requested_group=None,
        ).state,
        select_storage_authority(
            {"sessions": [], "capabilities": []},
            session_id=SESSION,
            requested_group=None,
        ).state,
        select_storage_authority(
            _status(capabilities=[]), session_id=SESSION, requested_group=None
        ).state,
        select_storage_authority(
            _status(capabilities=[_ready_authority("alpha", "beta")]),
            session_id=SESSION,
            requested_group=None,
        ).state,
        select_storage_authority(
            _status(capabilities=[_ready_authority("alpha")]),
            session_id=SESSION,
            requested_group="beta",
        ).state,
    }

    unready = {state for state in reachable if state != "ready"}

    assert unready
    assert unready <= set(SHARING_STATE_REMEDIES)


def test_a_ready_authority_is_still_selected_unchanged():
    selection = select_storage_authority(
        _status(capabilities=[_ready_authority("fcp-local-storage")]),
        session_id=SESSION,
        requested_group="fcp-local-storage",
    )

    assert selection == StorageAuthoritySelection(
        OWNER, "fcp-local-storage", "ready"
    )
