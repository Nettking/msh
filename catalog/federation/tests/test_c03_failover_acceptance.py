from __future__ import annotations

import copy

import pytest

from scripts.acceptance.c03_failover_verify import (
    FailoverVerificationError,
    verify_failover,
)


def _status(*, voter: str, leader: str, term: int, session_term: int) -> dict[str, object]:
    return {
        "schema": "fcp.control-plane.status.v1",
        "cluster_id": "cluster-a",
        "voter_id": voter,
        "role": "LEADER" if voter == leader else "FOLLOWER",
        "consensus_leader_id": leader,
        "consensus_term": term,
        "fencing_epoch": term,
        "commit_index": 10 + term,
        "last_applied": 10 + term,
        "federation_id": "federation-stable",
        "ready": True,
        "voter_only": False,
        "sessions": [
            {
                "session_id": "session-a",
                "creator_node_id": "creator-node",
                "leader_node_id": leader,
                "leadership_term": session_term,
            }
        ],
    }


def test_verifier_accepts_same_federation_quorum_failover_and_return_fencing() -> None:
    before = _status(voter="node-a", leader="node-a", term=3, session_term=2)
    after = _status(voter="node-b", leader="node-b", term=4, session_term=3)
    returned = _status(voter="node-a", leader="node-b", term=4, session_term=3)
    returned["role"] = "FOLLOWER"

    result = verify_failover(before, after, returned)

    assert result["passed"] is True
    assert result["same_federation"] is True
    assert result["creator_provenance_preserved"] is True
    assert result["leader_changed"] is True
    assert result["returned_old_leader_fenced"] is True


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda value: value.__setitem__("federation_id", "replacement"), "Federation ID changed"),
        (lambda value: value.__setitem__("consensus_term", 3), "newer consensus term"),
        (lambda value: value.__setitem__("consensus_leader_id", "node-a"), "leader did not change"),
        (
            lambda value: value["sessions"][0].__setitem__("creator_node_id", "other"),
            "creator provenance changed",
        ),
    ],
)
def test_verifier_fails_closed_on_invalid_failover_evidence(mutation, message: str) -> None:
    before = _status(voter="node-a", leader="node-a", term=3, session_term=2)
    after = _status(voter="node-b", leader="node-b", term=4, session_term=3)
    mutation(after)

    with pytest.raises(FailoverVerificationError, match=message):
        verify_failover(before, after)


def test_verifier_rejects_returned_old_leader_that_still_claims_leadership() -> None:
    before = _status(voter="node-a", leader="node-a", term=3, session_term=2)
    after = _status(voter="node-b", leader="node-b", term=4, session_term=3)
    returned = copy.deepcopy(after)
    returned["voter_id"] = "node-a"
    returned["role"] = "LEADER"

    with pytest.raises(FailoverVerificationError, match="reclaimed stale authority"):
        verify_failover(before, after, returned)
