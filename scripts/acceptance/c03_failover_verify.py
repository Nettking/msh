"""Read-only verifier for Federation v1 C03 leader-loss evidence.

The physical operator captures ``control-plane-status.json`` before removing the
current operational leader and again from the surviving elected leader.  When
the old host is later returned, its status may be supplied as a third snapshot
to prove that it accepted the newer term and did not reclaim authority.

This helper never changes product state and prints no endpoint, credential, key
or local-path material from the status documents.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

SCHEMA = "fcp.control-plane.status.v1"


class FailoverVerificationError(RuntimeError):
    """The captured C03 evidence does not prove safe Federation failover."""


def _load(path: Path | str) -> dict[str, Any]:
    target = Path(path)
    try:
        value = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FailoverVerificationError("C03 status snapshot is unreadable") from exc
    if not isinstance(value, dict) or value.get("schema") != SCHEMA:
        raise FailoverVerificationError("C03 status snapshot schema is invalid")
    return value


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FailoverVerificationError(f"{field} is not a non-negative integer")
    return value


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise FailoverVerificationError(f"{field} is missing")
    return value


def _sessions(status: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    raw = status.get("sessions")
    if not isinstance(raw, list) or not raw:
        raise FailoverVerificationError("C03 status contains no session authority")
    result: dict[str, dict[str, Any]] = {}
    for item in raw:
        if not isinstance(item, dict):
            raise FailoverVerificationError("C03 session authority is malformed")
        session_id = _text(item.get("session_id"), "session_id")
        if session_id in result:
            raise FailoverVerificationError("C03 session authority is duplicated")
        _text(item.get("creator_node_id"), "creator_node_id")
        _text(item.get("leader_node_id"), "leader_node_id")
        _positive_int(item.get("leadership_term"), "leadership_term")
        result[session_id] = item
    return result


def verify_failover(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    returned: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Prove same-Federation quorum failover and optional stale-leader fencing."""

    for name, status in (("before", before), ("after", after)):
        if status.get("schema") != SCHEMA:
            raise FailoverVerificationError(f"{name} status schema is invalid")
        if status.get("ready") is not True:
            raise FailoverVerificationError(f"{name} authority is not readiness-sealed")
        if status.get("voter_only") is not False:
            raise FailoverVerificationError(f"{name} snapshot is not an operational voter")

    federation_id = _text(before.get("federation_id"), "before federation_id")
    if after.get("federation_id") != federation_id:
        raise FailoverVerificationError("Federation ID changed across leader loss")
    cluster_id = _text(before.get("cluster_id"), "before cluster_id")
    if after.get("cluster_id") != cluster_id:
        raise FailoverVerificationError("control-plane cluster changed across leader loss")

    before_term = _positive_int(before.get("consensus_term"), "before consensus_term")
    after_term = _positive_int(after.get("consensus_term"), "after consensus_term")
    if after_term <= before_term:
        raise FailoverVerificationError("surviving voters did not advance to a newer consensus term")

    old_leader = _text(before.get("consensus_leader_id"), "before consensus_leader_id")
    new_leader = _text(after.get("consensus_leader_id"), "after consensus_leader_id")
    if old_leader == new_leader:
        raise FailoverVerificationError("leader did not change after the old leader was removed")
    if after.get("role") != "LEADER" or after.get("voter_id") != new_leader:
        raise FailoverVerificationError("after snapshot is not the elected surviving leader")
    if _positive_int(after.get("commit_index"), "after commit_index") != _positive_int(
        after.get("last_applied"), "after last_applied"
    ):
        raise FailoverVerificationError("surviving leader has unapplied committed authority")

    before_sessions = _sessions(before)
    after_sessions = _sessions(after)
    if set(before_sessions) != set(after_sessions):
        raise FailoverVerificationError("session set changed across leader loss")
    for session_id, old in before_sessions.items():
        new = after_sessions[session_id]
        if new.get("creator_node_id") != old.get("creator_node_id"):
            raise FailoverVerificationError("immutable creator provenance changed")
        if new.get("leader_node_id") != new_leader:
            raise FailoverVerificationError("operational session did not follow the elected leader")
        if _positive_int(new.get("leadership_term"), "after leadership_term") <= _positive_int(
            old.get("leadership_term"), "before leadership_term"
        ):
            raise FailoverVerificationError("session leadership term did not advance")

    returned_fenced = None
    if returned is not None:
        if returned.get("schema") != SCHEMA:
            raise FailoverVerificationError("returned-leader status schema is invalid")
        if returned.get("federation_id") != federation_id or returned.get("cluster_id") != cluster_id:
            raise FailoverVerificationError("returned host belongs to another Federation/cluster")
        returned_term = _positive_int(returned.get("consensus_term"), "returned consensus_term")
        if returned_term < after_term:
            raise FailoverVerificationError("returned old leader has not observed the newer term")
        if returned.get("consensus_leader_id") != new_leader:
            raise FailoverVerificationError("returned old leader does not recognize the elected successor")
        if returned.get("voter_id") == old_leader and returned.get("role") == "LEADER":
            raise FailoverVerificationError("returned old leader reclaimed stale authority")
        returned_fenced = True

    return {
        "schema": "fcp.c03.failover-verification.v1",
        "passed": True,
        "federation_id": federation_id,
        "cluster_id": cluster_id,
        "before_term": before_term,
        "after_term": after_term,
        "leader_changed": True,
        "creator_provenance_preserved": True,
        "same_federation": True,
        "returned_old_leader_fenced": returned_fenced,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify C03 physical leader-loss evidence")
    parser.add_argument("--before", required=True)
    parser.add_argument("--after", required=True)
    parser.add_argument("--returned")
    args = parser.parse_args(argv)
    try:
        result = verify_failover(
            _load(args.before),
            _load(args.after),
            _load(args.returned) if args.returned else None,
        )
    except FailoverVerificationError as exc:
        print(json.dumps({"schema": "fcp.c03.failover-verification.v1", "passed": False, "error": str(exc)}, sort_keys=True))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
