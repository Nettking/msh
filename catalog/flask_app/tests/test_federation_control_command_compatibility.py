from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from catalog.federation.software_update import APPROVED_BRANCH, APPROVED_REPOSITORY
from catalog.flask_app.services.federation_capability_requests import (
    EVENT_SCHEMA as CAPABILITY_SCHEMA,
)
from catalog.flask_app.services.federation_capability_requests import (
    request_payload,
    validate_request_payload,
)
from catalog.flask_app.services.federation_update_events import (
    EVENT_SCHEMA as UPDATE_SCHEMA,
)
from catalog.flask_app.services.federation_update_events import (
    command_payload,
    validate_command_payload,
)

TARGET = "a" * 40


def test_existing_control_command_wire_shapes_are_preserved() -> None:
    now = datetime.now(timezone.utc)
    update = command_payload(
        request_id="update-one",
        target_commit=TARGET,
        target_node_ids=("node-a", "node-a", "node-b"),
        created_at=now,
        expires_at=now + timedelta(minutes=10),
    )
    capability = request_payload(
        request_id="capability-one",
        target_node_ids=("node-a", "node-a", "node-b"),
        created_at=now,
        expires_at=now + timedelta(minutes=10),
    )

    assert update == {
        "schema": UPDATE_SCHEMA,
        "request_id": "update-one",
        "repository": APPROVED_REPOSITORY,
        "branch": APPROVED_BRANCH,
        "target_commit": TARGET,
        "target_node_ids": ["node-a", "node-b"],
        "created_at": update["created_at"],
        "expires_at": update["expires_at"],
    }
    assert capability == {
        "schema": CAPABILITY_SCHEMA,
        "request_id": "capability-one",
        "actions": ["benchmark", "contribute"],
        "target_node_ids": ["node-a", "node-b"],
        "created_at": capability["created_at"],
        "expires_at": capability["expires_at"],
    }
    assert validate_command_payload(update) is update
    assert validate_request_payload(capability) is capability


def test_both_control_command_validators_reject_reversed_lifetimes() -> None:
    now = datetime.now(timezone.utc)
    update = command_payload(
        request_id="update-one",
        target_commit=TARGET,
        target_node_ids=("node-a",),
        created_at=now,
        expires_at=now + timedelta(minutes=10),
    )
    capability = request_payload(
        request_id="capability-one",
        target_node_ids=("node-a",),
        created_at=now,
        expires_at=now + timedelta(minutes=10),
    )
    for payload, validator in (
        (update, validate_command_payload),
        (capability, validate_request_payload),
    ):
        payload["created_at"] = (now + timedelta(seconds=30)).isoformat()
        payload["expires_at"] = (now + timedelta(seconds=10)).isoformat()
        with pytest.raises(ValueError, match="expired_or_invalid_request"):
            validator(payload)
