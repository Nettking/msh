"""Keep MTConnect logical component paths routable without weakening relay redaction."""

from __future__ import annotations

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.relay.service import _ensure_bounded_json


def _payload(component_path: object = "Linear/X") -> dict[str, object]:
    return {
        "kind": "fcp-recorder-logical-storage-v1",
        "message": "request",
        "content": {
            "schema": "fcp.mtconnect.observations.v1",
            "observations": [
                {
                    "sequence": 1,
                    "component_path": component_path,
                    "native_value": "1.25",
                }
            ],
        },
    }


def test_mtconnect_logical_component_path_is_routable() -> None:
    _ensure_bounded_json(_payload(), field="payload")


def test_missing_mtconnect_component_path_is_routable() -> None:
    _ensure_bounded_json(_payload(None), field="payload")


@pytest.mark.parametrize(
    "component_path",
    [
        "/etc/fcp/control.sqlite3",
        "C:/fcp/data/control.sqlite3",
        "../control.sqlite3",
        "Controller/../control.sqlite3",
        "Controller\\X",
        "https://leader.example.com/storage",
        "100.64.0.4:8765/storage",
        "",
        " Linear/X",
    ],
)
def test_location_shaped_component_path_is_still_rejected(component_path: str) -> None:
    with pytest.raises(FederationValidationError) as caught:
        _ensure_bounded_json(_payload(component_path), field="payload")

    assert caught.value.code == "nonpublic-payload"


def test_component_path_outside_exact_mtconnect_schema_is_rejected() -> None:
    payload = _payload()
    content = payload["content"]
    assert isinstance(content, dict)
    content["schema"] = "fcp.mtconnect.observations.v2"

    with pytest.raises(FederationValidationError) as caught:
        _ensure_bounded_json(payload, field="payload")

    assert caught.value.code == "nonpublic-payload"


def test_component_path_outside_observation_record_is_rejected() -> None:
    with pytest.raises(FederationValidationError) as caught:
        _ensure_bounded_json(
            {
                "content": {
                    "schema": "fcp.mtconnect.observations.v1",
                    "component_path": "Linear/X",
                    "observations": [],
                }
            },
            field="payload",
        )

    assert caught.value.code == "nonpublic-payload"


def test_other_path_field_inside_mtconnect_observation_is_rejected() -> None:
    payload = _payload()
    content = payload["content"]
    assert isinstance(content, dict)
    observations = content["observations"]
    assert isinstance(observations, list)
    observation = observations[0]
    assert isinstance(observation, dict)
    observation["cache_path"] = "relative/cache"

    with pytest.raises(FederationValidationError) as caught:
        _ensure_bounded_json(payload, field="payload")

    assert caught.value.code == "nonpublic-payload"


def test_nested_component_path_is_not_allowlisted() -> None:
    payload = _payload()
    content = payload["content"]
    assert isinstance(content, dict)
    observations = content["observations"]
    assert isinstance(observations, list)
    observation = observations[0]
    assert isinstance(observation, dict)
    observation["attributes"] = {"component_path": "Linear/X"}

    with pytest.raises(FederationValidationError) as caught:
        _ensure_bounded_json(payload, field="payload")

    assert caught.value.code == "nonpublic-payload"


def test_credentials_inside_mtconnect_observation_are_still_rejected() -> None:
    payload = _payload()
    content = payload["content"]
    assert isinstance(content, dict)
    observations = content["observations"]
    assert isinstance(observations, list)
    observation = observations[0]
    assert isinstance(observation, dict)
    observation["token"] = "fcp_join_" + "e" * 40

    with pytest.raises(FederationValidationError) as caught:
        _ensure_bounded_json(payload, field="payload")

    assert caught.value.code == "nonpublic-payload"
