"""Keep MTConnect structural component paths routable without weakening redaction."""

from __future__ import annotations

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.relay.service import _ensure_bounded_json

_DEFAULT = object()


def _component_path() -> list[dict[str, object]]:
    return [
        {
            "type": "Controller",
            "id": "controller",
            "name": None,
            "native_name": None,
        },
        {
            "type": "Linear",
            "id": "x",
            "name": "X",
            "native_name": "X",
        },
    ]


def _payload(component_path: object = _DEFAULT) -> dict[str, object]:
    if component_path is _DEFAULT:
        component_path = _component_path()
    return {
        "kind": "fcp-recorder-logical-storage-v1",
        "message": "request",
        "content": {
            "schema": "fcp.mtconnect.observations.v1",
            "observations": [
                {
                    "schema": "fcp.mtconnect.observation.v2",
                    "sequence": 1,
                    "component_path": component_path,
                    "native_value": "1.25",
                }
            ],
        },
    }


def test_mtconnect_structural_component_path_is_routable() -> None:
    _ensure_bounded_json(_payload(), field="payload")


def test_empty_mtconnect_component_path_is_routable() -> None:
    _ensure_bounded_json(_payload([]), field="payload")


@pytest.mark.parametrize(
    "component_path",
    [
        "Linear/X",
        None,
        [{"type": "Linear", "id": "x", "name": "X"}],
        [
            {
                "type": "Linear",
                "id": "x",
                "name": "C:/fcp/data/control.sqlite3",
                "native_name": None,
            }
        ],
        [
            {
                "type": "Linear",
                "id": "x",
                "name": "https://leader.example.com/storage",
                "native_name": None,
            }
        ],
        [
            {
                "type": "Linear",
                "id": "x",
                "name": "100.64.0.4:8765",
                "native_name": None,
            }
        ],
        [
            {
                "type": "Linear",
                "id": "fcp_join_" + "e" * 40,
                "name": "X",
                "native_name": None,
            }
        ],
        [
            {
                "type": "Linear",
                "id": "x",
                "name": "X",
                "native_name": None,
                "path": "relative/cache",
            }
        ],
    ],
)
def test_non_structural_or_nonpublic_component_path_is_rejected(
    component_path: object,
) -> None:
    with pytest.raises(FederationValidationError) as caught:
        _ensure_bounded_json(_payload(component_path), field="payload")

    assert caught.value.code == "nonpublic-payload"


def test_component_path_outside_exact_mtconnect_content_schema_is_rejected() -> None:
    payload = _payload()
    content = payload["content"]
    assert isinstance(content, dict)
    content["schema"] = "fcp.mtconnect.observations.v2"

    with pytest.raises(FederationValidationError) as caught:
        _ensure_bounded_json(payload, field="payload")

    assert caught.value.code == "nonpublic-payload"


def test_component_path_outside_exact_observation_schema_is_rejected() -> None:
    payload = _payload()
    content = payload["content"]
    assert isinstance(content, dict)
    observations = content["observations"]
    assert isinstance(observations, list)
    observation = observations[0]
    assert isinstance(observation, dict)
    observation["schema"] = "fcp.mtconnect.observation.v1"

    with pytest.raises(FederationValidationError) as caught:
        _ensure_bounded_json(payload, field="payload")

    assert caught.value.code == "nonpublic-payload"


def test_component_path_outside_observation_record_is_rejected() -> None:
    with pytest.raises(FederationValidationError) as caught:
        _ensure_bounded_json(
            {
                "content": {
                    "schema": "fcp.mtconnect.observations.v1",
                    "component_path": _component_path(),
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
    observation["attributes"] = {"component_path": _component_path()}

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
