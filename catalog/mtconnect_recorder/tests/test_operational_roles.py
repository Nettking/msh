"""S1 semantic roles over immutable canonical MTConnect observations."""

from __future__ import annotations

from copy import deepcopy

import pytest

from catalog.mtconnect_recorder.canonical.observation import (
    CATEGORY_CONDITION,
    CATEGORY_EVENT,
    CATEGORY_SAMPLE,
    CanonicalObservation,
)
from catalog.mtconnect_recorder.operational import (
    SEGMENTATION_POLICY,
    RoleResolutionStatus,
    SemanticRole,
    device_key,
    resolve_device_roles,
)

SOURCE = "MACHINE-ALPHA-0001"
INSTANCE = 1786093233


def _observation(
    *,
    sequence: int = 1,
    source: str = SOURCE,
    instance: int = INSTANCE,
    machine_id: str = "machine-alpha",
    device_uuid: str | None = "device-alpha",
    category: str = CATEGORY_EVENT,
    observation_type: str = "Message",
    mtconnect_type: str | None = None,
    sub_type: str | None = None,
    data_item_id: str | None = "item-1",
    component_type: str = "Controller",
    component_id: str = "controller-1",
    component_name: str = "controller",
) -> CanonicalObservation:
    record = {
        "sequence": sequence,
        "machine_id": machine_id,
        "device_uuid": device_uuid,
        "device_name": machine_id,
        "category": category,
        "observation_type": observation_type,
        "sub_type": sub_type,
        "data_item_id": data_item_id,
        "stream_component": component_type,
        "stream_component_id": component_id,
        "stream_component_name": component_name,
        "native_value": "ACTIVE",
        "attributes": {"timestamp": f"2026-08-07T09:00:{sequence:02d}Z"},
    }
    if mtconnect_type is not None:
        record["type"] = mtconnect_type
    return CanonicalObservation.from_recorder_record(
        record,
        source_name=source,
        agent_instance_id=instance,
        raw_batch_sha256="a" * 64,
    )


def _single(observation: CanonicalObservation):
    resolutions = resolve_device_roles([observation])
    assert len(resolutions) == 1
    return resolutions[0]


def test_policy_and_device_key_are_deterministic_and_namespaced():
    assert SEGMENTATION_POLICY == "fcp.mtconnect.operational-segmentation.v1"

    uuid_observation = _observation(device_uuid=" device-42 ", machine_id="machine-7")
    machine_observation = _observation(device_uuid=None, machine_id=" device-42 ")

    assert device_key(uuid_observation) == "uuid:device-42"
    assert device_key(machine_observation) == "machine:device-42"
    assert device_key(uuid_observation) != device_key(machine_observation)


@pytest.mark.parametrize(
    ("category", "mtconnect_type", "component_type", "role"),
    [
        (CATEGORY_EVENT, "EXECUTION", "Controller", SemanticRole.EXECUTION_STATE),
        (
            CATEGORY_EVENT,
            "CONTROLLER_MODE",
            "Controller",
            SemanticRole.CONTROLLER_MODE,
        ),
        (CATEGORY_EVENT, "PROGRAM", "Path", SemanticRole.PROGRAM),
        (
            CATEGORY_EVENT,
            "PROGRAM_COMMENT",
            "Path",
            SemanticRole.PROGRAM_COMMENT,
        ),
        (CATEGORY_EVENT, "LINE", "Path", SemanticRole.LINE),
        (CATEGORY_EVENT, "TOOL_NUMBER", "Path", SemanticRole.TOOL_NUMBER),
        (CATEGORY_EVENT, "TOOL_GROUP", "Path", SemanticRole.TOOL_GROUP),
        (CATEGORY_EVENT, "PALLET_ID", "Device", SemanticRole.PALLET),
        (
            CATEGORY_SAMPLE,
            "SPINDLE_SPEED",
            "Spindle",
            SemanticRole.SPINDLE_VELOCITY,
        ),
        (
            CATEGORY_SAMPLE,
            "PATH_FEEDRATE",
            "Path",
            SemanticRole.PATH_FEEDRATE,
        ),
        (
            CATEGORY_SAMPLE,
            "AXIS_FEEDRATE",
            "Linear",
            SemanticRole.AXIS_FEEDRATE,
        ),
        (CATEGORY_SAMPLE, "LOAD", "Linear", SemanticRole.LOAD),
    ],
)
def test_probe_types_resolve_every_v1_role(
    category, mtconnect_type, component_type, role
):
    result = _single(
        _observation(
            category=category,
            observation_type="VendorElement",
            mtconnect_type=mtconnect_type,
            component_type=component_type,
        )
    ).role(role)

    assert result.status is RoleResolutionStatus.RESOLVED
    assert len(result.candidates) == 1


def test_roles_are_scoped_independently_to_each_device():
    first = _observation(
        device_uuid="device-alpha",
        data_item_id="execution-a",
        observation_type="Execution",
        mtconnect_type="EXECUTION",
    )
    second = _observation(
        sequence=2,
        device_uuid="device-beta",
        machine_id="machine-beta",
        data_item_id="execution-b",
        observation_type="Execution",
        mtconnect_type="EXECUTION",
    )

    resolutions = resolve_device_roles([first, second])

    assert [item.device_key for item in resolutions] == [
        "uuid:device-alpha",
        "uuid:device-beta",
    ]
    assert all(
        item.role(SemanticRole.EXECUTION_STATE).status
        is RoleResolutionStatus.RESOLVED
        for item in resolutions
    )


def test_same_device_identifier_in_two_agent_instances_stays_separate():
    first = _observation(
        instance=INSTANCE,
        observation_type="Execution",
        mtconnect_type="EXECUTION",
    )
    restarted = _observation(
        instance=INSTANCE + 1,
        observation_type="Execution",
        mtconnect_type="EXECUTION",
    )

    resolutions = resolve_device_roles([restarted, first])

    assert [(item.agent_instance_id, item.device_key) for item in resolutions] == [
        (INSTANCE, "uuid:device-alpha"),
        (INSTANCE + 1, "uuid:device-alpha"),
    ]


def test_declared_type_takes_precedence_over_element_and_incidental_id():
    resolution = _single(
        _observation(
            data_item_id="exec",
            observation_type="Execution",
            mtconnect_type="PROGRAM",
        )
    )

    assert resolution.role(SemanticRole.PROGRAM).status is RoleResolutionStatus.RESOLVED
    assert (
        resolution.role(SemanticRole.EXECUTION_STATE).status
        is RoleResolutionStatus.UNAVAILABLE
    )


def test_exact_observation_element_is_the_fallback_without_a_declared_type():
    resolution = _single(
        _observation(
            data_item_id="controller-state-42",
            observation_type="Execution",
            mtconnect_type=None,
        )
    )

    assert (
        resolution.role(SemanticRole.EXECUTION_STATE).status
        is RoleResolutionStatus.RESOLVED
    )


def test_source_specific_ids_do_not_create_semantic_roles():
    rows = [
        _observation(sequence=index, data_item_id=data_item_id)
        for index, data_item_id in enumerate(("exec", "tid", "spgm"), start=1)
    ]

    resolution = resolve_device_roles(rows)[0]

    assert all(
        item.status is RoleResolutionStatus.UNAVAILABLE for item in resolution.roles
    )


def test_incompatible_same_device_bindings_are_ambiguous():
    active = _observation(
        data_item_id="program-active",
        observation_type="Program",
        mtconnect_type="PROGRAM",
        sub_type="ACTIVE",
    )
    main = _observation(
        sequence=2,
        data_item_id="program-main",
        observation_type="Program",
        mtconnect_type="PROGRAM",
        sub_type="MAIN",
    )

    result = resolve_device_roles([active, main])[0].role(SemanticRole.PROGRAM)

    assert result.status is RoleResolutionStatus.AMBIGUOUS
    assert [candidate.sub_type for candidate in result.candidates] == ["ACTIVE", "MAIN"]


def test_compatible_multi_axis_channels_remain_resolved():
    x_load = _observation(
        data_item_id="x-load",
        category=CATEGORY_SAMPLE,
        observation_type="Load",
        mtconnect_type="LOAD",
        sub_type="ACTUAL",
        component_type="Linear",
        component_id="x-axis",
        component_name="X",
    )
    y_load = _observation(
        sequence=2,
        data_item_id="y-load",
        category=CATEGORY_SAMPLE,
        observation_type="Load",
        mtconnect_type="LOAD",
        sub_type="ACTUAL",
        component_type="Linear",
        component_id="y-axis",
        component_name="Y",
    )

    result = resolve_device_roles([y_load, x_load])[0].role(SemanticRole.LOAD)

    assert result.status is RoleResolutionStatus.RESOLVED
    assert [candidate.data_item_id for candidate in result.candidates] == [
        "x-load",
        "y-load",
    ]


def test_known_type_in_the_wrong_category_does_not_fall_through():
    condition = _observation(
        category=CATEGORY_CONDITION,
        observation_type="Load",
        mtconnect_type="LOAD",
    )

    assert (
        _single(condition).role(SemanticRole.LOAD).status
        is RoleResolutionStatus.UNAVAILABLE
    )


def test_repeated_input_is_deduplicated_and_order_independent():
    active = _observation(
        sequence=1,
        data_item_id="controller-state",
        observation_type="Execution",
        mtconnect_type="EXECUTION",
    )
    ready = _observation(
        sequence=2,
        data_item_id="controller-state",
        observation_type="Execution",
        mtconnect_type="EXECUTION",
    )

    forward = resolve_device_roles([active, ready])
    reversed_with_repeat = resolve_device_roles([ready, active, active])

    assert forward == reversed_with_repeat
    assert len(
        forward[0].role(SemanticRole.EXECUTION_STATE).candidates
    ) == 1


def test_resolution_does_not_mutate_canonical_evidence():
    rows = [
        _observation(
            data_item_id="controller-state",
            observation_type="Execution",
            mtconnect_type="EXECUTION",
        )
    ]
    snapshot = deepcopy(rows)

    resolve_device_roles(rows)

    assert rows == snapshot


def test_rotary_axis_velocity_is_not_assumed_to_be_spindle_velocity():
    c_axis = _observation(
        device_uuid="device-axis",
        machine_id="machine-axis",
        category=CATEGORY_SAMPLE,
        observation_type="RotaryVelocity",
        mtconnect_type="ROTARY_VELOCITY",
        component_type="Rotary",
        component_id="c-axis",
        component_name="C",
    )
    spindle = _observation(
        sequence=2,
        device_uuid="device-spindle",
        machine_id="machine-spindle",
        category=CATEGORY_SAMPLE,
        observation_type="RotaryVelocity",
        mtconnect_type="ROTARY_VELOCITY",
        component_type="Spindle",
        component_id="spindle-1",
        component_name="main",
    )

    by_device = {
        item.device_key: item for item in resolve_device_roles([spindle, c_axis])
    }

    assert (
        by_device["uuid:device-axis"].role(SemanticRole.SPINDLE_VELOCITY).status
        is RoleResolutionStatus.UNAVAILABLE
    )
    assert (
        by_device["uuid:device-spindle"]
        .role(SemanticRole.SPINDLE_VELOCITY)
        .status
        is RoleResolutionStatus.RESOLVED
    )
