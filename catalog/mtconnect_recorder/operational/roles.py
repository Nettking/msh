"""Device-scoped semantic roles derived from canonical MTConnect metadata."""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum

from ..canonical.observation import (
    CATEGORY_EVENT,
    CATEGORY_SAMPLE,
    CanonicalObservation,
)
from .policy import device_key


class SemanticRole(str, Enum):
    EXECUTION_STATE = "EXECUTION_STATE"
    CONTROLLER_MODE = "CONTROLLER_MODE"
    PROGRAM = "PROGRAM"
    PROGRAM_COMMENT = "PROGRAM_COMMENT"
    LINE = "LINE"
    TOOL_NUMBER = "TOOL_NUMBER"
    TOOL_GROUP = "TOOL_GROUP"
    PALLET = "PALLET"
    SPINDLE_VELOCITY = "SPINDLE_VELOCITY"
    PATH_FEEDRATE = "PATH_FEEDRATE"
    AXIS_FEEDRATE = "AXIS_FEEDRATE"
    LOAD = "LOAD"


class RoleResolutionStatus(str, Enum):
    RESOLVED = "RESOLVED"
    UNAVAILABLE = "UNAVAILABLE"
    AMBIGUOUS = "AMBIGUOUS"


@dataclass(frozen=True)
class RoleCandidate:
    """One unique data channel that can supply a semantic role."""

    data_item_id: str | None
    component_type: str | None
    component_id: str | None
    component_path: str | None
    category: str
    semantic_type: str
    sub_type: str | None
    observation_type: str

    @property
    def binding(self) -> tuple[str, str, str | None]:
        """Return the semantic binding used to detect incompatible channels."""

        return (self.category, self.semantic_type, self.sub_type)


@dataclass(frozen=True)
class RoleResolution:
    role: SemanticRole
    status: RoleResolutionStatus
    candidates: tuple[RoleCandidate, ...]


@dataclass(frozen=True)
class DeviceRoleResolution:
    source_key: str
    agent_instance_id: int
    device_key: str
    roles: tuple[RoleResolution, ...]

    def role(self, role: SemanticRole) -> RoleResolution:
        """Return one role result without exposing tuple ordering to callers."""

        return next(item for item in self.roles if item.role is role)


_ROLE_BY_CATEGORY_AND_TOKEN = {
    (CATEGORY_EVENT, "EXECUTION"): SemanticRole.EXECUTION_STATE,
    (CATEGORY_EVENT, "CONTROLLERMODE"): SemanticRole.CONTROLLER_MODE,
    (CATEGORY_EVENT, "PROGRAM"): SemanticRole.PROGRAM,
    (CATEGORY_EVENT, "PROGRAMCOMMENT"): SemanticRole.PROGRAM_COMMENT,
    (CATEGORY_EVENT, "LINE"): SemanticRole.LINE,
    (CATEGORY_EVENT, "TOOLNUMBER"): SemanticRole.TOOL_NUMBER,
    (CATEGORY_EVENT, "TOOLGROUP"): SemanticRole.TOOL_GROUP,
    (CATEGORY_EVENT, "PALLETID"): SemanticRole.PALLET,
    (CATEGORY_SAMPLE, "SPINDLESPEED"): SemanticRole.SPINDLE_VELOCITY,
    (CATEGORY_SAMPLE, "PATHFEEDRATE"): SemanticRole.PATH_FEEDRATE,
    (CATEGORY_SAMPLE, "AXISFEEDRATE"): SemanticRole.AXIS_FEEDRATE,
    (CATEGORY_SAMPLE, "LOAD"): SemanticRole.LOAD,
}
_CANONICAL_TYPE_BY_TOKEN = {
    "EXECUTION": "EXECUTION",
    "CONTROLLERMODE": "CONTROLLER_MODE",
    "PROGRAM": "PROGRAM",
    "PROGRAMCOMMENT": "PROGRAM_COMMENT",
    "LINE": "LINE",
    "TOOLNUMBER": "TOOL_NUMBER",
    "TOOLGROUP": "TOOL_GROUP",
    "PALLETID": "PALLET_ID",
    "SPINDLESPEED": "SPINDLE_SPEED",
    "ROTARYVELOCITY": "ROTARY_VELOCITY",
    "PATHFEEDRATE": "PATH_FEEDRATE",
    "AXISFEEDRATE": "AXIS_FEEDRATE",
    "LOAD": "LOAD",
}
_KNOWN_TYPE_TOKENS = frozenset(_CANONICAL_TYPE_BY_TOKEN)
_NON_ALNUM = re.compile(r"[^A-Z0-9]+")


def _token(value: str | None) -> str:
    return _NON_ALNUM.sub("", (value or "").strip().upper())


def _sub_type(value: str | None) -> str | None:
    normalized = re.sub(r"[^A-Z0-9]+", "_", (value or "").strip().upper()).strip(
        "_"
    )
    return normalized or None


def _role_for_known_type(
    *, category: str, token: str, component_type: str | None
) -> SemanticRole | None:
    role = _ROLE_BY_CATEGORY_AND_TOKEN.get((category, token))
    if role is not None:
        return role
    if (
        category == CATEGORY_SAMPLE
        and token == "ROTARYVELOCITY"
        and _token(component_type) == "SPINDLE"
    ):
        return SemanticRole.SPINDLE_VELOCITY
    return None


def _classify(
    observation: CanonicalObservation,
) -> tuple[SemanticRole, str] | None:
    declared_type = _token(observation.mtconnect_type)
    if declared_type:
        declared_role = _role_for_known_type(
            category=observation.category,
            token=declared_type,
            component_type=observation.component_type,
        )
        if declared_role is not None:
            return declared_role, _CANONICAL_TYPE_BY_TOKEN[declared_type]
        # A known type in the wrong category, or rotary velocity without
        # explicit Spindle component evidence, must not be reinterpreted from
        # the stream element name.
        if declared_type in _KNOWN_TYPE_TOKENS:
            return None

    element_type = _token(observation.observation_type)
    element_role = _role_for_known_type(
        category=observation.category,
        token=element_type,
        component_type=observation.component_type,
    )
    if element_role is None:
        return None
    return element_role, _CANONICAL_TYPE_BY_TOKEN[element_type]


def _candidate(
    observation: CanonicalObservation, *, semantic_type: str
) -> RoleCandidate:
    return RoleCandidate(
        data_item_id=observation.data_item_id,
        component_type=observation.component_type,
        component_id=observation.component_id,
        component_path=observation.component_path,
        category=observation.category,
        semantic_type=semantic_type,
        sub_type=_sub_type(observation.sub_type),
        observation_type=observation.observation_type,
    )


def _candidate_sort_key(candidate: RoleCandidate) -> tuple[str, ...]:
    return (
        candidate.category,
        candidate.semantic_type,
        candidate.sub_type or "",
        candidate.component_path or "",
        candidate.component_type or "",
        candidate.component_id or "",
        candidate.data_item_id or "",
        candidate.observation_type,
    )


def _resolve_role(
    role: SemanticRole, candidates: Iterable[RoleCandidate]
) -> RoleResolution:
    unique = tuple(sorted(set(candidates), key=_candidate_sort_key))
    if not unique:
        status = RoleResolutionStatus.UNAVAILABLE
    elif len({candidate.binding for candidate in unique}) == 1:
        status = RoleResolutionStatus.RESOLVED
    else:
        status = RoleResolutionStatus.AMBIGUOUS
    return RoleResolution(role=role, status=status, candidates=unique)


def resolve_device_roles(
    observations: Iterable[CanonicalObservation],
) -> tuple[DeviceRoleResolution, ...]:
    """Resolve every v1 role independently for each canonical device scope."""

    grouped: defaultdict[
        tuple[str, int, str], list[CanonicalObservation]
    ] = defaultdict(list)
    for observation in observations:
        grouped[
            (
                observation.source_key,
                observation.agent_instance_id,
                device_key(observation),
            )
        ].append(observation)

    resolved: list[DeviceRoleResolution] = []
    for (source, instance, canonical_device), rows in sorted(grouped.items()):
        candidates_by_role: defaultdict[SemanticRole, list[RoleCandidate]] = (
            defaultdict(list)
        )
        for observation in rows:
            classified = _classify(observation)
            if classified is None:
                continue
            role, semantic_type = classified
            candidates_by_role[role].append(
                _candidate(observation, semantic_type=semantic_type)
            )

        resolved.append(
            DeviceRoleResolution(
                source_key=source,
                agent_instance_id=instance,
                device_key=canonical_device,
                roles=tuple(
                    _resolve_role(role, candidates_by_role.get(role, ()))
                    for role in SemanticRole
                ),
            )
        )
    return tuple(resolved)


__all__ = [
    "DeviceRoleResolution",
    "RoleCandidate",
    "RoleResolution",
    "RoleResolutionStatus",
    "SemanticRole",
    "resolve_device_roles",
]
