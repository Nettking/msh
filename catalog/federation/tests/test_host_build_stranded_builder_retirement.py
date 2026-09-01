"""A checkout-scoped builder that outlives its checkout is FCP's to reclaim."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation import host_build
from catalog.federation.host_resources import PressureLevel, ResourceAssessment

CURRENT = "fcp-build-current"
ORPHAN = "fcp-build-orphan"
SIBLING = "fcp-build-sibling"
BUILDKIT = "buildx_buildkit_"


def _assessment(level: PressureLevel) -> ResourceAssessment:
    return ResourceAssessment(
        resource_id="device:docker",
        level=level,
        reasons=(),
        effective_free_bytes=12 * 1024**3,
        effective_free_inodes=100_000,
        reserved_bytes=0,
        reserved_inodes=0,
        observed_at=datetime.now(timezone.utc),
    )


def _stamp(root: Path) -> str:
    return "FCP_BUILDER_ROOT_HEX=" + str(root).encode("utf-8").hex()


class _Docker:
    """Small Docker whose insertion order is newest-to-oldest."""

    def __init__(
        self,
        *,
        builders: dict[str, list[str] | None],
        running: tuple[str, ...] = (),
        holding: tuple[str, ...] = (),
    ) -> None:
        self.builders = dict(builders)
        self.running = set(running)
        self.holding = set(holding)
        self.commands: list[list[str]] = []
        self.ids = {
            name: f"{index + 1:064x}" for index, name in enumerate(self.builders)
        }

    def pressured(self) -> bool:
        return bool(self.holding & set(self.builders))

    def run(self, _root, args, **_kwargs):
        command = list(args)
        self.commands.append(command)
        if command[:3] == ["docker", "buildx", "ls"]:
            return SimpleNamespace(
                returncode=0, stdout="\n".join(self.builders) + "\n", stderr=""
            )
        if command[:3] == ["docker", "buildx", "inspect"]:
            if command[3] in self.builders:
                return SimpleNamespace(
                    returncode=0,
                    stdout="Driver: docker-container\nStatus: stopped\n",
                    stderr="",
                )
            return SimpleNamespace(returncode=1, stdout="", stderr="no builder")
        if command[:3] == ["docker", "buildx", "stop"]:
            self.running.discard(command[3])
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if command[:3] == ["docker", "buildx", "rm"]:
            name = command[-1]
            self.builders.pop(name, None)
            self.running.discard(name)
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if command[:2] == ["docker", "ps"]:
            if "--all" not in command:
                return SimpleNamespace(
                    returncode=0,
                    stdout="".join(
                        f"{BUILDKIT}{name}0\n" for name in sorted(self.running)
                    ),
                    stderr="",
                )
            candidates = [
                name
                for name, environment in self.builders.items()
                if environment is not None and name.startswith("fcp-build-")
            ]
            before = next(
                (
                    item.removeprefix("before=")
                    for item in command
                    if item.startswith("before=")
                ),
                None,
            )
            if before is not None:
                matching = next(
                    (index for index, name in enumerate(candidates) if self.ids[name] == before),
                    None,
                )
                if matching is None:
                    return SimpleNamespace(returncode=1, stdout="", stderr="stale cursor")
                candidates = candidates[matching + 1 :]
            limit = int(command[command.index("--last") + 1])
            candidates = candidates[:limit]
            return SimpleNamespace(
                returncode=0,
                stdout="".join(
                    f"{self.ids[name]} {BUILDKIT}{name}0\n" for name in candidates
                ),
                stderr="",
            )
        if command[:2] == ["docker", "inspect"]:
            name = command[-1].removeprefix(BUILDKIT).removesuffix("0")
            environment = self.builders.get(name)
            if environment is None:
                return SimpleNamespace(returncode=1, stdout="", stderr="no such object")
            return SimpleNamespace(
                returncode=0, stdout="\n".join(environment) + "\n", stderr=""
            )
        return SimpleNamespace(returncode=1, stdout="", stderr="unexpected")

    def removed(self, name: str) -> bool:
        return any(
            command[:3] == ["docker", "buildx", "rm"] and command[-1] == name
            for command in self.commands
        )


def _install(monkeypatch: pytest.MonkeyPatch, docker: _Docker, root: Path) -> None:
    monkeypatch.setenv("FCP_BUILDER_RETIREMENT_STATE_DIR", str(root / "cursor"))
    monkeypatch.setattr(host_build, "builder_name", lambda _root: CURRENT)
    monkeypatch.setattr(host_build, "_docker_run", docker.run)

    class _Admission:
        def assessment(self, _backing_path):
            return _assessment(
                PressureLevel.CRITICAL if docker.pressured() else PressureLevel.NORMAL
            )

    monkeypatch.setattr(host_build, "PROCESS_RESOURCE_ADMISSION", _Admission())
    monkeypatch.setattr(
        host_build,
        "docker_resource_assessment",
        lambda _root, _env, **_kwargs: (root, _assessment(PressureLevel.CRITICAL)),
    )


def test_a_pressured_host_reclaims_fcp_cache_whose_checkout_is_gone(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    gone = tmp_path / "removed-checkout"
    docker = _Docker(
        builders={CURRENT: [_stamp(tmp_path)], ORPHAN: [_stamp(gone)]},
        holding=(ORPHAN,),
    )
    _install(monkeypatch, docker, tmp_path)

    host_build.preflight_disk(tmp_path, {})

    assert docker.removed(ORPHAN)
    assert ORPHAN not in docker.builders


def test_a_sibling_checkouts_builder_is_never_retired(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    sibling_root = tmp_path / "sibling"
    sibling_root.mkdir()
    docker = _Docker(
        builders={CURRENT: [_stamp(tmp_path)], SIBLING: [_stamp(sibling_root)]},
        holding=(SIBLING,),
    )
    _install(monkeypatch, docker, tmp_path)

    with pytest.raises(RuntimeError, match="insufficient_disk_for_update"):
        host_build.preflight_disk(tmp_path, {})

    assert not docker.removed(SIBLING)


def test_an_unstamped_builder_is_refused_rather_than_guessed_at(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    docker = _Docker(
        builders={CURRENT: [_stamp(tmp_path)], ORPHAN: ["PATH=/usr/bin"]},
        holding=(ORPHAN,),
    )
    _install(monkeypatch, docker, tmp_path)

    with pytest.raises(RuntimeError, match="insufficient_disk_for_update"):
        host_build.preflight_disk(tmp_path, {})

    assert not docker.removed(ORPHAN)


def test_a_builder_with_no_inspectable_container_is_not_a_candidate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    docker = _Docker(builders={CURRENT: [_stamp(tmp_path)], ORPHAN: None}, holding=(ORPHAN,))
    _install(monkeypatch, docker, tmp_path)

    with pytest.raises(RuntimeError, match="insufficient_disk_for_update"):
        host_build.preflight_disk(tmp_path, {})

    assert not docker.removed(ORPHAN)
    assert not any(
        command[:2] == ["docker", "inspect"] and command[-1] == f"{BUILDKIT}{ORPHAN}0"
        for command in docker.commands
    )


def test_a_running_buildkit_container_outranks_a_missing_checkout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    gone = tmp_path / "removed-checkout"
    docker = _Docker(
        builders={CURRENT: [_stamp(tmp_path)], ORPHAN: [_stamp(gone)]},
        running=(ORPHAN,),
        holding=(ORPHAN,),
    )
    _install(monkeypatch, docker, tmp_path)

    with pytest.raises(RuntimeError, match="insufficient_disk_for_update"):
        host_build.preflight_disk(tmp_path, {})

    assert not docker.removed(ORPHAN)


def test_non_fcp_builders_are_never_examined_at_all(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    gone = tmp_path / "removed-checkout"
    docker = _Docker(
        builders={
            CURRENT: [_stamp(tmp_path)],
            "default": [],
            "someone-elses": [_stamp(gone)],
            ORPHAN: [_stamp(gone)],
        },
        holding=(ORPHAN,),
    )
    _install(monkeypatch, docker, tmp_path)

    host_build.preflight_disk(tmp_path, {})

    assert docker.removed(ORPHAN)
    inspected = {
        command[-1]
        for command in docker.commands
        if command[:2] == ["docker", "inspect"]
    }
    assert inspected == {f"{BUILDKIT}{ORPHAN}0"}


def test_reclamation_never_overrides_the_pressure_verdict(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    gone = tmp_path / "removed-checkout"
    docker = _Docker(
        builders={
            CURRENT: [_stamp(tmp_path)],
            ORPHAN: [_stamp(gone)],
            "something-else": [],
        },
        holding=(ORPHAN, "something-else"),
    )
    _install(monkeypatch, docker, tmp_path)

    with pytest.raises(RuntimeError, match="insufficient_disk_for_update"):
        host_build.preflight_disk(tmp_path, {})

    assert docker.removed(ORPHAN)


def test_an_unpressured_start_does_not_enumerate_retirement_candidates(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    docker = _Docker(builders={CURRENT: [_stamp(tmp_path)]})
    _install(monkeypatch, docker, tmp_path)
    monkeypatch.setattr(
        host_build,
        "docker_resource_assessment",
        lambda _root, _env, **_kwargs: (tmp_path, _assessment(PressureLevel.NORMAL)),
    )

    host_build.preflight_disk(tmp_path, {})

    assert not any(
        command[:2] == ["docker", "ps"] and "--all" in command
        for command in docker.commands
    )
