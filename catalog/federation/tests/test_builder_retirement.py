"""The ownership frontier and bounded progress of stranded-builder retirement."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation import builder_retirement

ROOT = Path(__file__).resolve().parents[3]
PREFIX = "fcp-build-"
CONTAINER_PREFIX = "buildx_buildkit_"
CURRENT = "fcp-build-current"


@pytest.fixture(autouse=True)
def _isolated_cursor_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FCP_BUILDER_RETIREMENT_STATE_DIR", str(tmp_path / "cursor"))


def _stamp(path: str) -> str:
    return f"{builder_retirement.BUILDER_ROOT_ENV}={path.encode('utf-8').hex()}"


class _Docker:
    """Small Docker model whose insertion order is newest-to-oldest."""

    def __init__(self, builders: dict[str, list[str] | None]) -> None:
        self.builders = dict(builders)
        self.inspected: list[str] = []
        self.commands: list[list[str]] = []
        self.ids = {
            name: f"{index + 1:064x}" for index, name in enumerate(self.builders)
        }

    def run(self, args):
        command = list(args)
        self.commands.append(command)
        if command[:2] == ["docker", "ps"] and "--all" in command:
            candidates = [
                name
                for name, environment in self.builders.items()
                if environment is not None and name.startswith(PREFIX)
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
            stdout = "".join(
                f"{self.ids[name]} {CONTAINER_PREFIX}{name}0\n" for name in candidates
            )
            return SimpleNamespace(returncode=0, stdout=stdout, stderr="")
        if command[:2] == ["docker", "inspect"]:
            name = command[-1].removeprefix(CONTAINER_PREFIX).removesuffix("0")
            self.inspected.append(name)
            environment = self.builders.get(name)
            if environment is None:
                return SimpleNamespace(returncode=1, stdout="", stderr="no such object")
            return SimpleNamespace(
                returncode=0, stdout="\n".join(environment) + "\n", stderr=""
            )
        raise AssertionError(f"unexpected command: {command}")


def _retire(docker: _Docker, *, running=(), removable=True):
    removed: list[str] = []

    def remove(name: str) -> bool:
        if not removable:
            return False
        removed.append(name)
        return True

    outcome = builder_retirement.retire_stranded_builders(
        current_builder=CURRENT,
        builder_prefix=PREFIX,
        container_prefix=CONTAINER_PREFIX,
        docker_run=docker.run,
        container_running=lambda name: name in running,
        remove_builder=remove,
    )
    return outcome, removed


def test_the_owner_stamp_survives_a_path_buildx_would_otherwise_split() -> None:
    awkward = Path("/srv/Machines, Tools & Parts/fcp")
    option = builder_retirement.builder_root_driver_opt(awkward)

    assert "," not in option
    assert option.startswith(f"env.{builder_retirement.BUILDER_ROOT_ENV}=")
    assert builder_retirement.stamped_root([option.removeprefix("env.")]) == awkward


def test_an_undecodable_or_absent_stamp_yields_no_owner() -> None:
    env = builder_retirement.BUILDER_ROOT_ENV
    assert builder_retirement.stamped_root([]) is None
    assert builder_retirement.stamped_root(["PATH=/usr/bin"]) is None
    assert builder_retirement.stamped_root([f"{env}="]) is None
    assert builder_retirement.stamped_root([f"{env}=zz"]) is None
    assert builder_retirement.stamped_root([f"{env}=616"]) is None
    assert builder_retirement.stamped_root([f"{env}=ff"]) is None


def test_a_failed_enumeration_removes_nothing() -> None:
    def failing(_args):
        return SimpleNamespace(returncode=1, stdout="", stderr="daemon down")

    outcome = builder_retirement.retire_stranded_builders(
        current_builder=CURRENT,
        builder_prefix=PREFIX,
        container_prefix=CONTAINER_PREFIX,
        docker_run=failing,
        container_running=lambda _name: False,
        remove_builder=lambda _name: True,
    )

    assert outcome == builder_retirement.BuilderRetirementOutcome()


def test_only_a_stamped_owner_that_is_gone_licenses_removal(tmp_path: Path) -> None:
    present = tmp_path / "still-here"
    present.mkdir()
    gone = str(tmp_path / "removed")
    docker = _Docker(
        {
            CURRENT: [_stamp(str(tmp_path))],
            "fcp-build-gone": [_stamp(gone)],
            "fcp-build-live-sibling": [_stamp(str(present))],
            "fcp-build-unstamped": ["PATH=/usr/bin"],
            "fcp-build-no-container": None,
            "unrelated-builder": [_stamp(gone)],
        }
    )

    outcome, removed = _retire(docker)

    assert removed == ["fcp-build-gone"]
    assert outcome.retired == ("fcp-build-gone",)
    assert set(outcome.refused) == {"fcp-build-live-sibling", "fcp-build-unstamped"}
    assert CURRENT not in docker.inspected
    assert "unrelated-builder" not in docker.inspected
    assert "fcp-build-no-container" not in docker.inspected


def test_a_running_writer_is_refused_even_with_its_checkout_gone(tmp_path: Path) -> None:
    gone = str(tmp_path / "removed")
    docker = _Docker({"fcp-build-live": [_stamp(gone)]})

    outcome, removed = _retire(docker, running=("fcp-build-live",))

    assert removed == []
    assert outcome.refused == ("fcp-build-live",)


def test_a_removal_that_fails_is_reported_as_refused(tmp_path: Path) -> None:
    gone = str(tmp_path / "removed")
    docker = _Docker({"fcp-build-gone": [_stamp(gone)]})

    outcome, _removed = _retire(docker, removable=False)

    assert outcome.retired == ()
    assert outcome.refused == ("fcp-build-gone",)
    assert outcome.attempted == 1


def test_one_pass_is_bounded_before_output_reaches_python(tmp_path: Path) -> None:
    gone = str(tmp_path / "removed")
    docker = _Docker({f"fcp-build-{index:03d}": [_stamp(gone)] for index in range(200)})

    outcome, removed = _retire(docker)

    listing = docker.commands[0]
    assert listing[:2] == ["docker", "ps"]
    assert "--all" in listing
    assert listing[listing.index("--last") + 1] == str(
        builder_retirement.MAX_LISTED_BUILDERS_PER_PASS
    )
    assert f"name={CONTAINER_PREFIX}{PREFIX}" in listing
    assert outcome.listed == builder_retirement.MAX_LISTED_BUILDERS_PER_PASS
    # Every candidate in this fixture is removable, so each examination leads to
    # an attempt and the removal budget binds strictly first. Asserting that
    # examination also reaches its own ceiling asks for something this fixture
    # cannot produce: the pass stops examining once it may no longer remove,
    # rather than spending inspections on builders it cannot act on. The bound
    # is a ceiling, and which ceiling binds depends on what the pass finds.
    assert outcome.attempted == builder_retirement.MAX_REMOVAL_ATTEMPTS_PER_PASS
    assert outcome.examined <= builder_retirement.MAX_EXAMINED_BUILDERS_PER_PASS
    # One examination past the last attempt: the iteration that exhausts the
    # removal budget has already been counted before the loop breaks.
    assert outcome.examined == outcome.attempted + 1
    assert len(removed) == builder_retirement.MAX_REMOVAL_ATTEMPTS_PER_PASS


def test_unrelated_builders_do_not_occupy_the_server_side_page(tmp_path: Path) -> None:
    gone = str(tmp_path / "removed")
    builders: dict[str, list[str] | None] = {
        f"other-{index:03d}": [] for index in range(100)
    }
    builders["fcp-build-gone"] = [_stamp(gone)]
    docker = _Docker(builders)

    outcome, removed = _retire(docker)

    assert removed == ["fcp-build-gone"]
    assert outcome.listed == 1
    assert outcome.examined == 1


def test_cursor_advances_past_a_full_page_of_permanent_refusals(tmp_path: Path) -> None:
    present = tmp_path / "present"
    present.mkdir()
    gone = str(tmp_path / "removed")
    builders: dict[str, list[str] | None] = {
        f"fcp-build-live-{index:03d}": [_stamp(str(present))]
        for index in range(builder_retirement.MAX_LISTED_BUILDERS_PER_PASS)
    }
    builders["fcp-build-old-orphan"] = [_stamp(gone)]
    docker = _Docker(builders)

    first, first_removed = _retire(docker)
    second, second_removed = _retire(docker)

    assert first_removed == []
    assert first.listed == builder_retirement.MAX_LISTED_BUILDERS_PER_PASS
    assert second_removed == ["fcp-build-old-orphan"]
    assert second.retired == ("fcp-build-old-orphan",)
    second_listing = [command for command in docker.commands if command[:2] == ["docker", "ps"]][1]
    assert any(item.startswith("before=") for item in second_listing)


def test_the_windows_path_decides_the_same_way() -> None:
    windows = (ROOT / "scripts/windows/fcp_host_build.ps1").read_text(encoding="utf-8")

    assert f"$BuilderRootEnv = '{builder_retirement.BUILDER_ROOT_ENV}'" in windows
    assert (
        f"$MaxListedBuildersPerPass = {builder_retirement.MAX_LISTED_BUILDERS_PER_PASS}"
        in windows
    )
    assert (
        "$MaxExaminedBuildersPerPass = "
        f"{builder_retirement.MAX_EXAMINED_BUILDERS_PER_PASS}" in windows
    )
    assert (
        "$MaxRemovalAttemptsPerPass = "
        f"{builder_retirement.MAX_REMOVAL_ATTEMPTS_PER_PASS}" in windows
    )

    start = windows.index("function Invoke-FcpStrandedBuilderRetirement")
    retirement = windows[start : windows.index("function Ensure-FcpControllableBuilder")]
    assert "'ps', '--all', '--no-trunc', '--last'" in retirement
    assert "('name=' + $BuildKitContainerPrefix + $BuilderPrefix)" in retirement
    assert "('before=' + $cursor)" in retirement
    assert "Set-FcpBuilderRetirementCursor" in retirement
    assert "if ([string]::IsNullOrWhiteSpace($owner)) { continue }" in retirement
    assert "Test-Path -LiteralPath $owner -ErrorAction Stop" in retirement
    assert "catch { $ownerExists = $true }" in retirement
    assert "if ($ownerExists) { continue }" in retirement
    assert "if (Test-FcpBuildKitContainerRunning $name) { continue }" in retirement


def test_the_windows_builder_carries_the_owner_stamp_and_is_reclaimed_under_pressure() -> None:
    windows = (ROOT / "scripts/windows/fcp_host_build.ps1").read_text(encoding="utf-8")
    create = windows[
        windows.index("function Ensure-FcpControllableBuilder") : windows.index(
            "function Invoke-BuildCachePrune"
        )
    ]
    assert "'--driver-opt', (Get-FcpBuilderRootDriverOpt)" in create

    preflight = windows[
        windows.index("function Assert-DiskPreflight") : windows.index(
            "function Write-AtomicText"
        )
    ]
    assert (
        preflight.index("$level -in @('normal', 'warning')")
        < preflight.index("build_cache_discard_failed")
        < preflight.index("Invoke-FcpStrandedBuilderRetirement")
        < preflight.rindex("Get-FcpResourceFreeBytes")
        < preflight.index("insufficient_disk_for_update")
    )


def test_the_windows_stamp_reader_refuses_what_it_cannot_decode() -> None:
    windows = (ROOT / "scripts/windows/fcp_host_build.ps1").read_text(encoding="utf-8")
    start = windows.index("function Get-FcpBuilderStampedRoot")
    reader = windows[start : windows.index("function Invoke-FcpStrandedBuilderRetirement")]

    assert "New-Object System.Text.UTF8Encoding($false, $true)" in reader
    assert "$strict.GetString($bytes)" in reader
    assert "[System.Text.Encoding]::UTF8.GetString($bytes)" not in reader
    assert "catch { return '' }" in reader

    opt = windows[
        windows.index("function Get-FcpBuilderRootDriverOpt") : windows.index(
            "function Get-FcpBuilderStampedRoot"
        )
    ]
    assert "Normalize-DirectoryPath $RepoRoot" in opt
    assert "GetBytes($RepoRoot)" not in opt

    undecodable = b"\xff\xfe/gone".hex()
    assert (
        builder_retirement.stamped_root(
            [f"{builder_retirement.BUILDER_ROOT_ENV}={undecodable}"]
        )
        is None
    )
