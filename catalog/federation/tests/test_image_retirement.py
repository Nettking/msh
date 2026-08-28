"""Superseded FCP images are reclaimed, in bounded work, touching nothing else.

Two properties are under test and they fail differently.

*Only the right images go.* Image deletion is real and irreversible on someone's
host, so most of what is asserted here is what a pass must **refuse** to do.

*One pass is finite work.* A bound on successful removals is not a bound on
work: a device with lifetime accumulation can hold thousands of dangling FCP
images that are all ineligible, and enumerating, inspecting and attempting
removal on all of them is work proportional to lifetime history inside an
ordinary build. Every Docker call is budgeted, and the tests count calls rather
than trusting the outcome counters.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation import image_retirement
from catalog.federation.image_retirement import (
    BUILD_COMMIT_LABEL,
    retire_superseded_images,
)

ACTIVE = "a" * 40
SUPERSEDED = "b" * 40

# Resolved through getattr so this module still *collects* against a tree whose
# retirement has no such budget. A test that fails with AttributeError proves
# nothing; the assertion has to fail on a real call count instead.
EXAMINE_BUDGET = getattr(image_retirement, "MAX_EXAMINED_IMAGES_PER_PASS", 32)
ATTEMPT_BUDGET = getattr(image_retirement, "MAX_REMOVAL_ATTEMPTS_PER_PASS", 16)


class _FakeDocker:
    """A daemon that answers exactly the queries the pass is allowed to make."""

    def __init__(
        self,
        *,
        dangling: list[tuple[str, str]],
        referenced: tuple[str, ...] = (),
        removable: bool = True,
        list_returncode: int = 0,
        ps_returncode: int = 0,
    ) -> None:
        self.dangling = dangling
        self.referenced = referenced
        self.removable = removable
        self.list_returncode = list_returncode
        self.ps_returncode = ps_returncode
        self.commands: list[list[str]] = []
        self.removed: list[str] = []

    # -- call counters the bounded-work tests assert on ------------------
    def _count(self, prefix: list[str]) -> int:
        return sum(1 for c in self.commands if c[: len(prefix)] == prefix)

    @property
    def inspects(self) -> int:
        return self._count(["docker", "image", "inspect"])

    @property
    def removals(self) -> int:
        return self._count(["docker", "image", "rm"])

    @property
    def listings(self) -> int:
        return self._count(["docker", "image", "ls"])

    def __call__(self, args, **_kwargs):
        args = list(args)
        self.commands.append(args)
        if args[:2] == ["docker", "ps"]:
            return SimpleNamespace(
                returncode=self.ps_returncode,
                stdout="\n".join(self.referenced),
                stderr="",
            )
        if args[:4] == ["docker", "image", "ls", "--filter"]:
            rows = "\n".join(
                json.dumps({"ID": image_id}) for image_id, _ in self.dangling
            )
            return SimpleNamespace(
                returncode=self.list_returncode, stdout=rows, stderr=""
            )
        if args[:3] == ["docker", "image", "inspect"]:
            wanted = args[3]
            for image_id, commit in self.dangling:
                if image_id == wanted:
                    return SimpleNamespace(returncode=0, stdout=commit, stderr="")
            return SimpleNamespace(returncode=1, stdout="", stderr="no such image")
        if args[:3] == ["docker", "image", "rm"]:
            if not self.removable:
                return SimpleNamespace(returncode=1, stdout="", stderr="image is in use")
            self.removed.append(args[3])
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        raise AssertionError(f"unexpected docker command: {args}")


def _install(monkeypatch, fake: _FakeDocker) -> None:
    monkeypatch.setattr(image_retirement.subprocess, "run", fake)


# ----------------------------------------------------------------------
# One pass is finite work, whatever the backlog looks like.
# ----------------------------------------------------------------------


def test_a_backlog_of_refused_images_cannot_make_one_pass_unbounded(
    monkeypatch, tmp_path: Path
) -> None:
    """The consequence.

    Every candidate here is eligible but the daemon refuses every removal, so a
    bound that counts only *successful* removals never advances. The pass must
    still stop: refusals cost real work and have to spend the budget.
    """

    fake = _FakeDocker(
        dangling=[(f"sha256:old{index}", SUPERSEDED) for index in range(500)],
        removable=False,
    )
    _install(monkeypatch, fake)

    outcome = retire_superseded_images(tmp_path, {}, active_commit=ACTIVE)

    assert outcome.retired == ()
    assert fake.removals <= ATTEMPT_BUDGET, (
        f"{fake.removals} removal attempts were made against a backlog of "
        f"{len(fake.dangling)} refused images"
    )
    assert fake.inspects <= EXAMINE_BUDGET, (
        f"{fake.inspects} images were inspected in one pass"
    )


def test_a_backlog_of_current_commit_images_cannot_make_one_pass_unbounded(
    monkeypatch, tmp_path: Path
) -> None:
    """Every candidate is skipped for carrying the activated commit, so no
    removal ever happens. Inspecting all of them is still unbounded work."""

    fake = _FakeDocker(
        dangling=[(f"sha256:cur{index}", ACTIVE) for index in range(500)]
    )
    _install(monkeypatch, fake)

    outcome = retire_superseded_images(tmp_path, {}, active_commit=ACTIVE)

    assert outcome.retired == ()
    assert fake.removals == 0
    assert fake.inspects <= EXAMINE_BUDGET, (
        f"{fake.inspects} images were inspected in one pass"
    )


def test_the_listing_itself_is_truncated_before_anything_is_inspected(
    monkeypatch, tmp_path: Path
) -> None:
    """A huge listing must not become a huge number of inspect calls."""

    fake = _FakeDocker(
        dangling=[(f"sha256:old{index}", SUPERSEDED) for index in range(5000)]
    )
    _install(monkeypatch, fake)

    retire_superseded_images(tmp_path, {}, active_commit=ACTIVE)

    assert fake.listings == 1
    assert fake.inspects <= EXAMINE_BUDGET, (
        f"{fake.inspects} images were inspected from a 5000-row listing"
    )


def test_referenced_images_do_not_even_cost_an_inspection(
    monkeypatch, tmp_path: Path
) -> None:
    """A local check that can never lead to a removal should not spend budget,
    and must not stop the pass reaching images it can actually retire."""

    dangling = [(f"sha256:ref{index}", SUPERSEDED) for index in range(20)]
    dangling.append(("sha256:removable", SUPERSEDED))
    fake = _FakeDocker(
        dangling=dangling,
        referenced=tuple(f"sha256:ref{index}" for index in range(20)),
    )
    _install(monkeypatch, fake)

    outcome = retire_superseded_images(tmp_path, {}, active_commit=ACTIVE)

    assert outcome.retired == ("sha256:removable",)
    assert fake.inspects == 1, (
        f"{fake.inspects} inspections were spent on images that could never be "
        "removed because a container still references them"
    )


def test_repeated_passes_make_monotonic_progress(monkeypatch, tmp_path: Path) -> None:
    class _Draining(_FakeDocker):
        def __call__(self, args, **kwargs):
            result = super().__call__(args, **kwargs)
            if list(args)[:3] == ["docker", "image", "rm"] and self.removable:
                self.dangling = [e for e in self.dangling if e[0] != list(args)[3]]
            return result

    fake = _Draining(
        dangling=[(f"sha256:old{index}", SUPERSEDED) for index in range(10)]
    )
    _install(monkeypatch, fake)

    seen: list[int] = []
    for _ in range(5):
        retire_superseded_images(
            tmp_path, {}, active_commit=ACTIVE, attempt_limit=3, examined_limit=3
        )
        seen.append(len(fake.dangling))

    assert seen == [7, 4, 1, 0, 0]


# ----------------------------------------------------------------------
# Only the right images go.
# ----------------------------------------------------------------------


def test_a_superseded_fcp_image_is_reclaimed(monkeypatch, tmp_path: Path) -> None:
    fake = _FakeDocker(dangling=[("sha256:old", SUPERSEDED)])
    _install(monkeypatch, fake)

    outcome = retire_superseded_images(tmp_path, {}, active_commit=ACTIVE)

    assert outcome.retired == ("sha256:old",)
    assert fake.removed == ["sha256:old"]
    assert outcome.freed_any is True


def test_the_image_the_device_is_running_is_never_a_candidate(
    monkeypatch, tmp_path: Path
) -> None:
    fake = _FakeDocker(dangling=[("sha256:current", ACTIVE)])
    _install(monkeypatch, fake)

    assert retire_superseded_images(tmp_path, {}, active_commit=ACTIVE).retired == ()
    assert fake.removed == []


def test_an_image_a_container_still_references_is_left_alone(
    monkeypatch, tmp_path: Path
) -> None:
    fake = _FakeDocker(
        dangling=[("sha256:old", SUPERSEDED)], referenced=("sha256:old",)
    )
    _install(monkeypatch, fake)

    assert retire_superseded_images(tmp_path, {}, active_commit=ACTIVE).retired == ()
    assert fake.removed == []


def test_nothing_is_removed_when_the_reference_set_cannot_be_established(
    monkeypatch, tmp_path: Path
) -> None:
    """An unknown set of referenced images is not an empty one."""

    fake = _FakeDocker(dangling=[("sha256:old", SUPERSEDED)], ps_returncode=1)
    _install(monkeypatch, fake)

    assert retire_superseded_images(tmp_path, {}, active_commit=ACTIVE).retired == ()
    assert fake.removed == []


def test_nothing_is_removed_when_the_candidate_listing_fails(
    monkeypatch, tmp_path: Path
) -> None:
    fake = _FakeDocker(dangling=[("sha256:old", SUPERSEDED)], list_returncode=1)
    _install(monkeypatch, fake)

    assert retire_superseded_images(tmp_path, {}, active_commit=ACTIVE).retired == ()
    assert fake.removed == []


def test_an_unknown_active_commit_removes_nothing(monkeypatch, tmp_path: Path) -> None:
    fake = _FakeDocker(dangling=[("sha256:old", SUPERSEDED)])
    _install(monkeypatch, fake)

    assert retire_superseded_images(tmp_path, {}, active_commit="  ").retired == ()
    assert fake.removed == []


def test_an_image_whose_label_cannot_be_read_is_left_alone(
    monkeypatch, tmp_path: Path
) -> None:
    """An unreadable identity is not permission to delete."""

    fake = _FakeDocker(dangling=[("sha256:absent", SUPERSEDED)])
    fake.dangling = [("sha256:absent", SUPERSEDED)]
    _install(monkeypatch, fake)
    monkeypatch.setattr(
        image_retirement, "_image_build_commit", lambda *_a, **_k: None
    )

    assert retire_superseded_images(tmp_path, {}, active_commit=ACTIVE).retired == ()
    assert fake.removed == []


def test_only_dangling_fcp_labelled_images_are_ever_listed(
    monkeypatch, tmp_path: Path
) -> None:
    fake = _FakeDocker(dangling=[])
    _install(monkeypatch, fake)

    retire_superseded_images(tmp_path, {}, active_commit=ACTIVE)

    listing = [c for c in fake.commands if c[:3] == ["docker", "image", "ls"]]
    assert len(listing) == 1
    assert "dangling=true" in listing[0]
    assert f"label={BUILD_COMMIT_LABEL}" in listing[0]


def test_no_prune_or_volume_operation_is_ever_issued(
    monkeypatch, tmp_path: Path
) -> None:
    """The forbidden operations, asserted rather than assumed."""

    fake = _FakeDocker(
        dangling=[(f"sha256:old{index}", SUPERSEDED) for index in range(5)]
    )
    _install(monkeypatch, fake)

    retire_superseded_images(tmp_path, {}, active_commit=ACTIVE)

    for command in fake.commands:
        issued = " ".join(command)
        assert "prune" not in issued, issued
        assert "volume" not in issued, issued
        assert "system" not in issued, issued
        assert "--force" not in command
        assert "-f" not in command
        if command[:3] == ["docker", "image", "rm"]:
            assert len(command) == 4
            assert not command[3].startswith("-")


def test_a_daemon_refusal_is_recorded_rather_than_forced(
    monkeypatch, tmp_path: Path
) -> None:
    fake = _FakeDocker(dangling=[("sha256:old", SUPERSEDED)], removable=False)
    _install(monkeypatch, fake)

    outcome = retire_superseded_images(tmp_path, {}, active_commit=ACTIVE)

    assert outcome.retired == ()
    assert outcome.refused == ("sha256:old",)


def test_a_docker_failure_never_raises_into_the_build(
    monkeypatch, tmp_path: Path
) -> None:
    """Reclaiming disk must not turn an accepted build into a failed one."""

    def exploding(*_args, **_kwargs):
        raise subprocess.SubprocessError("daemon gone")

    monkeypatch.setattr(image_retirement.subprocess, "run", exploding)

    outcome = retire_superseded_images(tmp_path, {}, active_commit=ACTIVE)

    assert outcome.retired == ()


def test_retirement_runs_only_after_the_transition_is_verified(
    monkeypatch, tmp_path: Path
) -> None:
    """New-behaviour coverage, not a consequence test: it pins that retirement
    is handed the activated commit and runs after the build, its cache
    lifecycle, the image-identity proof and the post-build source proof."""

    from catalog.federation import host_build

    order: list[str] = []
    monkeypatch.setattr(host_build, "resolve_clean_commit", lambda _root: ACTIVE)
    monkeypatch.setattr(host_build, "preflight_disk", lambda *_a, **_k: None)
    monkeypatch.setattr(
        host_build,
        "controlled_core_build",
        lambda *_a, **_k: order.append("build"),
    )
    monkeypatch.setattr(
        host_build,
        "_verify_core_image_commits",
        lambda *_a, **_k: order.append("verify-identity"),
    )
    monkeypatch.setattr(
        host_build,
        "retire_superseded_images",
        lambda _root, _env, *, active_commit: order.append(f"retire:{active_commit}"),
        raising=False,
    )

    assert host_build.build_core_images_locked(tmp_path, {}) == ACTIVE

    assert order == ["build", "verify-identity", f"retire:{ACTIVE}"]


@pytest.mark.parametrize("budget", [0, 1, 5])
def test_the_budget_is_honoured_at_its_edges(
    monkeypatch, tmp_path: Path, budget: int
) -> None:
    """New-API coverage for the explicit budget parameters, not a consequence
    test: it uses keyword arguments that only this implementation accepts."""

    fake = _FakeDocker(
        dangling=[(f"sha256:old{index}", SUPERSEDED) for index in range(40)]
    )
    _install(monkeypatch, fake)

    outcome = retire_superseded_images(
        tmp_path,
        {},
        active_commit=ACTIVE,
        listed_limit=40,
        examined_limit=budget,
        attempt_limit=budget,
    )

    assert len(outcome.retired) == budget
    assert fake.removals == budget
    assert fake.inspects == budget


# ----------------------------------------------------------------------
# The "never raises into the build" contract, held against answers the
# daemon can actually give. Reclaiming disk is best effort; an accepted
# build must survive a cleanup pass that cannot make sense of anything.
# ----------------------------------------------------------------------


def test_a_listing_row_that_parses_but_is_not_an_object_never_raises(
    monkeypatch, tmp_path: Path
) -> None:
    """A non-object row is as unreadable as an unparseable one, not a crash.

    ``{{json .}}`` is expected to emit one object per row. When something emits
    anything else, reading ``.get`` off it raised ``AttributeError`` straight
    out of a best-effort cleanup and failed a build that had already been
    verified and accepted.
    """

    def answer(args, **_kwargs):
        args = list(args)
        if args[:2] == ["docker", "ps"]:
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if args[:3] == ["docker", "image", "ls"]:
            return SimpleNamespace(
                returncode=0, stdout='["not", "an", "object"]\n', stderr=""
            )
        raise AssertionError(f"nothing may be examined or removed: {args}")

    monkeypatch.setattr(image_retirement.subprocess, "run", answer)

    outcome = retire_superseded_images(tmp_path, {}, active_commit=ACTIVE)

    assert outcome.retired == ()
    assert outcome.listed == 0


def test_output_that_is_not_decodable_text_never_raises(
    monkeypatch, tmp_path: Path
) -> None:
    """``text=True`` raises ``UnicodeDecodeError`` on bytes that are not UTF-8.

    That is a ``ValueError``, neither an ``OSError`` nor a
    ``SubprocessError`` -- so it used to propagate out of the pass and fail the
    build. It has to be answered the same way every other unanswerable query
    is: decide nothing, remove nothing, let the build stand.
    """

    def undecodable(*_args, **_kwargs):
        b"\xff\xfe".decode("utf-8")

    monkeypatch.setattr(image_retirement.subprocess, "run", undecodable)

    outcome = retire_superseded_images(tmp_path, {}, active_commit=ACTIVE)

    assert outcome.retired == ()
    assert outcome.refused == ()


# ----------------------------------------------------------------------
# The call budget bounds the work. It does not bound the time, and this
# pass runs inside the host-mutation lock.
# ----------------------------------------------------------------------


def test_one_pass_is_bounded_in_time_and_not_only_in_calls(
    monkeypatch, tmp_path: Path
) -> None:
    """A slow daemon must not hold the host-mutation lock for the call budget.

    Every call is allowed ``DOCKER_TIMEOUT_SECONDS``, so the call budget alone
    permits a pass lasting the better part of an hour -- after an accepted
    build, inside the lock every launcher and update on the host waits behind.
    The pass stops at its own deadline instead and leaves the rest for the next
    one, which is the same property that already makes progress monotonic.
    """

    clock = {"now": 0.0}
    monkeypatch.setattr(image_retirement.time, "monotonic", lambda: clock["now"])

    fake = _FakeDocker(
        dangling=[(f"sha256:{index:04d}", SUPERSEDED) for index in range(64)]
    )
    slow = fake.__call__

    def ticking(args, **kwargs):
        # Each answer costs a second of the pass's wall clock.
        clock["now"] += 1.0
        return slow(args, **kwargs)

    monkeypatch.setattr(image_retirement.subprocess, "run", ticking)

    outcome = retire_superseded_images(
        tmp_path, {}, active_commit=ACTIVE, pass_seconds=6.0
    )

    spent = fake.inspects + fake.removals
    assert clock["now"] <= 8.0, (
        f"the pass ran for {clock['now']}s against a 6s budget"
    )
    assert spent < EXAMINE_BUDGET + ATTEMPT_BUDGET, (
        f"{spent} Docker calls were made after the pass deadline had passed"
    )
    # Whatever it did reach is still real, committed work.
    assert len(outcome.retired) == len(fake.removed)


def test_a_pass_with_no_time_left_decides_nothing(
    monkeypatch, tmp_path: Path
) -> None:
    """An exhausted deadline is an unanswerable query, so it removes nothing."""

    fake = _FakeDocker(dangling=[("sha256:aaa", SUPERSEDED)])
    _install(monkeypatch, fake)

    outcome = retire_superseded_images(
        tmp_path, {}, active_commit=ACTIVE, pass_seconds=0.0
    )

    assert outcome.retired == ()
    assert fake.removed == []
    assert fake.commands == []
