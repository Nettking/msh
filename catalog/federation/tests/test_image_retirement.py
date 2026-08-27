"""Superseded FCP images are reclaimed; nothing else is ever touched.

The bullet this closes is B07's "superseded unused FCP images after verified
activation/start transition". The risk it carries is that image deletion is a
real, irreversible operation on someone's host, so most of what is asserted
here is what the retirement pass must *refuse* to do.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

from catalog.federation import image_retirement
from catalog.federation.image_retirement import (
    BUILD_COMMIT_LABEL,
    retire_superseded_images,
)

ACTIVE = "a" * 40
SUPERSEDED = "b" * 40


class _FakeDocker:
    """A daemon that answers the three queries the pass is allowed to make."""

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
                return SimpleNamespace(
                    returncode=1, stdout="", stderr="image is in use"
                )
            self.removed.append(args[3])
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        raise AssertionError(f"unexpected docker command: {args}")


def _install(monkeypatch, fake: _FakeDocker) -> None:
    monkeypatch.setattr(image_retirement.subprocess, "run", fake)


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
    """The commit just activated identifies the live image, whatever its tags."""

    fake = _FakeDocker(dangling=[("sha256:current", ACTIVE)])
    _install(monkeypatch, fake)

    outcome = retire_superseded_images(tmp_path, {}, active_commit=ACTIVE)

    assert outcome.retired == ()
    assert fake.removed == []


def test_an_image_a_container_still_references_is_left_alone(
    monkeypatch, tmp_path: Path
) -> None:
    fake = _FakeDocker(
        dangling=[("sha256:old", SUPERSEDED)],
        referenced=("sha256:old",),
    )
    _install(monkeypatch, fake)

    outcome = retire_superseded_images(tmp_path, {}, active_commit=ACTIVE)

    assert outcome.retired == ()
    assert fake.removed == []


def test_nothing_is_removed_when_the_reference_set_cannot_be_established(
    monkeypatch, tmp_path: Path
) -> None:
    """An unknown set of referenced images is not an empty one."""

    fake = _FakeDocker(dangling=[("sha256:old", SUPERSEDED)], ps_returncode=1)
    _install(monkeypatch, fake)

    outcome = retire_superseded_images(tmp_path, {}, active_commit=ACTIVE)

    assert outcome.retired == ()
    assert fake.removed == []


def test_nothing_is_removed_when_the_candidate_listing_fails(
    monkeypatch, tmp_path: Path
) -> None:
    fake = _FakeDocker(dangling=[("sha256:old", SUPERSEDED)], list_returncode=1)
    _install(monkeypatch, fake)

    assert retire_superseded_images(tmp_path, {}, active_commit=ACTIVE).retired == ()
    assert fake.removed == []


def test_an_unknown_active_commit_removes_nothing(monkeypatch, tmp_path: Path) -> None:
    """Without the activated identity there is no way to tell current from old."""

    fake = _FakeDocker(dangling=[("sha256:old", SUPERSEDED)])
    _install(monkeypatch, fake)

    assert retire_superseded_images(tmp_path, {}, active_commit="  ").retired == ()
    assert fake.removed == []


def test_only_dangling_fcp_labelled_images_are_ever_listed(
    monkeypatch, tmp_path: Path
) -> None:
    """The narrowing is done by the daemon, so an unrelated project's untagged
    image is never even considered as a candidate."""

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

    fake = _FakeDocker(dangling=[("sha256:old", SUPERSEDED)])
    _install(monkeypatch, fake)

    retire_superseded_images(tmp_path, {}, active_commit=ACTIVE)

    flat = [" ".join(command) for command in fake.commands]
    for issued in flat:
        assert "prune" not in issued, issued
        assert "volume" not in issued, issued
        assert "system" not in issued, issued
    # Every removal names one image id explicitly.
    for command in fake.commands:
        if command[:3] == ["docker", "image", "rm"]:
            assert len(command) == 4
            assert not command[3].startswith("-")


def test_a_daemon_refusal_is_accepted_rather_than_forced(
    monkeypatch, tmp_path: Path
) -> None:
    fake = _FakeDocker(dangling=[("sha256:old", SUPERSEDED)], removable=False)
    _install(monkeypatch, fake)

    outcome = retire_superseded_images(tmp_path, {}, active_commit=ACTIVE)

    assert outcome.retired == ()
    assert outcome.refused == ("sha256:old",)
    for command in fake.commands:
        assert "--force" not in command
        assert "-f" not in command


def test_one_pass_removes_at_most_the_configured_batch(
    monkeypatch, tmp_path: Path
) -> None:
    """Cleanup must not become work proportional to how long the device has
    been updated."""

    fake = _FakeDocker(
        dangling=[(f"sha256:old{index}", SUPERSEDED) for index in range(50)]
    )
    _install(monkeypatch, fake)

    outcome = retire_superseded_images(tmp_path, {}, active_commit=ACTIVE, limit=4)

    assert len(outcome.retired) == 4
    assert len(fake.removed) == 4
    assert outcome.examined == 50


def test_repeated_passes_make_monotonic_progress(monkeypatch, tmp_path: Path) -> None:
    remaining = [(f"sha256:old{index}", SUPERSEDED) for index in range(10)]

    class _Draining(_FakeDocker):
        def __call__(self, args, **kwargs):
            result = super().__call__(args, **kwargs)
            if list(args)[:3] == ["docker", "image", "rm"]:
                self.dangling = [
                    entry for entry in self.dangling if entry[0] != list(args)[3]
                ]
            return result

    fake = _Draining(dangling=remaining)
    _install(monkeypatch, fake)

    seen: list[int] = []
    for _ in range(5):
        retire_superseded_images(tmp_path, {}, active_commit=ACTIVE, limit=3)
        seen.append(len(fake.dangling))

    assert seen == [7, 4, 1, 0, 0]


def test_a_docker_failure_never_raises_into_the_build(
    monkeypatch, tmp_path: Path
) -> None:
    """Reclaiming disk must not turn an accepted build into a failed one."""

    def exploding(*_args, **_kwargs):
        raise subprocess.SubprocessError("daemon gone")

    monkeypatch.setattr(image_retirement.subprocess, "run", exploding)

    outcome = retire_superseded_images(tmp_path, {}, active_commit=ACTIVE)

    assert outcome.retired == ()
    assert outcome.examined == 0


def test_a_verified_build_transition_reclaims_its_superseded_images(
    monkeypatch, tmp_path: Path
) -> None:
    """New behaviour, wired at the one point where the transition is verified.

    This is not a consequence-level test: on a tree without this module the
    file cannot import at all, which proves nothing. What it pins is that the
    retirement runs only after the build succeeded, its cache lifecycle
    completed and the source identity re-proved -- and that it is handed the
    commit that was actually activated.
    """

    from catalog.federation import host_build

    retirements: list[str] = []
    monkeypatch.setattr(
        host_build,
        "retire_superseded_images",
        lambda _root, _env, *, active_commit: retirements.append(active_commit),
        raising=False,
    )
    monkeypatch.setattr(host_build, "resolve_clean_commit", lambda _root: ACTIVE)
    monkeypatch.setattr(host_build, "preflight_disk", lambda *_a, **_k: None)
    monkeypatch.setattr(host_build, "prune_build_cache", lambda *_a, **_k: True)
    monkeypatch.setattr(
        host_build.subprocess,
        "run",
        lambda *_a, **_k: SimpleNamespace(returncode=0, stdout="", stderr=""),
    )

    assert host_build.build_core_images_locked(tmp_path, {}) == ACTIVE

    assert retirements == [ACTIVE], (
        "a verified build transition left its superseded images in place"
    )
