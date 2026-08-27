"""Retire FCP's own superseded core images after a verified build transition.

Every core build produces new ``relay``/``flask``/``recorder`` images and moves
the Compose tags onto them. The images those tags used to point at keep every
layer they own and lose their only name, and nothing in the product has ever
removed one. They accumulate once per update, for the life of the device, on
the very Docker backing resource B01 admits builds against.

The retirement rule here is deliberately the narrowest one that can free that
space, because the operation is a real deletion on someone's host:

* the image must be **dangling** -- it has no repository tag at all, so no
  Compose service, no ``docker run`` and no human reference can name it;
* it must carry FCP's own ``no.fcp.build_commit`` label, so it is provably
  something an FCP build produced rather than an unrelated project's layer;
* its build commit must differ from the commit that was just activated, so the
  image the device is now running is never a candidate;
* no container, running or stopped, may reference it.

Nothing else is ever removed. There is no ``docker system prune``, no
``image prune``, no volume deletion and no untagged-image sweep: each candidate
is removed by its own id, one at a time, and a refusal from the daemon is
accepted rather than forced.
"""

from __future__ import annotations

import json
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

#: Label written by both FCP Dockerfiles, carrying the exact source identity.
BUILD_COMMIT_LABEL = "no.fcp.build_commit"

#: Images one retirement pass may remove.
#
# Cleanup must not become unbounded foreground work merely because a device has
# been updated many times. A pass removes at most this many and returns; the
# next verified transition continues, so progress is monotonic across passes
# and no single pass is proportional to lifetime history.
MAX_RETIRED_IMAGES_PER_PASS = 16

DOCKER_TIMEOUT_SECONDS = 60.0


@dataclass(frozen=True)
class ImageRetirementOutcome:
    """What one bounded retirement pass actually did."""

    retired: tuple[str, ...]
    refused: tuple[str, ...]
    examined: int

    @property
    def freed_any(self) -> bool:
        return bool(self.retired)


def _docker(
    root: Path,
    args: Sequence[str],
    env: Mapping[str, str],
) -> subprocess.CompletedProcess[str] | None:
    """Run one bounded Docker query. ``None`` means it could not be answered."""

    try:
        return subprocess.run(
            list(args),
            cwd=root,
            env=dict(env),
            shell=False,
            check=False,
            capture_output=True,
            text=True,
            timeout=DOCKER_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError):
        return None


def _referenced_image_ids(root: Path, env: Mapping[str, str]) -> set[str] | None:
    """Every image id referenced by a container, running or not.

    ``None`` when the set cannot be established, which makes the caller refuse
    to remove anything: an unknown reference set is not an empty one.
    """

    completed = _docker(
        root,
        ["docker", "ps", "--all", "--no-trunc", "--format", "{{.Image}}"],
        env,
    )
    if completed is None or completed.returncode != 0:
        return None
    return {line.strip() for line in completed.stdout.splitlines() if line.strip()}


def _dangling_fcp_images(
    root: Path,
    env: Mapping[str, str],
) -> list[tuple[str, str]] | None:
    """Return ``(image_id, build_commit)`` for dangling FCP-built images.

    Filtering on both ``dangling=true`` and FCP's own label is done by the
    daemon, so an image that is merely untagged but not FCP's is never even
    considered here.
    """

    completed = _docker(
        root,
        [
            "docker",
            "image",
            "ls",
            "--filter",
            "dangling=true",
            "--filter",
            f"label={BUILD_COMMIT_LABEL}",
            "--no-trunc",
            "--format",
            "{{json .}}",
        ],
        env,
    )
    if completed is None or completed.returncode != 0:
        return None

    candidates: list[tuple[str, str]] = []
    for line in completed.stdout.splitlines():
        text = line.strip()
        if not text:
            continue
        try:
            record = json.loads(text)
        except ValueError:
            # An unreadable row is not evidence that anything is safe to
            # delete, so the whole pass refuses rather than guessing.
            return None
        image_id = str(record.get("ID") or "").strip()
        if not image_id:
            return None
        commit = _image_build_commit(root, image_id, env)
        if commit is None:
            return None
        candidates.append((image_id, commit))
    return candidates


def _image_build_commit(
    root: Path,
    image_id: str,
    env: Mapping[str, str],
) -> str | None:
    """Read one image's recorded build commit, or ``None`` if unreadable."""

    completed = _docker(
        root,
        [
            "docker",
            "image",
            "inspect",
            image_id,
            "--format",
            f"{{{{index .Config.Labels \"{BUILD_COMMIT_LABEL}\"}}}}",
        ],
        env,
    )
    if completed is None or completed.returncode != 0:
        return None
    return completed.stdout.strip()


def retire_superseded_images(
    root: Path,
    env: Mapping[str, str],
    *,
    active_commit: str,
    limit: int = MAX_RETIRED_IMAGES_PER_PASS,
) -> ImageRetirementOutcome:
    """Remove superseded FCP images after a verified build/activation transition.

    Safe to call when nothing needs retiring, and safe to call again after a
    partial pass. Never raises: reclaiming disk is best effort, and a device
    that cannot free space must still finish the update that triggered this.
    """

    if not active_commit.strip():
        # Without the identity that was just activated there is no way to tell
        # the current image from a superseded one.
        return ImageRetirementOutcome(retired=(), refused=(), examined=0)

    referenced = _referenced_image_ids(root, env)
    if referenced is None:
        return ImageRetirementOutcome(retired=(), refused=(), examined=0)

    candidates = _dangling_fcp_images(root, env)
    if candidates is None:
        return ImageRetirementOutcome(retired=(), refused=(), examined=0)

    active = active_commit.strip().lower()
    retired: list[str] = []
    refused: list[str] = []
    for image_id, commit in candidates:
        if len(retired) >= max(int(limit), 0):
            break
        if not commit or commit.strip().lower() == active:
            continue
        if image_id in referenced:
            continue
        completed = _docker(root, ["docker", "image", "rm", image_id], env)
        if completed is None or completed.returncode != 0:
            # The daemon refusing is a correct outcome, not something to force.
            refused.append(image_id)
            continue
        retired.append(image_id)

    return ImageRetirementOutcome(
        retired=tuple(retired),
        refused=tuple(refused),
        examined=len(candidates),
    )


__all__ = [
    "BUILD_COMMIT_LABEL",
    "MAX_RETIRED_IMAGES_PER_PASS",
    "ImageRetirementOutcome",
    "retire_superseded_images",
]
