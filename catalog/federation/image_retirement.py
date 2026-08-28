"""Retire FCP's own superseded core images after a verified build transition.

Every core build produces new ``relay``/``flask``/``recorder`` images and moves
the Compose tags onto them. The images those tags used to point at keep every
layer they own and lose their only name, and nothing in the product has ever
removed one. They accumulate once per update, for the life of the device, on
the very Docker backing resource B01 admits builds against.

Only FCP-owned dangling images may be removed, and one pass must remain bounded
in both Docker calls and listing consumption regardless of lifetime history.
"""

from __future__ import annotations

import json
import queue
import subprocess
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

BUILD_COMMIT_LABEL = "no.fcp.build_commit"
MAX_LISTED_IMAGES_PER_PASS = 64
MAX_EXAMINED_IMAGES_PER_PASS = 32
MAX_REMOVAL_ATTEMPTS_PER_PASS = 16
DOCKER_TIMEOUT_SECONDS = 60.0
MAX_PASS_SECONDS = 120.0
_listing_popen = subprocess.Popen


@dataclass(frozen=True)
class ImageRetirementOutcome:
    """What one bounded retirement pass actually did."""

    retired: tuple[str, ...]
    refused: tuple[str, ...]
    listed: int
    examined: int
    attempted: int

    @property
    def freed_any(self) -> bool:
        return bool(self.retired)


def _docker(
    root: Path,
    args: Sequence[str],
    env: Mapping[str, str],
    *,
    deadline: float,
) -> subprocess.CompletedProcess[str] | None:
    """Run one bounded Docker query; ``None`` means it was not answerable."""

    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return None
    try:
        return subprocess.run(
            list(args),
            cwd=root,
            env=dict(env),
            shell=False,
            check=False,
            capture_output=True,
            text=True,
            timeout=min(DOCKER_TIMEOUT_SECONDS, remaining),
        )
    except (OSError, subprocess.SubprocessError, ValueError):
        return None


def _stop_process(process: subprocess.Popen[str], deadline: float) -> None:
    if process.poll() is not None:
        return
    try:
        process.terminate()
    except OSError:
        return
    remaining = max(deadline - time.monotonic(), 0.0)
    try:
        process.wait(timeout=min(1.0, remaining))
        return
    except (OSError, subprocess.TimeoutExpired):
        pass
    try:
        process.kill()
    except OSError:
        return
    try:
        process.wait(timeout=min(1.0, max(deadline - time.monotonic(), 0.0)))
    except (OSError, subprocess.TimeoutExpired):
        pass


def _queue_reader_item(
    output: queue.Queue[str | BaseException | None],
    stop: threading.Event,
    item: str | BaseException | None,
) -> bool:
    while not stop.is_set():
        try:
            output.put(item, timeout=0.05)
            return True
        except queue.Full:
            continue
    return False


def _stream_lines(
    stream: TextIO,
    output: queue.Queue[str | BaseException | None],
    stop: threading.Event,
) -> None:
    try:
        for line in stream:
            if not _queue_reader_item(output, stop, line):
                return
    except BaseException as exc:  # noqa: BLE001 - delivered to bounded caller
        _queue_reader_item(output, stop, exc)
    finally:
        _queue_reader_item(output, stop, None)


def _referenced_image_ids(
    root: Path,
    env: Mapping[str, str],
    *,
    deadline: float,
) -> set[str] | None:
    completed = _docker(
        root,
        ["docker", "ps", "--all", "--no-trunc", "--format", "{{.Image}}"],
        env,
        deadline=deadline,
    )
    if completed is None or completed.returncode != 0:
        return None
    return {line.strip() for line in completed.stdout.splitlines() if line.strip()}


def _listed_dangling_fcp_image_ids(
    root: Path,
    env: Mapping[str, str],
    *,
    limit: int,
    deadline: float,
) -> list[str] | None:
    """Stream at most ``limit`` dangling FCP image rows from the Docker CLI.

    Docker has no image-list row limit. The CLI is therefore read incrementally
    through a one-item queue and terminated as soon as this pass consumes its
    row budget. A blocked producer is cut off by the same pass deadline.
    """

    if limit <= 0 or time.monotonic() >= deadline:
        return []
    args = [
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
    ]
    try:
        process = _listing_popen(
            args,
            cwd=root,
            env=dict(env),
            shell=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
            errors="strict",
        )
    except (OSError, ValueError):
        return None
    if process.stdout is None:
        _stop_process(process, deadline)
        return None

    output: queue.Queue[str | BaseException | None] = queue.Queue(maxsize=1)
    stop = threading.Event()
    reader = threading.Thread(
        target=_stream_lines,
        args=(process.stdout, output, stop),
        name="fcp-image-retirement-listing",
        daemon=True,
    )
    reader.start()
    listed: list[str] = []
    completed_naturally = False
    try:
        while len(listed) < limit:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            try:
                item = output.get(timeout=min(DOCKER_TIMEOUT_SECONDS, remaining))
            except queue.Empty:
                return None
            if item is None:
                completed_naturally = True
                break
            if isinstance(item, BaseException):
                return None
            text = item.strip()
            if not text:
                continue
            try:
                record = json.loads(text)
            except ValueError:
                return None
            if not isinstance(record, Mapping):
                return None
            image_id = str(record.get("ID") or "").strip()
            if not image_id:
                return None
            listed.append(image_id)

        if len(listed) >= limit:
            return listed
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        try:
            returncode = process.wait(timeout=min(DOCKER_TIMEOUT_SECONDS, remaining))
        except (OSError, subprocess.TimeoutExpired):
            return None
        return listed if completed_naturally and returncode == 0 else None
    finally:
        stop.set()
        _stop_process(process, deadline)
        try:
            process.stdout.close()
        except OSError:
            pass
        reader.join(timeout=0.2)


def _image_build_commit(
    root: Path,
    image_id: str,
    env: Mapping[str, str],
    *,
    deadline: float,
) -> str | None:
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
        deadline=deadline,
    )
    if completed is None or completed.returncode != 0:
        return None
    return completed.stdout.strip()


def retire_superseded_images(
    root: Path,
    env: Mapping[str, str],
    *,
    active_commit: str,
    listed_limit: int = MAX_LISTED_IMAGES_PER_PASS,
    examined_limit: int = MAX_EXAMINED_IMAGES_PER_PASS,
    attempt_limit: int = MAX_REMOVAL_ATTEMPTS_PER_PASS,
    pass_seconds: float = MAX_PASS_SECONDS,
) -> ImageRetirementOutcome:
    """Remove superseded FCP images after a verified build transition."""

    empty = ImageRetirementOutcome(
        retired=(), refused=(), listed=0, examined=0, attempted=0
    )
    if not active_commit.strip():
        return empty
    deadline = time.monotonic() + max(float(pass_seconds), 0.0)
    referenced = _referenced_image_ids(root, env, deadline=deadline)
    if referenced is None:
        return empty
    listed = _listed_dangling_fcp_image_ids(
        root,
        env,
        limit=max(int(listed_limit), 0),
        deadline=deadline,
    )
    if listed is None:
        return empty

    active = active_commit.strip().lower()
    examined_budget = max(int(examined_limit), 0)
    attempt_budget = max(int(attempt_limit), 0)
    retired: list[str] = []
    refused: list[str] = []
    examined = 0
    attempted = 0
    for image_id in listed:
        if examined >= examined_budget or attempted >= attempt_budget:
            break
        if time.monotonic() >= deadline:
            break
        if image_id in referenced:
            continue
        examined += 1
        commit = _image_build_commit(root, image_id, env, deadline=deadline)
        if commit is None or not commit or commit.strip().lower() == active:
            continue
        attempted += 1
        completed = _docker(
            root, ["docker", "image", "rm", image_id], env, deadline=deadline
        )
        if completed is None or completed.returncode != 0:
            refused.append(image_id)
            continue
        retired.append(image_id)
    return ImageRetirementOutcome(
        retired=tuple(retired),
        refused=tuple(refused),
        listed=len(listed),
        examined=examined,
        attempted=attempted,
    )


__all__ = [
    "BUILD_COMMIT_LABEL",
    "MAX_EXAMINED_IMAGES_PER_PASS",
    "MAX_LISTED_IMAGES_PER_PASS",
    "MAX_PASS_SECONDS",
    "MAX_REMOVAL_ATTEMPTS_PER_PASS",
    "ImageRetirementOutcome",
    "retire_superseded_images",
]
