"""Retire FCP Buildx builders whose owning checkout no longer exists.

FCP scopes its BuildKit writer to one checkout. Builders therefore need a safe
retirement frontier when their owning checkout disappears. Ownership is stamped
onto the BuildKit container at builder creation and is the only fact that can
license removal; age, size, and name alone never do.

One retirement pass is bounded independently of host lifetime. Candidate
enumeration uses ``docker ps`` rather than the global ``buildx ls`` table because
container listing supports both a name filter and a server-side ``--last`` bound.
A small cursor advances that bounded window toward older FCP BuildKit containers
across pressured passes, so unrelated host builders and permanently refused FCP
builders cannot pin every pass to the same prefix forever.
"""

from __future__ import annotations

import os
import re
import subprocess
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

BUILDER_ROOT_ENV = "FCP_BUILDER_ROOT_HEX"
MAX_LISTED_BUILDERS_PER_PASS = 32
MAX_EXAMINED_BUILDERS_PER_PASS = 16
MAX_REMOVAL_ATTEMPTS_PER_PASS = 8
_CURSOR_ID_RE = re.compile(r"^[0-9a-fA-F]{12,64}$")

DockerRun = Callable[[Sequence[str]], "subprocess.CompletedProcess[str]"]


def builder_root_driver_opt(root: Path) -> str:
    """Return the ``--driver-opt`` that stamps this checkout onto its builder."""

    encoded = str(root.resolve()).encode("utf-8").hex()
    return f"env.{BUILDER_ROOT_ENV}={encoded}"


def stamped_root(environment: Iterable[str]) -> Path | None:
    """Recover the owning checkout path from a BuildKit container environment."""

    prefix = f"{BUILDER_ROOT_ENV}="
    for entry in environment:
        line = entry.strip()
        if not line.startswith(prefix):
            continue
        encoded = line[len(prefix) :]
        if not encoded:
            return None
        try:
            decoded = bytes.fromhex(encoded).decode("utf-8")
        except (ValueError, UnicodeDecodeError):
            return None
        if not decoded:
            return None
        return Path(decoded)
    return None


@dataclass(frozen=True)
class BuilderRetirementOutcome:
    """What one bounded pass proved, refused, and actually removed."""

    retired: tuple[str, ...] = ()
    refused: tuple[str, ...] = ()
    listed: int = 0
    examined: int = 0
    attempted: int = 0


def _cursor_path(current_builder: str) -> Path:
    """Return a per-current-checkout cursor outside the source tree."""

    override = os.environ.get("FCP_BUILDER_RETIREMENT_STATE_DIR", "").strip()
    base = Path(override) if override else Path.home() / ".fcp" / "builder-retirement"
    return base / f"{current_builder}.cursor"


def _read_cursor(path: Path) -> str | None:
    try:
        value = path.read_text(encoding="ascii").strip()
    except (OSError, UnicodeError):
        return None
    return value.lower() if _CURSOR_ID_RE.fullmatch(value) else None


def _write_cursor(path: Path, value: str | None) -> None:
    """Persist only progress metadata; failures never license a deletion."""

    temporary: Path | None = None
    try:
        if value is None:
            path.unlink(missing_ok=True)
            return
        if not _CURSOR_ID_RE.fullmatch(value):
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
        temporary.write_text(value.lower() + "\n", encoding="ascii")
        temporary.replace(path)
    except (OSError, UnicodeError):
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def _builder_page(
    docker_run: DockerRun,
    *,
    builder_prefix: str,
    container_prefix: str,
    current_builder: str,
) -> tuple[str, ...] | None:
    """Return one bounded page of FCP builders and advance its durable cursor.

    Docker's container list supports filtering and ``--last``. The output is
    therefore bounded before it reaches Python; unlike ``buildx ls`` this does
    not first materialize the host's full lifetime builder table.
    """

    cursor_path = _cursor_path(current_builder)
    cursor = _read_cursor(cursor_path)
    arguments = [
        "docker",
        "ps",
        "--all",
        "--no-trunc",
        "--last",
        str(MAX_LISTED_BUILDERS_PER_PASS),
        "--filter",
        f"name={container_prefix}{builder_prefix}",
    ]
    if cursor is not None:
        arguments.extend(["--filter", f"before={cursor}"])
    arguments.extend(["--format", "{{.ID}} {{.Names}}"])
    try:
        listed = docker_run(arguments)
    except (OSError, subprocess.SubprocessError):
        return None
    if listed.returncode != 0:
        if cursor is not None:
            _write_cursor(cursor_path, None)
        return None

    builders: list[str] = []
    last_id: str | None = None
    seen_rows = 0
    exact_prefix = f"{container_prefix}{builder_prefix}"
    for raw in listed.stdout.splitlines():
        text = raw.strip()
        if not text:
            continue
        parts = text.split(maxsplit=1)
        if len(parts) != 2:
            return None
        container_id, container_name = parts
        if not _CURSOR_ID_RE.fullmatch(container_id):
            return None
        seen_rows += 1
        last_id = container_id.lower()
        if not container_name.startswith(exact_prefix) or not container_name.endswith("0"):
            continue
        name = container_name[len(container_prefix) : -1]
        if not name.startswith(builder_prefix) or name == current_builder:
            continue
        builders.append(name)

    _write_cursor(
        cursor_path,
        last_id if seen_rows >= MAX_LISTED_BUILDERS_PER_PASS else None,
    )
    return tuple(builders)


def _container_environment(
    docker_run: DockerRun,
    container_prefix: str,
    name: str,
) -> tuple[str, ...] | None:
    try:
        inspected = docker_run(
            [
                "docker",
                "inspect",
                "--format",
                "{{range .Config.Env}}{{println .}}{{end}}",
                f"{container_prefix}{name}0",
            ]
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if inspected.returncode != 0:
        return None
    return tuple(line.strip() for line in inspected.stdout.splitlines() if line.strip())


def retire_stranded_builders(
    *,
    current_builder: str,
    builder_prefix: str,
    container_prefix: str,
    docker_run: DockerRun,
    container_running: Callable[[str], bool],
    remove_builder: Callable[[str], bool],
) -> BuilderRetirementOutcome:
    """Remove FCP builders Docker proves belong to a checkout that is gone."""

    names = _builder_page(
        docker_run,
        builder_prefix=builder_prefix,
        container_prefix=container_prefix,
        current_builder=current_builder,
    )
    if names is None:
        return BuilderRetirementOutcome()

    retired: list[str] = []
    refused: list[str] = []
    examined = 0
    attempted = 0
    for name in names:
        if examined >= MAX_EXAMINED_BUILDERS_PER_PASS:
            break
        examined += 1
        environment = _container_environment(docker_run, container_prefix, name)
        if environment is None:
            refused.append(name)
            continue
        owner = stamped_root(environment)
        if owner is None:
            refused.append(name)
            continue
        try:
            owner_exists = owner.exists()
        except OSError:
            owner_exists = True
        if owner_exists:
            refused.append(name)
            continue
        if container_running(name):
            refused.append(name)
            continue
        if attempted >= MAX_REMOVAL_ATTEMPTS_PER_PASS:
            break
        attempted += 1
        if remove_builder(name):
            retired.append(name)
        else:
            refused.append(name)

    return BuilderRetirementOutcome(
        retired=tuple(retired),
        refused=tuple(refused),
        listed=len(names),
        examined=examined,
        attempted=attempted,
    )
