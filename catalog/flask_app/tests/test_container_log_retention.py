"""B07: supported containers must not write a lifetime log to the host.

Docker's ``json-file`` driver performs no rotation at all unless ``max-size``
is set. With no ``logging:`` declaration, a supported FCP service therefore
writes an unbounded log for as long as its container lives, and the only bound
is whatever the host daemon happens to be configured with -- which is not an
FCP cumulative bound.

The recorder shows the gap directly. ``catalog/mtconnect_recorder/runtime.py``
attaches both a ``RotatingFileHandler(maxBytes=2_000_000, backupCount=3)`` and
a ``StreamHandler`` to the same logger at the same level, so identical bytes go
to a path FCP deliberately bounds at ~8 MB and to a path that had no bound.

These tests parse the compose file structurally rather than matching text, so a
service added later without the shared policy fails here instead of quietly
reintroducing the unbounded path. They deliberately avoid a YAML dependency the
product does not otherwise carry.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
COMPOSE = REPO_ROOT / "docker-compose.yml"
ANCHOR = "fcp-container-logging"
SERVICE_RE = re.compile(r"^  ([A-Za-z0-9._-]+):$")
TOP_LEVEL_RE = re.compile(r"^[A-Za-z0-9._-]")


def _compose_lines() -> list[str]:
    return COMPOSE.read_text(encoding="utf-8").splitlines()


def _service_blocks() -> dict[str, list[str]]:
    """Return each top-level compose service and the lines belonging to it."""

    lines = _compose_lines()
    start = lines.index("services:")
    end = len(lines)
    for index in range(start + 1, len(lines)):
        line = lines[index]
        if line.strip() and TOP_LEVEL_RE.match(line) and not line.startswith("#"):
            end = index
            break

    blocks: dict[str, list[str]] = {}
    current: str | None = None
    for line in lines[start + 1 : end]:
        matched = SERVICE_RE.match(line)
        if matched:
            current = matched.group(1)
            blocks[current] = []
        elif current is not None:
            blocks[current].append(line)
    return blocks


def test_the_compose_file_declares_one_shared_retention_policy() -> None:
    text = COMPOSE.read_text(encoding="utf-8")

    assert f"x-{ANCHOR}: &{ANCHOR}" in text, "no FCP-owned logging policy is declared"
    # An explicit driver is the point: options are only guaranteed to apply to
    # a driver FCP chose, not to whichever driver the host daemon defaults to.
    assert 'driver: "json-file"' in text
    assert 'max-size: "${FCP_CONTAINER_LOG_MAX_SIZE:-10m}"' in text
    assert 'max-file: "${FCP_CONTAINER_LOG_MAX_FILE:-3}"' in text


def test_every_supported_service_is_bounded_by_that_policy() -> None:
    """The consequence: on main no service bounds its container log at all."""

    blocks = _service_blocks()
    assert len(blocks) >= 9, f"unexpected compose service set: {sorted(blocks)}"

    unbounded = [
        name
        for name, body in blocks.items()
        if f"    logging: *{ANCHOR}" not in body
    ]
    assert not unbounded, (
        "these supported services write an unbounded container log: "
        f"{sorted(unbounded)}"
    )


def test_each_service_uses_the_shared_anchor_rather_than_its_own_copy() -> None:
    """One policy, not nine that can drift apart."""

    blocks = _service_blocks()
    for name, body in blocks.items():
        inline = [
            line
            for line in body
            if "max-size" in line or "max-file" in line or "json-file" in line
        ]
        assert not inline, f"{name} re-declares the policy instead of sharing it"


def test_the_retention_bound_is_a_finite_size_and_count() -> None:
    """A bound that is not a finite size x count is not a bound."""

    text = COMPOSE.read_text(encoding="utf-8")
    size = re.search(r"max-size: \"\$\{FCP_CONTAINER_LOG_MAX_SIZE:-([^}]+)\}\"", text)
    count = re.search(r"max-file: \"\$\{FCP_CONTAINER_LOG_MAX_FILE:-([^}]+)\}\"", text)
    assert size is not None and count is not None
    assert re.fullmatch(r"\d+[kmg]", size.group(1)), size.group(1)
    assert int(count.group(1)) >= 1


def test_retention_does_not_touch_persistent_volumes() -> None:
    """Log retention is not volume deletion. The named volumes stay declared."""

    text = COMPOSE.read_text(encoding="utf-8")
    assert "\nvolumes:\n" in text
    for volume in ("ollama_models", "model_provider_models", "relay_state"):
        assert f"  {volume}:" in text


def test_the_bound_still_leaves_the_diagnostic_tail_every_consumer_reads() -> None:
    """The one programmatic consumer asks for a short tail, not full history."""

    headless = (REPO_ROOT / "headless_fcp.py").read_text(encoding="utf-8")
    assert '"logs", "--tail", "80"' in headless

    text = COMPOSE.read_text(encoding="utf-8")
    size = re.search(r"max-size: \"\$\{FCP_CONTAINER_LOG_MAX_SIZE:-(\d+)([kmg])\}\"", text)
    assert size is not None
    scale = {"k": 1024, "m": 1024**2, "g": 1024**3}[size.group(2)]
    retained_bytes = int(size.group(1)) * scale
    # 80 lines is trivially inside one rotation unit even at a generous
    # per-line size, so the bound cannot starve that consumer.
    assert retained_bytes > 80 * 4096
