from __future__ import annotations

import subprocess
from pathlib import Path

from catalog.federation.software_update import GitUpdateAdapter, UpdateInspection


def test_only_canonical_approved_remote_is_accepted() -> None:
    approved = GitUpdateAdapter._approved_remote
    assert approved("https://github.com/Nettking/msh.git")
    assert approved("git@github.com:Nettking/msh.git")
    assert approved("ssh://git@github.com/Nettking/msh")
    assert not approved("https://token@github.com/Nettking/msh.git")
    assert not approved("https://github.com/attacker/msh.git")
    assert not approved("file:///tmp/msh")


class _Runner:
    def __init__(self) -> None:
        self.commands: list[list[str]] = []

    def __call__(self, argv, **kwargs):
        self.commands.append(argv)
        command = tuple(argv[1:])
        outputs = {
            ("rev-parse", "--show-toplevel"): "/repo\n",
            ("remote", "get-url", "origin"): "https://github.com/Nettking/msh.git\n",
            ("symbolic-ref", "--quiet", "--short", "HEAD"): "main\n",
            ("rev-parse", "--verify", "HEAD^{commit}"): "1" * 40 + "\n",
            ("status", "--porcelain=v1", "--untracked-files=all"): "?? local.txt\n",
        }
        return subprocess.CompletedProcess(argv, 0, outputs.get(command, ""), "")


def test_dirty_checkout_fails_before_fetch_or_mutation() -> None:
    runner = _Runner()
    result = GitUpdateAdapter(Path("/repo"), runner=runner).inspect()
    assert result.state == "dirty"
    assert not any(command[1] in {"fetch", "merge", "reset", "clean", "checkout"} for command in runner.commands)


def test_apply_rejects_non_object_id_without_running_git() -> None:
    runner = _Runner()
    result = GitUpdateAdapter(Path("/repo"), runner=runner).apply("main; rm -rf /")
    assert result.code == "target_unavailable"
    assert runner.commands == []


def test_source_update_result_does_not_claim_running_installation_success() -> None:
    adapter = GitUpdateAdapter(Path("/repo"))
    target = "2" * 40
    adapter.inspect = lambda **_kwargs: UpdateInspection(
        "update_available", "1" * 40, target
    )
    adapter._git = lambda *_args: subprocess.CompletedProcess([], 0, "", "")
    adapter._resolve = lambda _ref: target

    result = adapter.apply(target)

    assert result.state == "source_updated_restart_required"
    assert result.code == "runtime_not_updated"
    assert "not rebuilt, reinstalled, or restarted" in result.message
