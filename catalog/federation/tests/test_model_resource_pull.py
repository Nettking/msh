from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from catalog.federation import model_resource_pull as model_pull
from catalog.federation.host_resources import PressureLevel, ResourceAssessment


def _assessment(level: PressureLevel, free: int) -> ResourceAssessment:
    return ResourceAssessment(
        resource_id="device:1",
        level=level,
        reasons=() if level is PressureLevel.NORMAL else (f"bytes_{level.name.lower()}",),
        effective_free_bytes=free,
        effective_free_inodes=100_000,
        reserved_bytes=0,
        reserved_inodes=0,
        observed_at=datetime.now(timezone.utc),
    )


class _Controller:
    def __init__(self, assessments: list[ResourceAssessment]) -> None:
        self.assessments = list(assessments)
        self.calls = 0

    def assessment(self, _path: Path) -> ResourceAssessment:
        index = min(self.calls, len(self.assessments) - 1)
        self.calls += 1
        return self.assessments[index]


def test_existing_model_needs_no_new_write_admission(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(model_pull, "_model_ready", lambda *_args: True)
    controller = _Controller([_assessment(PressureLevel.CRITICAL, 1)])

    result = model_pull.admitted_model_pull(
        tmp_path,
        model="llama3.2:3b",
        controller=controller,
    )

    assert result.ok is True
    assert result.code == "already_present"
    assert controller.calls == 0


def test_new_model_is_refused_before_pull_at_pressure(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(model_pull, "_model_ready", lambda *_args: False)
    controller = _Controller([_assessment(PressureLevel.PRESSURE, 12 * 1024**3)])
    started = False

    def _popen(*_args, **_kwargs):
        nonlocal started
        started = True
        raise AssertionError("pull must not start")

    monkeypatch.setattr(model_pull.subprocess, "Popen", _popen)

    result = model_pull.admitted_model_pull(
        tmp_path,
        model="llama3.2:3b",
        controller=controller,
    )

    assert result.ok is False
    assert result.code == "resource_pressure"
    assert started is False


def test_unbounded_pull_is_stopped_when_resource_reaches_pressure(
    monkeypatch, tmp_path
) -> None:
    monkeypatch.setattr(model_pull, "_model_ready", lambda *_args: False)
    controller = _Controller(
        [
            _assessment(PressureLevel.WARNING, 15 * 1024**3),
            _assessment(PressureLevel.WARNING, 14 * 1024**3),
            _assessment(PressureLevel.PRESSURE, 12 * 1024**3),
        ]
    )
    stopped: list[tuple[str, str]] = []

    class _Process:
        def poll(self):
            return None

        def wait(self, *, timeout):
            assert timeout == 30.0
            return 137

        def terminate(self):
            raise AssertionError("normal pressure stop should terminate through Docker")

    commands: list[list[str]] = []

    def _popen(command, **kwargs):
        commands.append(list(command))
        assert kwargs["cwd"] == tmp_path.resolve()
        assert kwargs["shell"] is False
        return _Process()

    monkeypatch.setattr(model_pull.subprocess, "Popen", _popen)
    monkeypatch.setattr(model_pull.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        model_pull,
        "_stop_model_writer",
        lambda _root, target, name: stopped.append((target.service, name)),
    )

    result = model_pull.admitted_model_pull(
        tmp_path,
        model="llama3.2:3b",
        controller=controller,
        timeout_seconds=60,
    )

    assert result.ok is False
    assert result.code == "resource_pressure"
    assert stopped and stopped[0][0] == "ollama"
    assert commands[0][:5] == ["docker", "compose", "--profile", "model-install", "run"]
    assert "--name" in commands[0]
    assert commands[0][-2:] == ["pull", "llama3.2:3b"]


def test_provider_profile_uses_separate_provider_writer(monkeypatch, tmp_path) -> None:
    ready = iter([False, True])
    monkeypatch.setattr(model_pull, "_model_ready", lambda *_args: next(ready))
    controller = _Controller(
        [
            _assessment(PressureLevel.NORMAL, 100 * 1024**3),
            _assessment(PressureLevel.NORMAL, 99 * 1024**3),
        ]
    )
    commands: list[list[str]] = []

    class _Process:
        def __init__(self):
            self.calls = 0

        def poll(self):
            self.calls += 1
            return None if self.calls == 1 else 0

    monkeypatch.setattr(
        model_pull.subprocess,
        "Popen",
        lambda command, **_kwargs: commands.append(list(command)) or _Process(),
    )
    monkeypatch.setattr(model_pull.time, "sleep", lambda _seconds: None)

    result = model_pull.admitted_model_pull(
        tmp_path,
        model="smollm2:360m",
        target_name="model-provider",
        controller=controller,
    )

    assert result.ok is True
    assert result.code == "installed"
    assert commands[0][2:5] == ["--profile", "provider", "run"]
    assert "model-provider-install" in commands[0]
