from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation import host_build
from catalog.federation.host_resources import PressureLevel


class _CompletedProcess:
    def __init__(self, returncode: int) -> None:
        self.returncode = returncode

    def poll(self):
        return self.returncode


def _warning_assessment(tmp_path: Path):
    return tmp_path, SimpleNamespace(level=PressureLevel.WARNING)


def test_failed_build_still_runs_cache_lifecycle(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        host_build,
        "docker_resource_assessment",
        lambda *_args, **_kwargs: _warning_assessment(tmp_path),
    )
    monkeypatch.setattr(
        host_build,
        "ensure_controllable_builder",
        lambda *_args, **_kwargs: "fcp-build-test",
    )
    monkeypatch.setattr(
        host_build.subprocess,
        "Popen",
        lambda *_args, **_kwargs: _CompletedProcess(1),
    )
    prunes: list[tuple[str | None, dict[str, str]]] = []
    monkeypatch.setattr(
        host_build,
        "prune_build_cache",
        lambda _root, env, *, name=None: prunes.append((name, dict(env))) or True,
    )
    stopped: list[str] = []
    monkeypatch.setattr(
        host_build,
        "stop_build_writer",
        lambda _root, name, _env, **_kwargs: stopped.append(name) or True,
    )

    with pytest.raises(RuntimeError, match="core_image_build_failed"):
        host_build.controlled_core_build(
            tmp_path,
            {"COMPOSE_PROJECT_NAME": "fcp", "FCP_BUILD_COMMIT": "a" * 40},
        )

    assert prunes == [
        (
            "fcp-build-test",
            {"COMPOSE_PROJECT_NAME": "fcp", "FCP_BUILD_COMMIT": "a" * 40},
        )
    ]
    assert stopped == ["fcp-build-test"]


def test_failed_build_and_cache_prune_failure_remain_distinct(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(
        host_build,
        "docker_resource_assessment",
        lambda *_args, **_kwargs: _warning_assessment(tmp_path),
    )
    monkeypatch.setattr(
        host_build,
        "ensure_controllable_builder",
        lambda *_args, **_kwargs: "fcp-build-test",
    )
    monkeypatch.setattr(
        host_build.subprocess,
        "Popen",
        lambda *_args, **_kwargs: _CompletedProcess(1),
    )
    monkeypatch.setattr(host_build, "prune_build_cache", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(host_build, "stop_build_writer", lambda *_args, **_kwargs: True)

    with pytest.raises(RuntimeError, match="build_failed_and_cache_prune_failed"):
        host_build.controlled_core_build(tmp_path, {})


def test_successful_build_requires_cache_prune_success(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        host_build,
        "docker_resource_assessment",
        lambda *_args, **_kwargs: _warning_assessment(tmp_path),
    )
    monkeypatch.setattr(
        host_build,
        "ensure_controllable_builder",
        lambda *_args, **_kwargs: "fcp-build-test",
    )
    monkeypatch.setattr(
        host_build.subprocess,
        "Popen",
        lambda *_args, **_kwargs: _CompletedProcess(0),
    )
    monkeypatch.setattr(host_build, "prune_build_cache", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(host_build, "stop_build_writer", lambda *_args, **_kwargs: True)

    with pytest.raises(RuntimeError, match="build_cache_prune_failed"):
        host_build.controlled_core_build(tmp_path, {})


def test_successful_build_reproves_source_identity(tmp_path: Path, monkeypatch) -> None:
    commits = iter(["c" * 40, "d" * 40])
    monkeypatch.setattr(host_build, "resolve_clean_commit", lambda _root: next(commits))
    monkeypatch.setattr(host_build, "preflight_disk", lambda *_args: None)
    monkeypatch.setattr(host_build, "controlled_core_build", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        host_build,
        "_verify_core_image_commits",
        lambda *_args, **_kwargs: None,
    )

    with pytest.raises(RuntimeError, match="build_context_changed"):
        host_build.build_core_images_locked(tmp_path, {})


def test_expected_commit_is_checked_before_build(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(host_build, "resolve_clean_commit", lambda _root: "e" * 40)
    called: list[bool] = []
    monkeypatch.setattr(
        host_build,
        "preflight_disk",
        lambda *_args: called.append(True),
    )

    with pytest.raises(RuntimeError, match="source_verification_failed"):
        host_build.build_core_images_locked(
            tmp_path,
            {},
            expected_commit="f" * 40,
        )

    assert called == []


def test_build_policy_matches_supported_update_figures() -> None:
    assert host_build.UPDATE_REQUIRED_FREE_BYTES == 10 * 1024**3
    assert host_build.BUILD_CACHE_KEEP_BYTES == 8 * 1024**3


@pytest.fixture
def built_images(monkeypatch):
    """Model Docker's distinct configured-image and created-container views."""
    candidate = "c" * 40
    current_id = "sha256:" + "1" * 64
    previous_id = "sha256:" + "2" * 64
    references = {service: f"fcp-{service}:candidate" for service in host_build.CORE_BUILD_SERVICES}
    state = SimpleNamespace(
        candidate=candidate,
        current_id=current_id,
        previous_id=previous_id,
        references=references,
        containers={},
        rendered={service: (0, reference) for service, reference in references.items()},
        images={reference: (current_id, candidate) for reference in references.values()},
        inspect_results={},
        inspected=[],
    )
    state.images.update({current_id: (current_id, candidate), previous_id: (previous_id, "b" * 40)})

    def docker(_root, arguments, *, env, timeout):
        assert env == {"COMPOSE_PROJECT_NAME": "reviewed-project"}
        assert timeout > 0
        if arguments[:4] == ["docker", "compose", "config", "--images"]:
            code, output = state.rendered[arguments[4]]
        elif arguments[:4] == ["docker", "compose", "images", "-q"]:
            code, output = 0, state.containers.get(arguments[4], "")
        else:
            assert arguments[:3] == ["docker", "image", "inspect"]
            reference = arguments[-1]
            state.inspected.append(reference)
            if reference in state.inspect_results:
                code, output = state.inspect_results[reference]
            else:
                image_id, label = state.images[reference]
                image_format = arguments[arguments.index("--format") + 1]
                code, output = 0, f"{image_id}|{label}" if "{{.Id}}" in image_format else label
        return SimpleNamespace(returncode=code, stdout=output, stderr="")

    monkeypatch.setattr(host_build, "_docker_run", docker)
    return state


@pytest.mark.parametrize("container_state", ["absent", "previous", "current"])
def test_built_image_identity_is_independent_of_created_containers(
    tmp_path: Path, built_images, container_state: str
) -> None:
    if container_state != "absent":
        image_id = built_images.previous_id if container_state == "previous" else built_images.current_id
        built_images.containers.update(dict.fromkeys(host_build.CORE_BUILD_SERVICES, image_id))

    host_build._verify_core_image_commits(
        tmp_path, {"COMPOSE_PROJECT_NAME": "reviewed-project"}, built_images.candidate,
    )

    assert set(built_images.inspected) == set(built_images.references.values())


@pytest.mark.parametrize("service", host_build.CORE_BUILD_SERVICES)
def test_wrong_built_image_is_rejected_even_when_running_container_is_current(
    tmp_path: Path, built_images, service: str
) -> None:
    built_images.containers.update(dict.fromkeys(host_build.CORE_BUILD_SERVICES, built_images.current_id))
    built_images.images[built_images.references[service]] = (built_images.previous_id, "b" * 40)

    with pytest.raises(RuntimeError, match="built_image_identity_mismatch"):
        host_build._verify_core_image_commits(
            tmp_path, {"COMPOSE_PROJECT_NAME": "reviewed-project"}, built_images.candidate,
        )


@pytest.mark.parametrize("rendered", [(1, "fcp-relay:candidate"), (0, ""), (0, "one\ntwo")])
def test_unavailable_or_ambiguous_configured_image_is_refused(
    tmp_path: Path, built_images, rendered: tuple[int, str]
) -> None:
    built_images.rendered["relay"] = rendered

    with pytest.raises(RuntimeError, match="built_image_identity_unavailable"):
        host_build._verify_core_image_commits(
            tmp_path, {"COMPOSE_PROJECT_NAME": "reviewed-project"}, built_images.candidate,
        )

    assert built_images.inspected == []


@pytest.mark.parametrize(
    "inspection",
    [(1, ""), (0, ""), (0, "missing-separator"), (0, "|" + "c" * 40),
     (0, "sha256:one|" + "c" * 40 + "\nsha256:two|" + "c" * 40)],
)
def test_failed_or_malformed_built_image_inspection_is_refused(
    tmp_path: Path, built_images, inspection: tuple[int, str]
) -> None:
    built_images.inspect_results[built_images.references["relay"]] = inspection

    with pytest.raises(RuntimeError, match="built_image_identity_unavailable"):
        host_build._verify_core_image_commits(
            tmp_path, {"COMPOSE_PROJECT_NAME": "reviewed-project"}, built_images.candidate,
        )


def test_built_image_without_commit_label_is_refused(tmp_path: Path, built_images) -> None:
    built_images.images[built_images.references["relay"]] = (built_images.current_id, "")

    with pytest.raises(RuntimeError, match="built_image_identity_mismatch"):
        host_build._verify_core_image_commits(
            tmp_path, {"COMPOSE_PROJECT_NAME": "reviewed-project"}, built_images.candidate,
        )
