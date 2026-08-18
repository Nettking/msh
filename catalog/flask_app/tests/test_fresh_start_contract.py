from __future__ import annotations

from pathlib import Path


def _start_script() -> str:
    return (Path(__file__).resolve().parents[3] / "start.cmd").read_text(encoding="utf-8")


def test_fresh_reset_is_verified_before_any_long_running_service_starts() -> None:
    script = _start_script()
    main_body = script.split("\n:resolve_build_commit", maxsplit=1)[0]
    reset_block = script.split("\n:reset_device_state", maxsplit=1)[1].split(
        "\n:show_help", maxsplit=1
    )[0]

    assert main_body.index("call :reset_device_state") < script.index(
        "docker compose up -d relay ollama recorder"
    )
    reset_command = (
        "docker compose run --rm --no-deps --build --entrypoint python flask -m "
        "catalog.flask_app.services.device_state_reset"
    )
    verify_command = (
        "docker compose run --rm --no-deps --entrypoint python flask -m "
        "catalog.flask_app.services.device_state_reset --verify-fresh"
    )
    assert reset_command in reset_block
    assert verify_command in reset_block
    assert reset_block.index(reset_command) < reset_block.index(verify_command)
    assert "docker compose exec -T flask python -m catalog.flask_app.services.device_state_reset --verify-fresh" not in script


def test_fresh_help_matches_factory_reset_contract() -> None:
    script = _start_script()
    reset_block = script.split("\n:reset_device_state", maxsplit=1)[1].split(
        "\n:show_help", maxsplit=1
    )[0]
    help_block = script.split("\n:show_help", maxsplit=1)[1]

    assert "human administrators, passwords, authentication secrets, and login sessions" in reset_block
    assert "source configuration and recorder configuration, checkpoints" in reset_block
    assert "analyses, results, digital-twin projections" in reset_block
    assert "preserves the machine recording corpus and its integrity metadata" in reset_block
    assert "Machine recordings, integrity metadata" in help_block
    assert "immutable checkout scaffolding survive" in help_block
    assert "source configuration, recorder checkpoints, results, and Ollama models" not in help_block.split(
        "The --fresh option", maxsplit=1
    )[1]
