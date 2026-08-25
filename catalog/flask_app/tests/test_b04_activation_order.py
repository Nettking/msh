from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_posix_launcher_finishes_compose_activation_before_update_agent() -> None:
    script = (ROOT / "start.sh").read_text(encoding="utf-8")

    agent = 'nohup python3 "$ROOT/scripts/posix/fcp_update_agent.py"'
    assert script.index("docker compose up -d relay recorder") < script.index(agent)
    assert script.index("docker compose up -d flask") < script.index(agent)
    assert script.index("docker compose up -d ollama") < script.index(agent)
    assert script.index("docker compose exec -T flask python -c") < script.index(agent)
    assert script.index("docker compose ps relay ollama flask recorder") < script.index(agent)


def test_windows_launcher_finishes_compose_activation_before_update_agent() -> None:
    script = (ROOT / "start.cmd").read_text(encoding="utf-8")
    main_body = script.split("\n:repair_checkout_scaffolding", maxsplit=1)[0]

    agent = "call :start_update_agent"
    assert main_body.count(agent) == 1
    assert main_body.index("docker compose up -d relay recorder") < main_body.index(agent)
    assert main_body.index("docker compose up -d flask") < main_body.index(agent)
    assert main_body.index("docker compose up -d ollama") < main_body.index(agent)
    assert main_body.index("docker compose port flask 5000") < main_body.index(agent)
    assert main_body.index("Waiting for the FCP webapp") < main_body.index(agent)
