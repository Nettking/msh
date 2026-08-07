from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]


def test_compose_defines_installable_language_model_provider() -> None:
    compose = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    assert "model-provider:" in compose
    assert 'profiles: ["provider"]' in compose
    assert "${MSH_PROVIDER_BIND:-0.0.0.0}:${MSH_PROVIDER_PORT:-11434}:11434" in compose
    assert (
        "    volumes:\n"
        "      - type: volume\n"
        "        source: model_provider_models\n"
        "        target: /root/.ollama"
    ) in compose
    assert (
        "name: ${MSH_MODEL_PROVIDER_VOLUME_NAME:-msh_model_provider_models}"
        in compose
    )
    assert "no.msh.capability=language-model" in compose
    assert "model-provider-install:" in compose
    assert "OLLAMA_HOST=http://model-provider:11434" in compose
    assert '${MSH_PROVIDER_MODEL:-smollm2:360m}' in compose


def test_compose_restores_local_model_installer_referenced_by_setup() -> None:
    compose = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    assert "ollama-pull:" in compose
    assert "OLLAMA_HOST=http://ollama:11434" in compose
    assert '${MSH_AI_MODEL:-llama3.2:3b}' in compose
