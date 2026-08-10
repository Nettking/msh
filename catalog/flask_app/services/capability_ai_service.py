"""Ollama probes and model operations driven only by ``CapabilityConfig``."""

from __future__ import annotations

import json
import time
from typing import Any
from urllib import request

from .capability_config_service import AI_MODEL_CHOICES, CapabilityConfig

AI_BENCHMARK_PROMPT = "Reply with exactly: FCP_OK"
AI_RESPONSE_TIME_BANDS: list[dict[str, object]] = [
    {
        "key": "fast",
        "label": "Fast",
        "upper_ms": 5_000,
        "description": "Comfortable for interactive setup and short repository questions.",
    },
    {
        "key": "usable",
        "label": "Usable",
        "upper_ms": 15_000,
        "description": "Acceptable for occasional questions, but not instant.",
    },
    {
        "key": "slow",
        "label": "Slow",
        "upper_ms": 30_000,
        "description": "Works, but the user will notice the wait.",
    },
    {
        "key": "too_slow",
        "label": "Too slow",
        "upper_ms": None,
        "description": "Choose a smaller model or stronger machine for normal use.",
    },
]


def _model_aliases(model_name: str) -> set[str]:
    base = model_name.split(":", 1)[0]
    return {model_name, base}


def ai_provider_label(config: CapabilityConfig) -> str:
    if config.ai_provider_mode == "connected":
        return config.ai_provider_name or "Connected computer"
    return "This computer"


def _ollama_status_base(config: CapabilityConfig) -> dict[str, Any]:
    return {
        "provider": {
            "capability": "language-model",
            "protocol": "ollama",
            "mode": config.ai_provider_mode,
            "name": ai_provider_label(config),
            "base_url": config.ollama_base_url,
        },
        "selected_model": config.ai_model,
        "selected_model_installed": False,
        "models": [],
        "installed_by_profile": {key: False for key in AI_MODEL_CHOICES},
        "installed_by_model": {
            choice["model"]: False for choice in AI_MODEL_CHOICES.values()
        },
    }


def response_time_assessment(
    elapsed_ms: float | int | None,
) -> dict[str, object]:
    if elapsed_ms is None:
        return {
            "key": "unavailable",
            "label": "Unavailable",
            "description": "No response-time measurement is available.",
        }
    for band in AI_RESPONSE_TIME_BANDS:
        upper_ms = band["upper_ms"]
        if upper_ms is None or float(elapsed_ms) <= float(upper_ms):
            return {
                "key": band["key"],
                "label": band["label"],
                "description": band["description"],
            }
    return {
        "key": "too_slow",
        "label": "Too slow",
        "description": "Choose a smaller model or stronger machine for normal use.",
    }


def ollama_status(
    config: CapabilityConfig,
    timeout_seconds: float = 2.0,
) -> dict[str, Any]:
    """Probe the configured Ollama endpoint without implying contribution authority."""

    base_status = _ollama_status_base(config)
    provider_name = ai_provider_label(config)
    try:
        req = request.Request(
            f"{config.ollama_base_url.rstrip('/')}/api/tags",
            method="GET",
        )
        with request.urlopen(req, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:  # pragma: no cover - depends on local/remote Ollama
        return {
            **base_status,
            "running": False,
            "message": (
                f"{provider_name} is not reachable at {config.ollama_base_url}: {exc}"
            ),
        }

    models = sorted(
        str(item.get("name") or "")
        for item in payload.get("models", [])
        if item.get("name")
    )
    installed_names = set(models)
    installed_by_profile = {
        key: bool(installed_names & _model_aliases(choice["model"]))
        for key, choice in AI_MODEL_CHOICES.items()
    }
    installed_by_model = {
        choice["model"]: installed_by_profile[key]
        for key, choice in AI_MODEL_CHOICES.items()
    }
    selected_installed = bool(
        installed_names & _model_aliases(config.ai_model)
    )
    return {
        **base_status,
        "running": True,
        "selected_model_installed": selected_installed,
        "models": models,
        "installed_by_profile": installed_by_profile,
        "installed_by_model": installed_by_model,
        "message": (
            f"{provider_name} is connected and {config.ai_model} is ready."
            if selected_installed
            else (
                f"{provider_name} is connected, but {config.ai_model} is not installed yet."
            )
        ),
    }


def benchmark_ollama_response_time(
    config: CapabilityConfig,
    *,
    model: str | None = None,
    timeout_seconds: int = 120,
) -> dict[str, Any]:
    """Measure one explicit model probe without enabling a contribution."""

    selected_model = str(model or config.ai_model).strip()
    payload = json.dumps(
        {
            "model": selected_model,
            "stream": False,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a startup response-time check. Reply with exactly FCP_OK."
                    ),
                },
                {"role": "user", "content": AI_BENCHMARK_PROMPT},
            ],
            "options": {"num_predict": 8},
        }
    ).encode("utf-8")
    req = request.Request(
        f"{config.ollama_base_url.rstrip('/')}/api/chat",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    started = time.perf_counter()
    try:
        with request.urlopen(req, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8")
        elapsed_ms = round((time.perf_counter() - started) * 1000.0)
        response_payload = json.loads(body)
    except Exception as exc:  # pragma: no cover - depends on local/remote Ollama
        elapsed_ms = round((time.perf_counter() - started) * 1000.0)
        return {
            "ok": False,
            "model": selected_model,
            "elapsed_ms": elapsed_ms,
            "assessment": response_time_assessment(None),
            "thresholds": AI_RESPONSE_TIME_BANDS,
            "message": f"Could not test {selected_model}: {exc}",
        }

    content = str(
        response_payload.get("message", {}).get("content") or ""
    ).strip()
    assessment = response_time_assessment(elapsed_ms)
    return {
        "ok": True,
        "model": selected_model,
        "elapsed_ms": elapsed_ms,
        "assessment": assessment,
        "thresholds": AI_RESPONSE_TIME_BANDS,
        "answer_preview": content[:80],
        "message": (
            f"{selected_model} responded in {elapsed_ms} ms. "
            f"Assessment: {assessment['label']}."
        ),
    }


def pull_ollama_model(
    config: CapabilityConfig,
    timeout_seconds: int = 900,
) -> tuple[bool, str]:
    """Install the configured model at the configured endpoint."""

    payload = json.dumps({"name": config.ai_model, "stream": False}).encode("utf-8")
    req = request.Request(
        f"{config.ollama_base_url.rstrip('/')}/api/pull",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8")
    except Exception as exc:  # pragma: no cover - depends on local/remote Ollama
        return False, f"Could not pull {config.ai_model}: {exc}"
    return True, (
        f"Ollama model is installed or updated on {ai_provider_label(config)}: "
        f"{config.ai_model}. Response: {body[:200]}"
    )


__all__ = [
    "AI_BENCHMARK_PROMPT",
    "AI_RESPONSE_TIME_BANDS",
    "ai_provider_label",
    "benchmark_ollama_response_time",
    "ollama_status",
    "pull_ollama_model",
    "response_time_assessment",
]
