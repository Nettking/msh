"""Setup-time AI model comparison and recommendation helpers."""

from __future__ import annotations

from typing import Any

from .server_setup_service import (
    AI_MODEL_CHOICES,
    AI_RESPONSE_TIME_BANDS,
    ServerSetupSettings,
    benchmark_ollama_response_time,
    ollama_status,
)

ACCEPTABLE_BANDS = {"fast", "usable"}
MARGINAL_BANDS = {"slow"}
PROFILE_STRENGTH = {profile: index for index, profile in enumerate(AI_MODEL_CHOICES)}
EDGE_PROFILE = "edge-small"


def _row(profile: str, *, installed: bool, result: dict[str, Any] | None = None) -> dict[str, Any]:
    choice = AI_MODEL_CHOICES[profile]
    return {
        "profile": profile,
        "label": choice["label"],
        "model": choice["model"],
        "device": choice["device"],
        "installed": installed,
        "tested": result is not None,
        "result": result or {},
    }


def _result_band(row: dict[str, Any]) -> str:
    return str(row.get("result", {}).get("assessment", {}).get("key") or "unavailable")


def _result_elapsed(row: dict[str, Any]) -> int:
    value = row.get("result", {}).get("elapsed_ms")
    return int(value) if isinstance(value, int | float) else 999_999


def _recommend(rows: list[dict[str, Any]]) -> dict[str, Any]:
    tested = [row for row in rows if row.get("tested")]
    successful = [row for row in tested if row.get("result", {}).get("ok")]
    acceptable = [row for row in successful if _result_band(row) in ACCEPTABLE_BANDS]
    marginal = [row for row in successful if _result_band(row) in MARGINAL_BANDS]
    edge = next((row for row in rows if row.get("profile") == EDGE_PROFILE), None)

    if acceptable:
        recommended = max(acceptable, key=lambda row: PROFILE_STRENGTH[str(row["profile"])])
        elapsed = _result_elapsed(recommended)
        assessment = recommended["result"]["assessment"]["label"]
        return {
            "verdict": "supported",
            "hardware_supported": True,
            "recommended_profile": recommended["profile"],
            "recommended_model": recommended["model"],
            "recommended_label": recommended["label"],
            "message": f"Recommended model: {recommended['label']} ({recommended['model']}). It responded in {elapsed} ms, which is {assessment.lower()}.",
        }

    if marginal:
        recommended = min(marginal, key=_result_elapsed)
        elapsed = _result_elapsed(recommended)
        return {
            "verdict": "marginal",
            "hardware_supported": True,
            "recommended_profile": recommended["profile"],
            "recommended_model": recommended["model"],
            "recommended_label": recommended["label"],
            "message": f"This computer is marginal for local AI. Use {recommended['label']} ({recommended['model']}) only if a {elapsed} ms wait is acceptable.",
        }

    if edge and edge.get("tested") and _result_band(edge) == "too_slow":
        return {
            "verdict": "insufficient_hardware",
            "hardware_supported": False,
            "recommended_profile": "",
            "recommended_model": "",
            "recommended_label": "Disable local AI",
            "message": "This computer does not appear suitable for local MSH AI. Even Edge small (smollm2:360m) was too slow. On old Raspberry Pi-class hardware, disable AI or run the AI service on a stronger computer.",
        }

    if edge and edge.get("tested") and not edge.get("result", {}).get("ok"):
        return {
            "verdict": "benchmark_failed",
            "hardware_supported": False,
            "recommended_profile": "",
            "recommended_model": "",
            "recommended_label": "No safe recommendation",
            "message": "The smallest model failed during benchmarking. Check Ollama logs. If it timed out on old Raspberry Pi-class hardware, disable AI or use a stronger computer.",
        }

    if not edge or not edge.get("installed"):
        choice = AI_MODEL_CHOICES[EDGE_PROFILE]
        return {
            "verdict": "needs_edge_test",
            "hardware_supported": None,
            "recommended_profile": EDGE_PROFILE,
            "recommended_model": choice["model"],
            "recommended_label": choice["label"],
            "message": "No installed model proved usable. Install and test Edge small (smollm2:360m) before deciding that this computer is unsuitable.",
        }

    return {
        "verdict": "no_recommendation",
        "hardware_supported": None,
        "recommended_profile": "",
        "recommended_model": "",
        "recommended_label": "No recommendation",
        "message": "The benchmark did not produce enough information to recommend a model.",
    }


def compare_ollama_setup_models(settings: ServerSetupSettings) -> dict[str, Any]:
    """Benchmark installed standard setup models and recommend a setup profile."""

    if not settings.ai_enabled:
        return {
            "ok": False,
            "rows": [],
            "thresholds": AI_RESPONSE_TIME_BANDS,
            "recommendation": {
                "verdict": "ai_disabled",
                "hardware_supported": None,
                "recommended_profile": "",
                "recommended_model": "",
                "recommended_label": "AI disabled",
                "message": "Enable local AI before benchmarking model choices.",
            },
            "message": "Enable local AI before benchmarking model choices.",
        }

    status = ollama_status(settings, timeout_seconds=2.0)
    if not status.get("running"):
        choice = AI_MODEL_CHOICES[EDGE_PROFILE]
        return {
            "ok": False,
            "rows": [],
            "thresholds": AI_RESPONSE_TIME_BANDS,
            "recommendation": {
                "verdict": "ollama_not_ready",
                "hardware_supported": None,
                "recommended_profile": EDGE_PROFILE,
                "recommended_model": choice["model"],
                "recommended_label": choice["label"],
                "message": "Ollama is not reachable. Start Ollama and test Edge small first on low-power hardware.",
            },
            "message": str(status.get("message") or "Ollama is not reachable."),
        }

    installed_by_profile = dict(status.get("installed_by_profile") or {})
    rows: list[dict[str, Any]] = []
    for profile, choice in AI_MODEL_CHOICES.items():
        installed = bool(installed_by_profile.get(profile))
        if not installed:
            rows.append(_row(profile, installed=False))
            continue
        result = benchmark_ollama_response_time(settings, model=choice["model"])
        rows.append(_row(profile, installed=True, result=result))

    recommendation = _recommend(rows)
    return {
        "ok": True,
        "rows": rows,
        "thresholds": AI_RESPONSE_TIME_BANDS,
        "recommendation": recommendation,
        "message": recommendation["message"],
        "models": status.get("models", []),
    }
