from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def _text(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_recorder_defaults_are_site_neutral() -> None:
    model = _text("catalog/mtconnect_recorder/model.py")
    live = _text("catalog/flask_app/services/live_service.py")
    tailscale = _text("scripts/start_tailscale_recorder.py")

    assert "DEFAULT_SOURCES: dict[str, str] = {}" in model
    assert "DEFAULT_MACHINES: tuple[str, ...] = ()" in live
    assert 'DEFAULT_DEVICE_NAME: Final = "FCP MTConnect recorder"' in tailscale


def test_release_facing_product_and_docs_do_not_embed_deployment_identity() -> None:
    paths = [
        "scripts/start_tailscale_recorder.py",
        "start_recorder.py",
        "catalog/mtconnect_recorder/model.py",
        "catalog/flask_app/services/live_service.py",
        "docs/standalone_recorder.md",
        "docs/quick_start.md",
        "docs/recording_control.md",
        "docs/server_setup.md",
        "docs/termux_phone.md",
        "catalog/standalone-recorder_v2/README.md",
    ]
    combined = "\n".join(_text(path) for path in paths)
    forbidden = (
        "Mekanisk Service " + "Halden",
        "Maskin " + "4",
        "192.168.200" + ".",
    )
    for marker in forbidden:
        assert marker not in combined
