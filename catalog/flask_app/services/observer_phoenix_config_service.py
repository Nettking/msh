"""UI-facing service for Observer Phoenix connector settings."""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

from catalog.observer_phoenix.client import ObserverPhoenixClient, ObserverPhoenixError
from catalog.observer_phoenix.settings import (
    clear_local_settings,
    resolve_runtime_config,
    save_last_test,
    save_local_settings,
    snapshot,
)


class ObserverPhoenixConfigService:
    def __init__(self, data_dir: Path | str = Path("data")) -> None:
        self.data_dir = Path(data_dir)

    def status_model(self) -> dict[str, Any]:
        return asdict(snapshot(self.data_dir))

    def save_from_form(self, form: Any) -> tuple[bool, str]:
        try:
            timeout = int(form.get("timeout_seconds") or 30)
            save_local_settings(
                data_dir=self.data_dir,
                base_url=form.get("base_url", ""),
                username=form.get("username", ""),
                password=form.get("password", ""),
                verify_tls=form.get("verify_tls") == "on",
                timeout_seconds=timeout,
                keep_existing_password=True,
            )
        except (ObserverPhoenixError, ValueError) as exc:
            return False, str(exc)
        return True, "Observer Phoenix credentials saved to local runtime settings."

    def clear_local(self) -> tuple[bool, str]:
        clear_local_settings(self.data_dir)
        return True, "Local Observer Phoenix settings cleared. Environment variables may still configure the connector."

    def test_connection(self) -> tuple[bool, str]:
        try:
            config, source = resolve_runtime_config(self.data_dir)
            client = ObserverPhoenixClient(config)
            machines = client.list_machines()
            message = f"Connected to Observer Phoenix using {source} settings. Machines returned: {len(machines)}."
            save_last_test(self.data_dir, connected=True, message=message, machine_count=len(machines), source=source)
            return True, message
        except Exception as exc:  # noqa: BLE001
            message = f"Observer Phoenix connection failed: {exc}"
            try:
                save_last_test(self.data_dir, connected=False, message=message, machine_count=None, source="unknown")
            except Exception:
                pass
            return False, message
