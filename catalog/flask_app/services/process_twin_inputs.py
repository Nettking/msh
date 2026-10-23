"""Read DT bindings from the existing source inventory, without registration.

This adapter reuses FCP's existing local data.read policy. The optional private
inventory entries describe data locations and identity, not grants or copied
Federation authority. Shared/operator authority requires its owner's explicit
local read adapter through the extension hook; a local cache never substitutes.
"""

from __future__ import annotations

from pathlib import Path

from catalog.digital_twin.adapters import InputRegistry, InputSource
from catalog.digital_twin.contracts import TwinError, require, revision

from .source_inventory_service import SourceInventoryError, SourceInventoryService


class InventoryInputRegistry(InputRegistry):
    def __init__(self, data_root, authorize, blocked_roots=()):
        self.root = Path(data_root)
        self.authorize_local_data = authorize
        self.blocked_roots = (
            self.root / "federation" / "shared",
            *map(Path, blocked_roots),
        )

    def _load(self):
        path = self.root / "source_config" / "machines_and_sensors.json"
        require(path.is_file(), "INPUT_REGISTRY_UNAVAILABLE", 503)
        try:
            inventory = SourceInventoryService(path).load()
            require(
                inventory.get("schema") == "fcp.source_inventory.v1",
                "INPUT_REGISTRY_UNAVAILABLE",
                503,
            )
            machines = {v["id"]: v for v in inventory["machines"]}
            entries = inventory.get("process_twin_inputs", [])
            require(isinstance(entries, list), "INPUT_REGISTRY_UNAVAILABLE", 503)
            sources = []
            for entry in entries:
                require(
                    isinstance(entry, dict) and entry.get("machine_id") in machines,
                    "INPUT_REGISTRY_UNAVAILABLE",
                    503,
                )
                require(
                    entry.get("kind") in ("CANONICAL_DEVICE", "LEGACY_JSONL"),
                    "INPUT_REGISTRY_UNAVAILABLE",
                    503,
                )
                require(
                    isinstance(entry.get("relative_paths"), list)
                    and all(isinstance(v, str) for v in entry["relative_paths"]),
                    "INPUT_REGISTRY_UNAVAILABLE",
                    503,
                )
                require(
                    isinstance(entry.get("binding"), dict),
                    "INPUT_REGISTRY_UNAVAILABLE",
                    503,
                )
                paths = tuple(
                    self.root / relative for relative in entry["relative_paths"]
                )
                require(
                    not any(
                        path.resolve().is_relative_to(blocked.resolve())
                        for path in paths
                        for blocked in self.blocked_roots
                    ),
                    "INPUT_REGISTRY_UNAVAILABLE",
                    503,
                )
                sources.append(
                    InputSource(
                        input_id=entry["input_id"],
                        binding_revision=revision(entry),
                        kind=entry["kind"],
                        display_name=machines[entry["machine_id"]]["name"],
                        root=self.root,
                        paths=paths,
                        binding=entry["binding"],
                        authorize=self.authorize_local_data,
                        utc_verified=entry.get("clock_binding") == "UTC_NORMALIZED",
                        origin_status=entry.get("origin_status", "UNKNOWN"),
                    )
                )
            return InputRegistry(sources)
        except (
            SourceInventoryError,
            OSError,
            KeyError,
            TypeError,
            ValueError,
            TwinError,
        ):
            raise TwinError("INPUT_REGISTRY_UNAVAILABLE", 503) from None

    def get(self, input_id, actor, record=None):
        return self._load().get(input_id, actor, record)

    def list(self, actor):
        return self._load().list(actor)

    def check_scopes(self, scopes, actor):
        self._load().check_scopes(scopes, actor)
