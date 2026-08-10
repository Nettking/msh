"""Bounded MTConnect discovery for recorder setup.

Discovery is deliberately limited to an explicit, private IPv4 subnet. The
service probes the standard MTConnect ``/probe`` endpoint concurrently, derives
machine identity from the returned device model, and stores only the latest
scan summary. It does not start recording or grant recorder authority; selected
results only update recorder configuration.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import datetime, timezone
from hashlib import sha256
from ipaddress import IPv4Address, IPv4Network, ip_address, ip_network
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import requests

from catalog.mtconnect_recorder.parsing import machine_display_name, parse_probe

from .capability_config_service import (
    CAPABILITY_CONFIG_PATH,
    CapabilityConfig,
    CapabilityConfigError,
    format_recorder_sources,
    load_capability_config,
    parse_recorder_sources,
)

DEFAULT_SCAN_PATH = Path("data") / "source_state" / "mtconnect_network_scan.json"
DEFAULT_CHECKPOINT_PATH = (
    Path("data") / "source_state" / "mtconnect_recorder_state.json"
)
DEFAULT_MTCONNECT_PORT = 5000
MAX_SCAN_ADDRESSES = 256
MAX_SCAN_WORKERS = 32
DEFAULT_CONNECT_TIMEOUT_SECONDS = 0.35
DEFAULT_READ_TIMEOUT_SECONDS = 1.5
MAX_CONNECT_TIMEOUT_SECONDS = 1.0
MAX_READ_TIMEOUT_SECONDS = 3.0
MAX_PROBE_BYTES = 8 * 1024 * 1024
SCAN_SCHEMA = "fcp.mtconnect.network_scan.v1"

_RFC1918_NETWORKS = (
    IPv4Network("10.0.0.0/8"),
    IPv4Network("172.16.0.0/12"),
    IPv4Network("192.168.0.0/16"),
)

ProbeFetcher = Callable[[str, float, float], str]


class MtconnectDiscoveryError(RuntimeError):
    """Raised when discovery input or a selected result is unsafe/invalid."""


def validate_scan_cidr(value: str) -> IPv4Network:
    """Return a safe scan network.

    Only explicit RFC1918 IPv4 CIDRs with at most 256 total addresses are
    accepted. A ``/24`` is the largest allowed network; narrower subnets such
    as ``/25`` or a single ``/32`` are also valid.
    """

    raw = str(value or "").strip()
    if "/" not in raw:
        raise MtconnectDiscoveryError(
            "Enter an explicit private IPv4 CIDR, for example "
            "192.168.200.0/24."
        )
    try:
        network = ip_network(raw, strict=False)
    except ValueError as exc:
        raise MtconnectDiscoveryError(
            "The machine network must be a valid private IPv4 CIDR."
        ) from exc
    if not isinstance(network, IPv4Network):
        raise MtconnectDiscoveryError(
            "Network discovery currently supports private IPv4 CIDRs only."
        )
    if network.prefixlen < 24 or network.num_addresses > MAX_SCAN_ADDRESSES:
        raise MtconnectDiscoveryError(
            "Network discovery is limited to a /24 or smaller subnet "
            "(at most 256 addresses)."
        )
    if not any(network.subnet_of(private) for private in _RFC1918_NETWORKS):
        raise MtconnectDiscoveryError(
            "Network discovery is limited to private RFC1918 IPv4 addresses."
        )
    return network


def _bounded_timeout(value: float, *, maximum: float, minimum: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = minimum
    return max(minimum, min(parsed, maximum))


def _validated_port(value: int | str) -> int:
    try:
        port = int(value)
    except (TypeError, ValueError) as exc:
        raise MtconnectDiscoveryError("The MTConnect port must be a number.") from exc
    if not 1 <= port <= 65535:
        raise MtconnectDiscoveryError(
            "The MTConnect port must be between 1 and 65535."
        )
    return port


def _utc_now_precise() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _default_probe_fetcher(
    probe_url: str,
    connect_timeout_seconds: float,
    read_timeout_seconds: float,
) -> str:
    """Fetch one bounded MTConnect device model without following redirects."""

    with requests.get(
        probe_url,
        headers={"User-Agent": "FCP-MTConnect-discovery/1.0"},
        timeout=(connect_timeout_seconds, read_timeout_seconds),
        allow_redirects=False,
        stream=True,
    ) as response:
        response.raise_for_status()
        chunks: list[bytes] = []
        total = 0
        for chunk in response.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue
            total += len(chunk)
            if total > MAX_PROBE_BYTES:
                raise MtconnectDiscoveryError(
                    "MTConnect /probe response exceeded the discovery size limit."
                )
            chunks.append(chunk)
        body = b"".join(chunks)
        encoding = response.encoding or "utf-8"
        return body.decode(encoding, errors="replace")


def _text(value: object) -> str:
    return str(value or "").strip()


def _slug(value: str, *, fallback: str = "MTConnect") -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-._")
    return (cleaned or fallback)[:80]


def _short_digest(value: str, length: int = 12) -> str:
    return sha256(value.encode("utf-8")).hexdigest()[:length]


def _machine_from_device(
    device: Mapping[str, Any],
    *,
    host: str,
    port: int,
) -> dict[str, Any]:
    device_uuid = _text(device.get("uuid"))
    serial_number = _text(
        device.get("serial_number")
        or (device.get("description_attributes") or {}).get("serialNumber")
    )
    device_id = _text(device.get("id"))
    reported_name = _text(device.get("name"))

    if device_uuid:
        identity_kind = "uuid"
        identity_value = device_uuid
        machine_id = device_uuid
    elif serial_number:
        identity_kind = "serial"
        identity_value = serial_number
        machine_id = f"serial:{serial_number}"
    elif device_id:
        identity_kind = "device_id"
        identity_value = f"{device_id}@{host}:{port}"
        machine_id = f"mtconnect-device:{identity_value}"
    else:
        identity_kind = "host"
        identity_value = f"{host}:{port}"
        machine_id = f"mtconnect-host:{identity_value}"

    display_name = machine_display_name(
        device_name=reported_name,
        serial_number=serial_number,
        machine_id=identity_value,
        fallback=f"MTConnect {host}:{port}",
    )
    description_attributes = device.get("description_attributes")
    if not isinstance(description_attributes, Mapping):
        description_attributes = {}

    return {
        "machine_id": machine_id,
        "identity_kind": identity_kind,
        "identity_value": identity_value,
        "identity_key": f"{identity_kind}:{identity_value}",
        "display_name": display_name,
        "reported_name": reported_name,
        "device_uuid": device_uuid,
        "serial_number": serial_number,
        "device_id": device_id,
        "description": _text(device.get("description")),
        "manufacturer": _text(description_attributes.get("manufacturer")),
        "model": _text(description_attributes.get("model")),
        "station": _text(description_attributes.get("station")),
        "host": host,
        "port": port,
    }


def _source_name_for(machines: list[dict[str, Any]]) -> str:
    if len(machines) == 1:
        return _slug(str(machines[0]["identity_value"]))
    identity_material = "|".join(
        sorted(str(machine["identity_key"]) for machine in machines)
    )
    return f"MTConnect-{_short_digest(identity_material)}"


def _ensure_unique_names(results: list[dict[str, Any]]) -> None:
    """Disambiguate bad duplicate device metadata without changing identity."""

    display_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    for result in results:
        source_key = str(result["source_name"]).casefold()
        source_counts[source_key] = source_counts.get(source_key, 0) + 1
        for machine in result["machines"]:
            display_key = str(machine["display_name"]).casefold()
            display_counts[display_key] = display_counts.get(display_key, 0) + 1

    used_source_names: set[str] = set()
    for result in results:
        for machine in result["machines"]:
            key = str(machine["display_name"]).casefold()
            if display_counts.get(key, 0) > 1:
                machine["display_name"] = (
                    f"{machine['display_name']} @ "
                    f"{result['host']}:{result['port']}"
                )

        original = str(result["source_name"])
        if source_counts.get(original.casefold(), 0) > 1:
            original = _slug(f"{original}-{result['host']}-{result['port']}")
        candidate = original
        if candidate.casefold() in used_source_names:
            candidate = _slug(
                f"{original}-{_short_digest(str(result['source_id']), 8)}"
            )
        result["source_name"] = candidate
        used_source_names.add(candidate.casefold())

        if len(result["machines"]) == 1:
            result["display_name"] = result["machines"][0]["display_name"]


def _private_ipv4_cidr_from_url(url: str) -> str:
    try:
        host = urlsplit(str(url or "")).hostname
        address = ip_address(host or "")
    except ValueError:
        return ""
    if not isinstance(address, IPv4Address):
        return ""
    if not any(address in private for private in _RFC1918_NETWORKS):
        return ""
    return str(ip_network(f"{address}/24", strict=False))


class MtconnectDiscoveryService:
    """Scan a small machine subnet and manage explicit result selection."""

    def __init__(
        self,
        *,
        scan_path: Path | str = DEFAULT_SCAN_PATH,
        config_path: Path | str = CAPABILITY_CONFIG_PATH,
        checkpoint_path: Path | str = DEFAULT_CHECKPOINT_PATH,
        probe_fetcher: ProbeFetcher | None = None,
        max_workers: int = MAX_SCAN_WORKERS,
        connect_timeout_seconds: float = DEFAULT_CONNECT_TIMEOUT_SECONDS,
        read_timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS,
    ) -> None:
        self.scan_path = Path(scan_path)
        self.config_path = Path(config_path)
        self.checkpoint_path = Path(checkpoint_path)
        self.probe_fetcher = probe_fetcher or _default_probe_fetcher
        self.max_workers = max(1, min(int(max_workers), MAX_SCAN_WORKERS))
        self.connect_timeout_seconds = _bounded_timeout(
            connect_timeout_seconds,
            maximum=MAX_CONNECT_TIMEOUT_SECONDS,
            minimum=0.05,
        )
        self.read_timeout_seconds = _bounded_timeout(
            read_timeout_seconds,
            maximum=MAX_READ_TIMEOUT_SECONDS,
            minimum=0.1,
        )

    def scan(
        self,
        cidr: str,
        *,
        port: int | str = DEFAULT_MTCONNECT_PORT,
    ) -> dict[str, Any]:
        """Probe every usable address and persist a complete latest-scan model."""

        network = validate_scan_cidr(cidr)
        scan_port = _validated_port(port)
        addresses = list(network.hosts())
        started_at = _utc_now_precise()
        running = {
            "schema": SCAN_SCHEMA,
            "state": "running",
            "cidr": str(network),
            "port": scan_port,
            "started_at": started_at,
            "finished_at": "",
            "network_address_count": network.num_addresses,
            "addresses_scanned": 0,
            "failure_count": 0,
            "agents_found": 0,
            "machines_found": 0,
            "results": [],
        }
        self._write_scan(running)

        results: list[dict[str, Any]] = []
        failures = 0
        workers = min(self.max_workers, max(1, len(addresses)))
        with ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="mtconnect-discovery",
        ) as executor:
            futures = {
                executor.submit(
                    self._discover_address,
                    str(address),
                    scan_port,
                ): address
                for address in addresses
            }
            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception:  # noqa: BLE001 - one host must not abort the scan
                    failures += 1
                    continue
                if result is None:
                    failures += 1
                    continue
                results.append(result)

        results.sort(
            key=lambda item: (IPv4Address(item["host"]), item["port"])
        )
        _ensure_unique_names(results)
        machines_found = sum(len(result["machines"]) for result in results)
        payload = {
            **running,
            "state": "complete",
            "finished_at": _utc_now_precise(),
            "addresses_scanned": len(addresses),
            "failure_count": failures,
            "agents_found": len(results),
            "machines_found": machines_found,
            "results": results,
        }
        self._write_scan(payload)
        return payload

    def scan_network(
        self,
        cidr: str,
        *,
        port: int | str = DEFAULT_MTCONNECT_PORT,
    ) -> dict[str, Any]:
        """Compatibility alias with a UI-oriented name."""

        return self.scan(cidr, port=port)

    def _discover_address(self, host: str, port: int) -> dict[str, Any] | None:
        base_url = f"http://{host}:{port}"
        probe_url = f"{base_url}/probe"
        xml_text = self.probe_fetcher(
            probe_url,
            self.connect_timeout_seconds,
            self.read_timeout_seconds,
        )
        if isinstance(xml_text, bytes):
            xml_text = xml_text.decode("utf-8", errors="replace")
        probe = parse_probe(str(xml_text))
        machines = [
            _machine_from_device(device, host=host, port=port)
            for device in probe.devices.values()
            if isinstance(device, Mapping)
        ]
        if not machines:
            return None
        machines.sort(
            key=lambda item: (
                str(item["display_name"]).casefold(),
                str(item["identity_key"]),
            )
        )
        identities = "|".join(
            sorted(str(machine["identity_key"]) for machine in machines)
        )
        source_id = (
            "mtconnect-source-"
            + _short_digest(f"{base_url}|{identities}", length=16)
        )
        return {
            "source_id": source_id,
            "source_name": _source_name_for(machines),
            "display_name": (
                machines[0]["display_name"]
                if len(machines) == 1
                else f"{len(machines)} MTConnect devices at {host}:{port}"
            ),
            "base_url": base_url,
            "probe_url": probe_url,
            "host": host,
            "port": port,
            "probe_sha256": probe.sha256,
            "machine_count": len(machines),
            "machines": machines,
        }

    def last_scan(self) -> dict[str, Any]:
        if not self.scan_path.exists():
            return {
                "schema": SCAN_SCHEMA,
                "state": "never",
                "results": [],
            }
        try:
            payload = json.loads(self.scan_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {
                "schema": SCAN_SCHEMA,
                "state": "unavailable",
                "message": "The saved MTConnect network scan could not be read.",
                "results": [],
            }
        if not isinstance(payload, dict):
            return {
                "schema": SCAN_SCHEMA,
                "state": "unavailable",
                "message": "The saved MTConnect network scan has an invalid shape.",
                "results": [],
            }
        payload.setdefault("schema", SCAN_SCHEMA)
        payload.setdefault("results", [])
        return payload

    def recommended_cidr(self, config: CapabilityConfig | None = None) -> str:
        """Derive a safe /24 from scan history, config, or recorder checkpoint."""

        previous = self.last_scan()
        previous_cidr = _text(previous.get("cidr"))
        if previous_cidr:
            try:
                return str(validate_scan_cidr(previous_cidr))
            except MtconnectDiscoveryError:
                pass

        if config is None:
            try:
                config = load_capability_config(self.config_path)
            except (CapabilityConfigError, OSError):
                config = None
        if config is not None:
            try:
                configured = parse_recorder_sources(config.recorder_sources)
            except CapabilityConfigError:
                configured = {}
            for url in configured.values():
                candidate = _private_ipv4_cidr_from_url(url)
                if candidate:
                    return candidate

        checkpoint = self._read_json(self.checkpoint_path)
        sources = checkpoint.get("sources")
        if isinstance(sources, Mapping):
            for name in sorted(sources, key=lambda value: str(value).casefold()):
                item = sources.get(name)
                if not isinstance(item, Mapping):
                    continue
                candidate = _private_ipv4_cidr_from_url(_text(item.get("base_url")))
                if candidate:
                    return candidate
        return ""

    def merge_selected_results(
        self,
        config: CapabilityConfig,
        selected: Iterable[str | Mapping[str, Any]],
        *,
        scan: Mapping[str, Any] | None = None,
    ) -> CapabilityConfig:
        """Merge explicitly selected Agents into recorder configuration only.

        The operation never grants recorder authority. Capability/contribution
        state independently decides whether recording may be activated.
        """

        selected_values = list(selected)
        if not selected_values:
            raise MtconnectDiscoveryError(
                "Select at least one discovered MTConnect source."
            )

        scan_payload = dict(scan or self.last_scan())
        results = scan_payload.get("results")
        if not isinstance(results, list):
            results = []
        by_id = {
            _text(result.get("source_id")): result
            for result in results
            if isinstance(result, Mapping) and _text(result.get("source_id"))
        }
        by_name = {
            _text(result.get("source_name")): result
            for result in results
            if isinstance(result, Mapping) and _text(result.get("source_name"))
        }

        resolved: list[Mapping[str, Any]] = []
        for item in selected_values:
            if isinstance(item, Mapping):
                resolved.append(item)
                continue
            key = _text(item)
            result = by_id.get(key) or by_name.get(key)
            if result is None:
                raise MtconnectDiscoveryError(
                    "The selected discovery result is no longer available: "
                    f"{key}"
                )
            resolved.append(result)

        try:
            merged = parse_recorder_sources(config.recorder_sources)
        except CapabilityConfigError as exc:
            raise MtconnectDiscoveryError(str(exc)) from exc
        for result in resolved:
            name = _text(result.get("source_name"))
            base_url = _text(result.get("base_url"))
            if not name or not base_url:
                raise MtconnectDiscoveryError(
                    "A selected discovery result is missing its source identity "
                    "or MTConnect URL."
                )
            for existing_name, existing_url in list(merged.items()):
                if existing_url.casefold() == base_url.casefold():
                    del merged[existing_name]
            if name in merged and merged[name] != base_url:
                name = _slug(f"{name}-{_short_digest(base_url, length=8)}")
            if name in merged and merged[name] != base_url:
                raise MtconnectDiscoveryError(
                    "Could not create a unique recorder source name for "
                    f"{base_url}."
                )
            merged[name] = base_url

        return replace(
            config,
            recorder_sources=format_recorder_sources(merged),
            updated_at=_utc_now_precise(),
        )

    def merge_selected_sources(
        self,
        config: CapabilityConfig,
        selected: Iterable[str | Mapping[str, Any]],
        *,
        scan: Mapping[str, Any] | None = None,
    ) -> CapabilityConfig:
        """Compatibility alias for recorder-configuration callers."""

        return self.merge_selected_results(config, selected, scan=scan)

    def _write_scan(self, payload: Mapping[str, Any]) -> None:
        self.scan_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.scan_path.with_name(self.scan_path.name + ".tmp")
        temporary.write_text(
            json.dumps(
                dict(payload),
                indent=2,
                sort_keys=True,
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )
        temporary.replace(self.scan_path)

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}


_DISCOVERY_SERVICE: MtconnectDiscoveryService | None = None


def get_mtconnect_discovery_service() -> MtconnectDiscoveryService:
    global _DISCOVERY_SERVICE
    if _DISCOVERY_SERVICE is None:
        _DISCOVERY_SERVICE = MtconnectDiscoveryService()
    return _DISCOVERY_SERVICE
