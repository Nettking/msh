"""HTTP client for the SKF Observer Phoenix Data Service REST API."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import os
from typing import Any

import requests

from catalog.common.source_sync import format_utc


DEFAULT_TIMEOUT_SECONDS = 30


class ObserverPhoenixError(RuntimeError):
    """Raised when Observer Phoenix authentication or API calls fail."""


@dataclass
class ObserverPhoenixConfig:
    base_url: str
    username: str
    password: str
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    verify_tls: bool = True

    @classmethod
    def from_env(cls) -> "ObserverPhoenixConfig":
        base_url = os.getenv("OBSERVER_PHOENIX_BASE_URL", "").strip()
        username = os.getenv("OBSERVER_PHOENIX_USERNAME", "").strip()
        password = os.getenv("OBSERVER_PHOENIX_PASSWORD", "")
        verify_text = os.getenv("OBSERVER_PHOENIX_VERIFY_TLS", "true").strip().lower()
        timeout = int(os.getenv("OBSERVER_PHOENIX_TIMEOUT_SECONDS", str(DEFAULT_TIMEOUT_SECONDS)))
        if not base_url or not username or not password:
            raise ObserverPhoenixError(
                "Set OBSERVER_PHOENIX_BASE_URL, OBSERVER_PHOENIX_USERNAME, "
                "and OBSERVER_PHOENIX_PASSWORD before using the connector."
            )
        return cls(
            base_url=base_url,
            username=username,
            password=password,
            timeout_seconds=timeout,
            verify_tls=verify_text not in {"0", "false", "no"},
        )


class ObserverPhoenixClient:
    """Minimal authenticated API client.

    The API uses ``POST /token`` with username/password credentials and then a
    bearer token in the ``Authorization`` header for subsequent requests. The
    client refreshes the token once if a request receives HTTP 401.
    """

    def __init__(self, config: ObserverPhoenixConfig, session: requests.Session | None = None):
        self.config = config
        self.session = session or requests.Session()
        self.access_token: str | None = None
        self.refresh_token: str | None = None

    @classmethod
    def from_env(cls) -> "ObserverPhoenixClient":
        return cls(ObserverPhoenixConfig.from_env())

    def _url(self, path: str) -> str:
        return f"{self.config.base_url.rstrip('/')}/{path.lstrip('/')}"

    def authenticate(self, *, force: bool = False) -> None:
        if self.access_token and not force:
            return
        payload = {
            "username": self.config.username,
            "password": self.config.password,
            "grant_type": "password",
        }
        response = self.session.post(
            self._url("/token"),
            data=payload,
            timeout=self.config.timeout_seconds,
            verify=self.config.verify_tls,
        )
        if response.status_code >= 400:
            raise ObserverPhoenixError(f"Observer Phoenix authentication failed: {response.status_code} {response.text}")
        self._store_token_response(response.json())

    def refresh_access_token(self) -> None:
        if not self.refresh_token:
            self.authenticate(force=True)
            return
        response = self.session.post(
            self._url("/token"),
            data={"grant_type": "refresh_token", "refresh_token": self.refresh_token},
            timeout=self.config.timeout_seconds,
            verify=self.config.verify_tls,
        )
        if response.status_code >= 400:
            self.authenticate(force=True)
            return
        self._store_token_response(response.json())

    def _store_token_response(self, payload: dict[str, Any]) -> None:
        access_token = payload.get("access_token")
        if not access_token:
            raise ObserverPhoenixError("Observer Phoenix token response did not include access_token.")
        self.access_token = str(access_token)
        refresh_token = payload.get("refresh_token")
        if refresh_token:
            self.refresh_token = str(refresh_token)

    def request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        retry_on_unauthorized: bool = True,
    ) -> Any:
        self.authenticate()
        headers = {"Authorization": f"Bearer {self.access_token}"}
        if json_body is not None:
            headers["Content-Type"] = "application/json"
        response = self.session.request(
            method.upper(),
            self._url(path),
            headers=headers,
            params=params,
            json=json_body,
            timeout=self.config.timeout_seconds,
            verify=self.config.verify_tls,
        )
        if response.status_code == 401 and retry_on_unauthorized:
            self.refresh_access_token()
            return self.request_json(method, path, params=params, json_body=json_body, retry_on_unauthorized=False)
        if response.status_code >= 400:
            raise ObserverPhoenixError(f"Observer Phoenix API request failed: {response.status_code} {response.text}")
        if not response.content:
            return None
        return response.json()

    def list_machines(self) -> list[dict[str, Any]]:
        return list(self.request_json("GET", "/v1/machines/") or [])

    def list_machine_points(self, machine_id: int | str) -> list[dict[str, Any]]:
        params = {
            "IncludeLastMeasurement": "false",
            "IncludeDiagnoses": "false",
            "IncludeAlarmInfo": "false",
            "IncludeOverallAlarm": "false",
            "IncludeFrequencies": "false",
            "IncludeStatus": "false",
        }
        return list(self.request_json("GET", f"/v1/machines/{machine_id}/points/", params=params) or [])

    def trend_measurements(
        self,
        point_id: int | str,
        *,
        from_utc: datetime | str | None = None,
        to_utc: datetime | str | None = None,
        num_readings: int | None = None,
        direction_number: int | None = None,
        include_alarm_info: bool = False,
        descending: bool = False,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "includeAlarmInfo": str(include_alarm_info).lower(),
            "descending": str(descending).lower(),
        }
        if from_utc is not None:
            params["fromDateUTC"] = format_utc(from_utc)
        if to_utc is not None:
            params["toDateUTC"] = format_utc(to_utc)
        if num_readings is not None:
            params["numReadings"] = int(num_readings)
        if direction_number is not None:
            params["directionNumber"] = int(direction_number)
        return list(self.request_json("GET", f"/v1/points/{point_id}/trendMeasurements", params=params) or [])

    def alarms(
        self,
        *,
        point_id: int | str | None = None,
        from_utc: datetime | str | None = None,
        to_utc: datetime | str | None = None,
        max_number_of_alarms: int | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if point_id is not None:
            params["pointId"] = point_id
        if from_utc is not None:
            params["fromDateUTC"] = format_utc(from_utc)
        if to_utc is not None:
            params["toDateUTC"] = format_utc(to_utc)
        if max_number_of_alarms is not None:
            params["maxNumberOfAlarms"] = int(max_number_of_alarms)
        return list(self.request_json("GET", "/v2/alarms", params=params) or [])
