"""Shared HTTP clients for the istSOS migration utilities."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Iterator

import requests


DEFAULT_TIMEOUT = 30
MAX_BULK_SIZE = 2000


def format_entity_id(entity_id: Any) -> str:
    if isinstance(entity_id, bool):
        raise TypeError("Entity ID cannot be a boolean")
    if isinstance(entity_id, int):
        return str(entity_id)
    text = str(entity_id)
    if text.lstrip("-").isdigit():
        return text
    return f"'{text.replace(chr(39), chr(39) * 2)}'"


def escape_odata_string(value: str) -> str:
    return value.replace("'", "''")


def parse_result_value(value: Any) -> Any:
    """Convert numeric strings to numbers and preserve other result values."""
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return value
    return int(number) if number.is_integer() else number


def observation_query_params(
    start: str | None,
    end: str | None,
    select: str,
) -> dict[str, str]:
    filters = []
    if start:
        filters.append(f"phenomenonTime ge {start}")
    if end:
        filters.append(f"phenomenonTime le {end}")
    params = {"$select": select, "$orderby": "phenomenonTime"}
    if filters:
        params["$filter"] = " and ".join(filters)
    return params


class IstSOS2Client:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.verify = False

    def request(self, path: str) -> dict[str, Any]:
        response = self.session.get(
            f"{self.base_url}/{path.lstrip('/')}",
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def get_procedure(self, service: str, procedure: str) -> dict[str, Any]:
        payload = self.request(
            f"wa/istsos/services/{service}/procedures/{procedure}"
        )
        return payload.get("data", {})

    def get_observation_values(
        self,
        service: str,
        procedure: str,
        observed_property: str,
        start: str,
        end: str,
    ) -> list[list[Any]]:
        payload = self.request(
            f"wa/istsos/services/{service}/operations/getobservation/"
            f"offerings/temporary/procedures/{procedure}/"
            f"observedproperties/{observed_property}/eventtime/{start}/{end}"
        )
        data = payload.get("data", [])
        if not data:
            return []
        return data[0].get("result", {}).get("DataArray", {}).get("values", [])


class IstSOS4Client:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout: int = DEFAULT_TIMEOUT,
        refresh_margin_seconds: int = 300,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.timeout = timeout
        self.refresh_margin_seconds = refresh_margin_seconds
        self.session = requests.Session()
        self.access_token: str | None = None
        self.expires_at: datetime | None = None

    def login(self) -> None:
        response = self.session.post(
            f"{self.base_url}/Login",
            data={
                "username": self.username,
                "password": self.password,
                "grant_type": "password",
            },
            timeout=self.timeout,
        )
        self.raise_for_status(response, "Login failed")
        payload = response.json()
        self.access_token = payload["access_token"]
        expires_in = payload.get("expires_in")
        if expires_in is None:
            self.expires_at = None
            return
        expires = int(expires_in)
        if expires < int(time.time()) + 86400:
            expires += int(time.time())
        self.expires_at = datetime.fromtimestamp(expires, timezone.utc)

    def ensure_token(self) -> None:
        refresh_at = (
            self.expires_at.timestamp() - self.refresh_margin_seconds
            if self.expires_at
            else None
        )
        if not self.access_token or (
            refresh_at is not None and time.time() >= refresh_at
        ):
            self.login()

    def request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        url = path if path.startswith(("http://", "https://")) else (
            f"{self.base_url}/{path.lstrip('/')}"
        )
        kwargs.setdefault("timeout", self.timeout)
        supplied_headers = kwargs.pop("headers", {})
        self.ensure_token()
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            **supplied_headers,
        }
        response = self.session.request(method, url, headers=headers, **kwargs)
        if response.status_code == 401:
            self.login()
            headers["Authorization"] = f"Bearer {self.access_token}"
            response = self.session.request(
                method, url, headers=headers, **kwargs
            )
        self.raise_for_status(response)
        return response

    def iter_collection(
        self,
        path: str,
        params: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        next_path: str | None = path
        next_params = params
        while next_path:
            payload = self.request("GET", next_path, params=next_params).json()
            yield from payload.get("value", [])
            next_path = payload.get("@iot.nextLink")
            next_params = None

    def get_network_id(self, network_name: str) -> Any:
        networks = list(
            self.iter_collection(
                "/Networks",
                {"$filter": f"name eq '{escape_odata_string(network_name)}'"},
            )
        )
        if not networks:
            raise ValueError(f"Network not found: {network_name}")
        if len(networks) > 1:
            raise ValueError(f"Multiple networks are named: {network_name}")
        return networks[0]["@iot.id"]

    def get_datastreams(self, network_name: str = "") -> list[dict[str, Any]]:
        if network_name:
            network_id = format_entity_id(self.get_network_id(network_name))
            path = f"/Networks({network_id})/Datastreams"
        else:
            path = "/Datastreams"
        return list(self.iter_collection(path, {"$orderby": "name"}))

    def get_datastream_id(self, name: str) -> Any:
        escaped_name = escape_odata_string(name)
        datastreams = list(
            self.iter_collection(
                "/Datastreams",
                {"$filter": f"name eq '{escaped_name}'"},
            )
        )
        if not datastreams:
            raise ValueError(f"Datastream not found in istSOS4: {name}")
        if len(datastreams) > 1:
            raise ValueError(f"Multiple istSOS4 datastreams are named: {name}")
        return datastreams[0]["@iot.id"]

    def get_observations(
        self,
        datastream_id: Any,
        start: str | None = None,
        end: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        entity_id = format_entity_id(datastream_id)
        yield from self.iter_collection(
            f"/Datastreams({entity_id})/Observations",
            observation_query_params(
                start,
                end,
                "result,phenomenonTime,resultTime,resultQuality",
            ),
        )

    def get_observation_times(
        self,
        datastream_id: Any,
        start: str | None = None,
        end: str | None = None,
    ) -> Iterator[str]:
        entity_id = format_entity_id(datastream_id)
        observations = self.iter_collection(
            f"/Datastreams({entity_id})/Observations",
            observation_query_params(start, end, "phenomenonTime"),
        )
        for observation in observations:
            phenomenon_time = observation.get("phenomenonTime")
            if phenomenon_time:
                yield phenomenon_time

    def post_observations(
        self,
        datastream_id: Any,
        observations: list[dict[str, Any]],
    ) -> int:
        data_array = []
        for observation in observations:
            phenomenon_time = observation["phenomenonTime"]
            data_array.append(
                [
                    parse_result_value(observation.get("result")),
                    phenomenon_time,
                    observation.get("resultTime") or phenomenon_time,
                    str(observation.get("resultQuality")),
                ]
            )
        return self.post_data_array(datastream_id, data_array)

    def post_data_array(
        self,
        datastream_id: Any,
        data_array: list[list[Any]],
    ) -> int:
        inserted = 0
        for offset in range(0, len(data_array), MAX_BULK_SIZE):
            batch = data_array[offset : offset + MAX_BULK_SIZE]
            payload = [
                {
                    "Datastream": {"@iot.id": datastream_id},
                    "components": [
                        "result",
                        "phenomenonTime",
                        "resultTime",
                        "resultQuality",
                    ],
                    "dataArray": batch,
                }
            ]
            self.request(
                "POST",
                "/BulkObservations",
                headers={"Content-type": "application/json"},
                data=json.dumps(payload),
            )
            inserted += len(batch)
        return inserted

    @staticmethod
    def raise_for_status(
        response: requests.Response,
        context: str | None = None,
    ) -> None:
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            message = f"{context}: {exc}" if context else str(exc)
            body = response.text.strip()
            if body:
                message = f"{message}; response body: {body}"
            raise requests.HTTPError(
                message,
                response=response,
                request=response.request,
            ) from exc
