"""Read-only HTTP client for a legacy istSOS2 service.

istSOS2 speaks its own REST API, not SensorThings, so istsos4_client cannot
talk to it. Everything on the istSOS4 side goes through istsos4_client.Client.
"""

from __future__ import annotations

from typing import Any

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_TIMEOUT = 60
# Read-only GETs: retry transient gateway errors (~1 min of backoff in total).
RETRY = Retry(total=5, backoff_factor=2, status_forcelist=(502, 503, 504))

# TLS verification is disabled on purpose (verify = False below).
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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
        adapter = HTTPAdapter(max_retries=RETRY)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

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
