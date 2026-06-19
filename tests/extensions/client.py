"""
client.py -- thin OGC SensorThings API (STA) v1.1 client over httpx.

COPIED VERBATIM from tests/conformance/client.py (per the task: reuse the client,
keep the seed fixtures separate). The NETWORK extension suite is intentionally
isolated from the conformance suite, so it carries its own copy rather than
importing across trees.
"""

from __future__ import annotations

from urllib.parse import quote

import httpx

DEFAULT_BASE_URL = "http://localhost:8018/v4/v1.1"

_SAFE_VALUE = "$()/:,'=;.-"


def _encode_params(params) -> str:
    items = params.items() if isinstance(params, dict) else params
    parts = []
    for key, value in items:
        if value is None:
            parts.append(quote(str(key), safe="$"))
        else:
            parts.append(f"{quote(str(key), safe='$')}={quote(str(value), safe=_SAFE_VALUE)}")
    return "&".join(parts)


def entity_id(entity: dict):
    return entity["@iot.id"]


def self_link(entity: dict) -> str:
    return entity["@iot.selfLink"]


def id_from_self_link(url: str):
    inner = url.rstrip("/").rsplit("(", 1)[-1]
    inner = inner.rsplit(")", 1)[0]
    if len(inner) >= 2 and inner[0] == "'" and inner[-1] == "'":
        return inner[1:-1].replace("''", "'")
    try:
        return int(inner)
    except ValueError:
        return inner


def format_id(eid) -> str:
    if isinstance(eid, bool):
        raise TypeError("entity id cannot be a bool")
    if isinstance(eid, int):
        return str(eid)
    if isinstance(eid, str) and eid.lstrip("-").isdigit():
        return eid
    escaped = str(eid).replace("'", "''")
    return f"'{escaped}'"


class STAClient:
    """Minimal STA client. Verbs return raw httpx.Response; *_json helpers parse."""

    def __init__(self, base_url: str = DEFAULT_BASE_URL, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self._http = httpx.Client(timeout=timeout, follow_redirects=True)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "STAClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def url(self, path: str, params=None) -> str:
        if path.startswith(("http://", "https://")):
            full = path
        else:
            full = f"{self.base_url}/{path.lstrip('/')}"
        if params:
            qs = _encode_params(params)
            if qs:
                full += ("&" if "?" in full else "?") + qs
        return full

    def get(self, path: str, params=None, **kw) -> httpx.Response:
        return self._http.get(self.url(path, params), **kw)

    def post(self, path: str, json=None, params=None, **kw) -> httpx.Response:
        return self._http.post(self.url(path, params), json=json, **kw)

    def patch(self, path: str, json=None, params=None, **kw) -> httpx.Response:
        return self._http.patch(self.url(path, params), json=json, **kw)

    def put(self, path: str, json=None, params=None, **kw) -> httpx.Response:
        return self._http.put(self.url(path, params), json=json, **kw)

    def delete(self, path: str, params=None, **kw) -> httpx.Response:
        return self._http.delete(self.url(path, params), **kw)

    def get_json(self, path: str, params=None, **kw) -> dict:
        resp = self.get(path, params=params, **kw)
        resp.raise_for_status()
        return resp.json()

    def collection(self, name: str, params=None) -> dict:
        return self.get_json(name, params=params)

    def values(self, name: str, params=None) -> list:
        return self.collection(name, params=params).get("value", [])

    def by_id(self, name: str, eid, params=None) -> dict:
        return self.get_json(f"{name}({format_id(eid)})", params=params)

    def nav(self, path: str, params=None) -> dict:
        return self.get_json(path, params=params)

    def follow_self_link(self, entity: dict, params=None) -> dict:
        return self.get_json(self_link(entity), params=params)

    def create(self, collection: str, payload: dict) -> httpx.Response:
        return self.post(collection, json=payload)

    def location_of(self, resp: httpx.Response) -> str:
        return resp.headers["location"]
