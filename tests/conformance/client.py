"""
client.py -- a thin OGC SensorThings API (STA) v1.1 client over httpx.

Shared by every conformance test file. It exists so the test authors never
fight URL joining or query-string encoding, and so id handling stays agnostic
to the underlying id type (integer in istSOS4 today, but tests must not assume).

Design notes
------------
* URL building is explicit (no reliance on httpx base_url RFC-3986 join, which
  drops the last path segment when the base lacks a trailing slash). Absolute
  URLs (self/next/navigation links the server emits) are passed through as-is.
* Query options are encoded manually with percent-encoding using a safe set
  that keeps STA syntax readable ( $ ( ) / : , ' = ; . - ) and turns spaces
  into %20 (NOT '+', which some servers mis-decode inside $filter). The server
  emits %24/%20-style links itself, so this round-trips cleanly.
* `params` may be a dict or a list of (key, value) pairs (use the list form when
  order matters or a key repeats).
"""

from __future__ import annotations

from urllib.parse import quote

import httpx

DEFAULT_BASE_URL = "http://localhost:8018/v4/v1.1"

# Characters left un-escaped in query VALUES. Space is deliberately excluded so
# it becomes %20. Single quotes (string literals) and STA punctuation are kept
# literal for readability; the server accepts both literal and percent forms.
_SAFE_VALUE = "$()/:,'=;.-"


def encode_params(params) -> str:
    """Encode a dict or list of (k, v) pairs into a query string (no leading '?')."""
    items = params.items() if isinstance(params, dict) else params
    parts = []
    for key, value in items:
        if value is None:
            parts.append(quote(str(key), safe="$"))
        else:
            parts.append(f"{quote(str(key), safe='$')}={quote(str(value), safe=_SAFE_VALUE)}")
    return "&".join(parts)


def entity_id(entity: dict):
    """Return the @iot.id of an entity dict (id-type-agnostic: int or str)."""
    return entity["@iot.id"]


def self_link(entity: dict) -> str:
    """Return the absolute @iot.selfLink of an entity dict."""
    return entity["@iot.selfLink"]


def id_from_self_link(url: str):
    """Extract the id segment from a selfLink / Location header, e.g.
    'http://h/Things(266)' -> 266 (int) ; "Things('abc')" -> 'abc' (str).

    Falls back to the raw inner text when it is neither a bare int nor a
    single-quoted string, keeping the helper id-type-agnostic.
    """
    inner = url.rstrip("/").rsplit("(", 1)[-1]
    inner = inner.rsplit(")", 1)[0]
    if len(inner) >= 2 and inner[0] == "'" and inner[-1] == "'":
        return inner[1:-1].replace("''", "'")
    try:
        return int(inner)
    except ValueError:
        return inner


def format_id(eid) -> str:
    """Render an id for use inside a resource path: Things(<here>).

    Integers/numerics go bare; strings are single-quoted per OData/STA. This is
    the inverse of id_from_self_link and keeps path building id-type-agnostic.
    """
    if isinstance(eid, bool):  # guard: bool is a subclass of int
        raise TypeError("entity id cannot be a bool")
    if isinstance(eid, int):
        return str(eid)
    if isinstance(eid, str) and eid.lstrip("-").isdigit():
        return eid
    escaped = str(eid).replace("'", "''")
    return f"'{escaped}'"


class STAClient:
    """Minimal STA client. Verbs return raw httpx.Response; *_json helpers parse.

    Test authors assert on status AND body, so the verb methods never raise on
    HTTP error status by themselves -- only the explicit `*_json` helpers call
    raise_for_status (use them for "this must succeed" reads).
    """

    def __init__(self, base_url: str = DEFAULT_BASE_URL, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self._http = httpx.Client(timeout=timeout, follow_redirects=True)

    # -- lifecycle --------------------------------------------------------
    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "STAClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -- url building -----------------------------------------------------
    def url(self, path: str, params=None) -> str:
        if path.startswith(("http://", "https://")):
            full = path
        else:
            full = f"{self.base_url}/{path.lstrip('/')}"
        if params:
            qs = encode_params(params)
            if qs:
                full += ("&" if "?" in full else "?") + qs
        return full

    # -- raw verbs --------------------------------------------------------
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

    # -- json read helpers (raise on non-2xx) -----------------------------
    def get_json(self, path: str, params=None, **kw) -> dict:
        resp = self.get(path, params=params, **kw)
        resp.raise_for_status()
        return resp.json()

    def collection(self, name: str, params=None) -> dict:
        """GET /<name> and return the parsed collection document."""
        return self.get_json(name, params=params)

    def values(self, name: str, params=None) -> list:
        """GET /<name> and return its `value` array (empty list if absent)."""
        return self.collection(name, params=params).get("value", [])

    def by_id(self, name: str, eid, params=None) -> dict:
        """GET /<name>(<id>) -> entity document."""
        return self.get_json(f"{name}({format_id(eid)})", params=params)

    def nav(self, path: str, params=None) -> dict:
        """GET an arbitrary (possibly deep / absolute) resource path as JSON."""
        return self.get_json(path, params=params)

    def follow_self_link(self, entity: dict, params=None) -> dict:
        """Re-fetch an entity by following its absolute @iot.selfLink."""
        return self.get_json(self_link(entity), params=params)

    # -- create helper ----------------------------------------------------
    def create(self, collection: str, payload: dict) -> httpx.Response:
        """POST a payload to a collection (or navigation link). Returns the
        raw response so callers can assert 201 + Location header themselves."""
        return self.post(collection, json=payload)

    def location_of(self, resp: httpx.Response) -> str:
        """Return the absolute URL from a 201 response's Location header."""
        return resp.headers["location"]
