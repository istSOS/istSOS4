"""
conftest.py -- fixtures for tests/connector/validate

This suite checks the connector's *generated output* (DCAT-AP 3.0.0 and
STAC 1.0) against the official upstream schemas -- it's a conformance
check against a running instance, not a unit test with mocks like the
rest of tests/connector.

Everything it needs is fetched into ./turtle automatically the first time
you run it:
  - the official DCAT-AP 3.0.0 SHACL shapes (SEMICeu), cached for a week
    since they basically never change
  - the connector's own DCAT/STAC output, re-fetched every session since
    that's the thing under test

So `pytest -v tests/connector/validate` works standalone -- the only
requirement is a reachable connector instance (CONNECTOR_BASE_URL).

Route paths and the default base URL below are confirmed against the
actual repo (app/__init__.py, app/main.py, app/v1/api.py, and
app/v1/connector/api.py on gsoc/final-cleanup-and-auth), not guessed:

  - app/main.py does `app.mount(f"{SUBPATH}{VERSION}", api.v1)`, and
    app/v1/api.py does `v1.include_router(connector.v1, prefix="/connector")`.
    With the istSOS defaults (HOSTNAME=http://localhost:8018,
    SUBPATH=/istsos4, VERSION=/v1.1) that puts the connector root at
    http://localhost:8018/istsos4/v1.1/connector -- NOT bare
    http://localhost:8018. CONNECTOR_BASE_URL below is that full path,
    already including /connector; override it whole if your deployment
    changes HOSTNAME/SUBPATH/VERSION.
  - api/app/v1/connector/api.py's real DCAT routes are lowercase
    /dcat/root(.ttl), /dcat/orphan(.ttl), /dcat/{network_id}(.ttl) --
    not /DCAT/*. network_id is a path param typed `int`, so
    CONNECTOR_NETWORKS must be a list of integers, not names like
    "network_1" (existing unit tests under tests/connector use ids like
    1, 2, 7 -- 1,2 is a sane default for a dev DB).
  - /dcat/orphan and /dcat/{network_id} legitimately 404 when NETWORK=0
    (see api.py's docstrings: orphan/network scopes only exist under
    NETWORK=1) -- that is a real "this scope doesn't exist" answer, not
    "the connector is broken", so it's treated as optional/absent below
    rather than failing the whole fixture. /dcat/root(.ttl) 404 only
    happens when DCAT_TRANSFORMER itself is off, which does mean there
    is nothing to validate, so that one is fatal (skip).
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest
import requests

VALIDATE_DIR = Path(__file__).parent
TURTLE_DIR = VALIDATE_DIR / "turtle"
TURTLE_DIR.mkdir(exist_ok=True)

# --- Official DCAT-AP 3.0.0 SHACL shapes (SEMICeu) -------------------------
# Verified live at https://github.com/SEMICeu/DCAT-AP/blob/master/releases/3.0.0/shacl/dcat-ap-SHACL.ttl
DCAT_AP_SHACL_URL = (
    "https://raw.githubusercontent.com/SEMICeu/DCAT-AP/master/"
    "releases/3.0.0/shacl/dcat-ap-SHACL.ttl"
)
SHACL_SHAPES_FILE = TURTLE_DIR / "dcat-ap-SHACL.ttl"
SHACL_MAX_AGE_S = 7 * 24 * 3600  # re-download weekly

# --- Connector under test ---------------------------------------------------
# Full connector-router root, i.e. {HOSTNAME}{SUBPATH}{VERSION}/connector
# with istSOS's own defaults (app/__init__.py). Override the whole value if
# your deployment sets HOSTNAME/SUBPATH/VERSION differently.
CONNECTOR_BASE_URL = os.environ.get(
    "CONNECTOR_BASE_URL", "http://localhost:8018/istsos4/v1.1/connector"
).rstrip("/")
CONNECTOR_TIMEOUT_S = float(os.environ.get("CONNECTOR_TIMEOUT_S", "30"))

# TODO query the db for the actual network ids instead of hardcoding them here.
NETWORKS = [
    n.strip() for n in os.environ.get("CONNECTOR_NETWORKS", "1,2").split(",") if n.strip()
]


def _download(url: str, dest: Path) -> None:
    resp = requests.get(url, timeout=CONNECTOR_TIMEOUT_S)
    resp.raise_for_status()
    dest.write_bytes(resp.content)


def _stale(path: Path, max_age_s: float) -> bool:
    return not path.exists() or (time.time() - path.stat().st_mtime) > max_age_s


def _get(url: str) -> requests.Response:
    """
    GET url, skipping the whole session on a connection-level failure
    (connector not running at all). HTTP-level responses -- including
    404/503 -- are returned as-is for the caller to interpret, since those
    mean different things here (see module docstring).
    """
    try:
        return requests.get(url, timeout=CONNECTOR_TIMEOUT_S)
    except requests.exceptions.ConnectionError as exc:
        pytest.skip(f"Connector not reachable at {url}: {exc}")


@pytest.fixture(scope="session")
def connector_base_url() -> str:
    return CONNECTOR_BASE_URL


@pytest.fixture(scope="session")
def shacl_shapes_file() -> Path:
    """Official DCAT-AP 3.0.0 SHACL shapes. Downloaded once, re-used for a week."""
    if _stale(SHACL_SHAPES_FILE, SHACL_MAX_AGE_S):
        try:
            _download(DCAT_AP_SHACL_URL, SHACL_SHAPES_FILE)
        except requests.RequestException as exc:
            if not SHACL_SHAPES_FILE.exists():
                pytest.skip(f"Could not download DCAT-AP SHACL shapes: {exc}")
            # else: network hiccup but we have a cached copy -- fall through and use it.
    return SHACL_SHAPES_FILE

# TODO retrieve dcat files from redis instead of doing all this hassle.. 
# but STAC has to be done by fetching the catalog and parsing it.
@pytest.fixture(scope="session")
def dcat_turtle_files(connector_base_url) -> list[Path]:
    """
    The connector's own DCAT-AP output, re-fetched every session (it's the
    thing under test). root.ttl is mandatory -- a 404 there means
    DCAT_TRANSFORMER=0 and there is nothing to validate, so that skips the
    whole suite. orphan.ttl and each network's .ttl are optional: a 404
    there just means that scope doesn't exist in this deployment
    (NETWORK=0, or that network id isn't populated) and is skipped
    per-file, not treated as a failure.
    """
    files: list[Path] = []

    root_resp = _get(f"{connector_base_url}/dcat/root.ttl")
    if root_resp.status_code == 404:
        pytest.skip(
            "DCAT_TRANSFORMER appears disabled on the connector "
            f"(404 at {connector_base_url}/dcat/root.ttl)"
        )
    if root_resp.status_code == 503:
        pytest.skip(
            "DCAT catalog has not been generated yet (503) -- "
            "run a harvest cycle first"
        )
    root_resp.raise_for_status()
    root_dest = TURTLE_DIR / "root.ttl"
    root_dest.write_bytes(root_resp.content)
    files.append(root_dest)

    optional_endpoints = {}
    for network in NETWORKS:
        optional_endpoints[f"network_{network}.ttl"] = f"{connector_base_url}/dcat/{network}.ttl"

    for filename, url in optional_endpoints.items():
        resp = _get(url)
        if resp.status_code == 404:
            # Scope genuinely doesn't exist here (NETWORK=0, or this
            # network id isn't in the DB) -- not a connector problem.
            continue
        if resp.status_code == 503:
            pytest.skip(f"DCAT scope at {url} has not been generated yet (503)")
        resp.raise_for_status()
        dest = TURTLE_DIR / filename
        dest.write_bytes(resp.content)
        files.append(dest)

    return files