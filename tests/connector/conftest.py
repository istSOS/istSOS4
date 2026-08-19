"""
conftest.py -- bootstrap and shared fixtures for tests/connector.

Bootstrap
---------
`app` lives at api/app, not on sys.path when pytest is invoked from the
repo root (e.g. `pytest tests/connector`), which is what api/tests/ relies
on the caller doing (`cd api && pytest`) instead. This suite lives outside
api/, so it inserts api/ onto sys.path itself, before anything imports app.

STAC_TRANSFORMER and DCAT_TRANSFORMER are read once at import time by
api.py's route decorators (@_require_enabled bakes the flag's value into
the decorator at decoration time, not read again per-request), so they
can't be toggled later by patching a name -- they're forced on here via
os.environ, before api.py (or anything importing it) is first loaded.

Fixtures
--------
Every other connector flag (AUTHORIZATION, ANONYMOUS_VIEWER,
OPEN_CATALOG_METADATA, CATALOG_CLOSED_NETWORKS) is imported by name
(`from app... import X`) into each module that uses it, so each importer
owns an independent binding -- patching the `app` package or the config
module does NOT propagate to them. set_connector_flags patches the actual
consuming modules directly.
"""

from __future__ import annotations

import os
import sys

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_API_DIR = os.path.join(os.path.dirname(os.path.dirname(_THIS_DIR)), "api")
if _API_DIR not in sys.path:
    sys.path.insert(0, _API_DIR)

os.environ.setdefault("STAC_TRANSFORMER", "1")
os.environ.setdefault("DCAT_TRANSFORMER", "1")
os.environ.setdefault("SECRET_KEY", "test-secret-key-1234567890-test")

import pytest

# Every module that imports CATALOG_CLOSED_NETWORKS directly from config.py.
_CLOSED_NETWORKS_MODULES = (
    "app.v1.connector.auth_gate",
    "app.v1.connector.cache",
    "app.v1.connector.stac_transformer",
    "app.v1.connector.dcat_transformer",
)


@pytest.fixture
def set_connector_flags(monkeypatch):
    """
    Factory fixture: set_connector_flags(authorization=1, ...) patches the
    connector's auth/visibility flags for the current test only (monkeypatch
    auto-reverts on teardown, so tests never leak state into each other).
    """

    def _set(
        *,
        authorization: int | None = None,
        anonymous_viewer: int | None = None,
        open_catalog_metadata: bool | None = None,
        closed_networks: list[int] | None = None,
    ) -> None:
        if authorization is not None:
            monkeypatch.setattr("app.v1.connector.auth_gate.AUTHORIZATION", authorization)
        if anonymous_viewer is not None:
            monkeypatch.setattr("app.v1.connector.auth_gate.ANONYMOUS_VIEWER", anonymous_viewer)
        if open_catalog_metadata is not None:
            monkeypatch.setattr("app.v1.connector.auth_gate.OPEN_CATALOG_METADATA", open_catalog_metadata)
        if closed_networks is not None:
            frozen = frozenset(closed_networks)
            for mod in _CLOSED_NETWORKS_MODULES:
                monkeypatch.setattr(f"{mod}.CATALOG_CLOSED_NETWORKS", frozen)

    return _set
