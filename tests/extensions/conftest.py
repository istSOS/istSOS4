"""
conftest.py -- shared fixtures for the istSOS4 EXTENSION suites.

DELIBERATELY SEPARATE from tests/conformance/conftest.py: this tree is a sibling
of tests/conformance/, so the conformance conftest (and its `seed`, which loads
entitiesDefault.json WITHOUT a network_id and would fail under NETWORK=1) is NOT
an ancestor and is never inherited. Extension seeds live in the per-extension
subfolders (e.g. network/conftest.py).

Provides only the generic plumbing: base_url, a session STAClient, and a
function-scoped unique_name factory. client.py is the copied STA client.
"""

from __future__ import annotations

import os
import sys
import uuid

import pytest

# Anchor this dir on sys.path so subfolder tests can `import client` under
# --import-mode=importlib (no package __init__.py needed).
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client import STAClient, DEFAULT_BASE_URL  # noqa: E402


@pytest.fixture(scope="session")
def base_url() -> str:
    return os.environ.get("STA_BASE_URL", DEFAULT_BASE_URL).rstrip("/")


@pytest.fixture(scope="session")
def client(base_url) -> STAClient:
    c = STAClient(base_url=base_url)
    yield c
    c.close()


@pytest.fixture
def unique_name():
    """Factory: unique_name('prefix') -> 'prefix-<uuid12>' for collision-free entities."""
    def _make(prefix: str = "ext") -> str:
        return f"{prefix}-{uuid.uuid4().hex[:12]}"
    return _make
