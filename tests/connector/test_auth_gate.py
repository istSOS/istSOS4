"""
Unit tests for auth_gate.gate()'s access-control matrix -- calls the gate
dependency directly with no HTTP layer involved. See test_auth.py for the
same matrix exercised end-to-end through the mounted router, and
test_dcat_gate_parity.py for the STAC/DCAT route-parity regression this
gate's design is meant to guarantee.
"""

import pytest

from app.v1.connector.auth_gate import gate


@pytest.mark.asyncio
async def test_auth_disabled(set_connector_flags):
    set_connector_flags(authorization=0, anonymous_viewer=0)

    assert await gate(network_id=None, current_user=None) is None
    assert await gate(network_id=5, current_user=None) is None


@pytest.mark.asyncio
async def test_anonymous_viewer_enabled(set_connector_flags):
    set_connector_flags(authorization=1, anonymous_viewer=1)

    assert await gate(network_id=None, current_user=None) is None
    assert await gate(network_id=5, current_user=None) is None


@pytest.mark.asyncio
async def test_strict_mode_authenticated(set_connector_flags):
    set_connector_flags(
        authorization=1, anonymous_viewer=0, open_catalog_metadata=True, closed_networks=[7]
    )
    user = {"sub": "testuser", "role": "viewer"}

    # Authenticated requests pass regardless of network.
    assert await gate(network_id=None, current_user=user) is None
    assert await gate(network_id=7, current_user=user) is None
    assert await gate(network_id=1, current_user=user) is None


@pytest.mark.asyncio
async def test_strict_mode_unauthenticated_open_metadata(set_connector_flags):
    set_connector_flags(
        authorization=1, anonymous_viewer=0, open_catalog_metadata=True, closed_networks=[7]
    )

    # Open metadata: unauthenticated passes for any non-closed network.
    assert await gate(network_id=1, current_user=None) is None
    assert await gate(network_id=None, current_user=None) is None


@pytest.mark.asyncio
async def test_strict_mode_unauthenticated_closed_metadata(set_connector_flags):
    set_connector_flags(
        authorization=1, anonymous_viewer=0, open_catalog_metadata=False, closed_networks=[7]
    )

    # OPEN_CATALOG_METADATA=0: unauthenticated requires login (401), not 404.
    res = await gate(network_id=1, current_user=None)
    assert res is not None
    assert res.status_code == 401


@pytest.mark.asyncio
async def test_strict_mode_unauthenticated_closed_network(set_connector_flags):
    set_connector_flags(
        authorization=1, anonymous_viewer=0, open_catalog_metadata=True, closed_networks=[7]
    )

    # Closed network override returns 404 (hidden) regardless of OPEN_CATALOG_METADATA.
    res = await gate(network_id=7, current_user=None)
    assert res is not None
    assert res.status_code == 404
