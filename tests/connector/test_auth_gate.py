"""
Unit tests for auth_gate.make_gate()'s access-control matrix -- calls the
gate dependency directly with no HTTP layer involved. See test_auth.py for
the same matrix exercised end-to-end through the mounted router.
"""

import pytest

from app.v1.connector.auth_gate import make_gate


@pytest.mark.asyncio
async def test_auth_disabled(set_connector_flags):
    set_connector_flags(authorization=0, anonymous_viewer=0)
    shallow_gate = make_gate(deep_tier=False)
    deep_gate = make_gate(deep_tier=True)

    assert await shallow_gate(network_id=None, current_user=None) is None
    assert await deep_gate(network_id=5, current_user=None) is None


@pytest.mark.asyncio
async def test_anonymous_viewer_enabled(set_connector_flags):
    set_connector_flags(authorization=1, anonymous_viewer=1)
    shallow_gate = make_gate(deep_tier=False)
    deep_gate = make_gate(deep_tier=True)

    assert await shallow_gate(network_id=None, current_user=None) is None
    assert await deep_gate(network_id=5, current_user=None) is None


@pytest.mark.asyncio
async def test_strict_mode_authenticated(set_connector_flags):
    set_connector_flags(
        authorization=1, anonymous_viewer=0, open_catalog_metadata=True, closed_networks=[7]
    )
    shallow_gate = make_gate(deep_tier=False)
    deep_gate = make_gate(deep_tier=True)
    user = {"sub": "testuser", "role": "viewer"}

    # Authenticated requests pass regardless of tier or closed network.
    assert await shallow_gate(network_id=None, current_user=user) is None
    assert await shallow_gate(network_id=7, current_user=user) is None
    assert await deep_gate(network_id=1, current_user=user) is None
    assert await deep_gate(network_id=7, current_user=user) is None


@pytest.mark.asyncio
async def test_strict_mode_unauthenticated_shallow_open(set_connector_flags):
    set_connector_flags(
        authorization=1, anonymous_viewer=0, open_catalog_metadata=True, closed_networks=[7]
    )
    shallow_gate = make_gate(deep_tier=False)

    assert await shallow_gate(network_id=1, current_user=None) is None


@pytest.mark.asyncio
async def test_strict_mode_unauthenticated_shallow_closed_metadata(set_connector_flags):
    set_connector_flags(
        authorization=1, anonymous_viewer=0, open_catalog_metadata=False, closed_networks=[7]
    )
    shallow_gate = make_gate(deep_tier=False)

    res = await shallow_gate(network_id=1, current_user=None)
    assert res is not None
    assert res.status_code == 401


@pytest.mark.asyncio
async def test_strict_mode_unauthenticated_deep(set_connector_flags):
    set_connector_flags(
        authorization=1, anonymous_viewer=0, open_catalog_metadata=True, closed_networks=[7]
    )
    deep_gate = make_gate(deep_tier=True)

    res = await deep_gate(network_id=1, current_user=None)
    assert res is not None
    assert res.status_code == 401


@pytest.mark.asyncio
async def test_strict_mode_unauthenticated_closed_network(set_connector_flags):
    set_connector_flags(
        authorization=1, anonymous_viewer=0, open_catalog_metadata=True, closed_networks=[7]
    )
    shallow_gate = make_gate(deep_tier=False)
    deep_gate = make_gate(deep_tier=True)

    res_shallow = await shallow_gate(network_id=7, current_user=None)
    assert res_shallow is not None
    assert res_shallow.status_code == 404

    res_deep = await deep_gate(network_id=7, current_user=None)
    assert res_deep is not None
    assert res_deep.status_code == 404
