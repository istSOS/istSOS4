import pytest
import app
import app.v1.connector.config as connector_config
from app.v1.connector.auth_gate import make_gate, _login_required, _hidden


@pytest.mark.asyncio
async def test_auth_disabled():
    shallow_gate = make_gate(deep_tier=False)
    deep_gate = make_gate(deep_tier=True)

    app.AUTHORIZATION = 0
    app.ANONYMOUS_VIEWER = 0

    assert await shallow_gate(network_id=None, current_user=None) is None
    assert await deep_gate(network_id=5, current_user=None) is None


@pytest.mark.asyncio
async def test_anonymous_viewer_enabled():
    shallow_gate = make_gate(deep_tier=False)
    deep_gate = make_gate(deep_tier=True)

    app.AUTHORIZATION = 1
    app.ANONYMOUS_VIEWER = 1

    assert await shallow_gate(network_id=None, current_user=None) is None
    assert await deep_gate(network_id=5, current_user=None) is None


@pytest.mark.asyncio
async def test_strict_mode_authenticated():
    shallow_gate = make_gate(deep_tier=False)
    deep_gate = make_gate(deep_tier=True)

    app.AUTHORIZATION = 1
    app.ANONYMOUS_VIEWER = 0
    connector_config.OPEN_CATALOG_METADATA = True
    connector_config.CATALOG_CLOSED_NETWORKS = frozenset([7])

    user = {"sub": "testuser", "role": "viewer"}

    # Authenticated requests pass regardless of tier or closed network
    assert await shallow_gate(network_id=None, current_user=user) is None
    assert await shallow_gate(network_id=7, current_user=user) is None
    assert await deep_gate(network_id=1, current_user=user) is None
    assert await deep_gate(network_id=7, current_user=user) is None


@pytest.mark.asyncio
async def test_strict_mode_unauthenticated_shallow_open():
    shallow_gate = make_gate(deep_tier=False)

    app.AUTHORIZATION = 1
    app.ANONYMOUS_VIEWER = 0
    connector_config.OPEN_CATALOG_METADATA = True
    connector_config.CATALOG_CLOSED_NETWORKS = frozenset([7])

    # Shallow tier with open metadata passes
    assert await shallow_gate(network_id=1, current_user=None) is None


@pytest.mark.asyncio
async def test_strict_mode_unauthenticated_shallow_closed_metadata():
    shallow_gate = make_gate(deep_tier=False)

    app.AUTHORIZATION = 1
    app.ANONYMOUS_VIEWER = 0
    connector_config.OPEN_CATALOG_METADATA = False
    connector_config.CATALOG_CLOSED_NETWORKS = frozenset([7])

    # Shallow tier with closed metadata requires login (401)
    res = await shallow_gate(network_id=1, current_user=None)
    assert res is not None
    assert res.status_code == 401


@pytest.mark.asyncio
async def test_strict_mode_unauthenticated_deep():
    deep_gate = make_gate(deep_tier=True)

    app.AUTHORIZATION = 1
    app.ANONYMOUS_VIEWER = 0
    connector_config.OPEN_CATALOG_METADATA = True
    connector_config.CATALOG_CLOSED_NETWORKS = frozenset([7])

    # Deep tier requires login (401)
    res = await deep_gate(network_id=1, current_user=None)
    assert res is not None
    assert res.status_code == 401


@pytest.mark.asyncio
async def test_strict_mode_unauthenticated_closed_network():
    shallow_gate = make_gate(deep_tier=False)
    deep_gate = make_gate(deep_tier=True)

    app.AUTHORIZATION = 1
    app.ANONYMOUS_VIEWER = 0
    connector_config.OPEN_CATALOG_METADATA = True
    connector_config.CATALOG_CLOSED_NETWORKS = frozenset([7])

    # Closed network override returns 404 (hidden) for both shallow and deep tiers
    res_shallow = await shallow_gate(network_id=7, current_user=None)
    assert res_shallow is not None
    assert res_shallow.status_code == 404

    res_deep = await deep_gate(network_id=7, current_user=None)
    assert res_deep is not None
    assert res_deep.status_code == 404
