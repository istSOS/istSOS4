"""
End-to-end tests for the connector's auth matrix, exercised through the
mounted router (v1) rather than calling auth_gate directly (see
test_auth_gate.py for the unit-level version).

STAC_TRANSFORMER/DCAT_TRANSFORMER are forced on in conftest.py's env
bootstrap, not here -- api.py's route decorators bake that flag in at
import time, so it can't be toggled per-test. SECRET_KEY is set the same
way, so app.oauth's SECRET_KEY (imported by name, same as everything
else here) is already correct before any test module loads.
"""

from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

from app.oauth import create_access_token
from app.v1.api import v1
from app.v1.connector.harvester import HarvestedNetwork, HarvestedNetworkCatalog, HarvestedThing
import app.v1.connector.stac_transformer as stac_transformer
import app.v1.connector.dcat_transformer as dcat_transformer
from app.v1.connector.cache import get_dcat_metadata


@pytest.fixture
def mock_cache(monkeypatch):
    """Mock the cache layer so route tests run without a Redis dependency."""
    monkeypatch.setattr("app.v1.connector.api.get_catalog", AsyncMock(return_value={"id": "test-cat", "type": "Catalog"}))
    monkeypatch.setattr("app.v1.connector.api.get_collection", AsyncMock(return_value={"id": "thing-1", "type": "Collection"}))
    monkeypatch.setattr("app.v1.connector.api.get_network_catalog", AsyncMock(return_value={"id": "network-1", "type": "Catalog"}))
    monkeypatch.setattr("app.v1.connector.api.get_dcat_root", AsyncMock(return_value="@prefix dcat: <http://www.w3.org/ns/dcat#> ."))
    monkeypatch.setattr("app.v1.connector.api.get_dcat_root_jsonld", AsyncMock(return_value='{"@type": "dcat:Catalog"}'))
    monkeypatch.setattr("app.v1.connector.api.get_dcat_orphan", AsyncMock(return_value="@prefix dcat: <http://www.w3.org/ns/dcat#> ."))
    monkeypatch.setattr("app.v1.connector.api.get_dcat_orphan_jsonld", AsyncMock(return_value='{"@type": "dcat:Catalog"}'))
    monkeypatch.setattr("app.v1.connector.api.get_dcat_network", AsyncMock(return_value="@prefix dcat: <http://www.w3.org/ns/dcat#> ."))
    monkeypatch.setattr("app.v1.connector.api.get_dcat_network_jsonld", AsyncMock(return_value='{"@type": "dcat:Catalog"}'))


@pytest.fixture
def valid_auth_headers(monkeypatch):
    """Generate valid Bearer token headers and mock get_user_from_db."""
    token, _ = create_access_token({"sub": "admin", "role": "admin"})
    monkeypatch.setattr("app.oauth.get_user_from_db", AsyncMock(return_value={"id": 1, "username": "admin", "role": "admin", "uri": ""}))
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_authorization_disabled(mock_cache, set_connector_flags):
    """AUTHORIZATION=0: shallow and deep routes both public, no token needed."""
    set_connector_flags(authorization=0, anonymous_viewer=0)

    async with AsyncClient(transport=ASGITransport(app=v1), base_url="http://test") as ac:
        res_shallow = await ac.get("/connector/stac")
        assert res_shallow.status_code == 200

        res_deep = await ac.get("/connector/dcat/orphan")
        assert res_deep.status_code == 200


@pytest.mark.asyncio
async def test_authorization_anonymous_viewer(mock_cache, set_connector_flags):
    """AUTHORIZATION=1, ANONYMOUS_VIEWER=1: shallow and deep routes both public."""
    set_connector_flags(authorization=1, anonymous_viewer=1)

    async with AsyncClient(transport=ASGITransport(app=v1), base_url="http://test") as ac:
        res_shallow = await ac.get("/connector/stac")
        assert res_shallow.status_code == 200

        res_deep = await ac.get("/connector/dcat/orphan")
        assert res_deep.status_code == 200


@pytest.mark.asyncio
async def test_strict_mode_open_metadata(mock_cache, set_connector_flags):
    """AUTHORIZATION=1, ANONYMOUS_VIEWER=0, OPEN_CATALOG_METADATA=1:
    every connector route is public, STAC and DCAT alike (no route is
    gated purely by being a network/orphan sub-catalog -- see
    test_dcat_gate_parity.py for the regression this guards)."""
    set_connector_flags(
        authorization=1, anonymous_viewer=0, open_catalog_metadata=True, closed_networks=[]
    )

    async with AsyncClient(transport=ASGITransport(app=v1), base_url="http://test") as ac:
        res_stac = await ac.get("/connector/stac")
        assert res_stac.status_code == 200

        res_dcat_orphan = await ac.get("/connector/dcat/orphan")
        assert res_dcat_orphan.status_code == 200

        res_dcat_network = await ac.get("/connector/dcat/1")
        assert res_dcat_network.status_code == 200


@pytest.mark.asyncio
async def test_strict_mode_closed_metadata(mock_cache, valid_auth_headers, set_connector_flags):
    """AUTHORIZATION=1, ANONYMOUS_VIEWER=0, OPEN_CATALOG_METADATA=0:
    every connector route requires a token (401 without, 200 with)."""
    set_connector_flags(
        authorization=1, anonymous_viewer=0, open_catalog_metadata=False, closed_networks=[]
    )

    async with AsyncClient(transport=ASGITransport(app=v1), base_url="http://test") as ac:
        res_stac_anon = await ac.get("/connector/stac")
        assert res_stac_anon.status_code == 401
        res_stac_auth = await ac.get("/connector/stac", headers=valid_auth_headers)
        assert res_stac_auth.status_code == 200

        res_dcat_anon = await ac.get("/connector/dcat/orphan")
        assert res_dcat_anon.status_code == 401
        res_dcat_auth = await ac.get("/connector/dcat/orphan", headers=valid_auth_headers)
        assert res_dcat_auth.status_code == 200


@pytest.mark.asyncio
async def test_closed_network_override(mock_cache, valid_auth_headers, set_connector_flags):
    """AUTHORIZATION=1, ANONYMOUS_VIEWER=0, CATALOG_CLOSED_NETWORKS=3,7:
    closed network routes return 404 without token (not 401), 200 with token.
    Unclosed network 1 obeys OPEN_CATALOG_METADATA, on both STAC and DCAT."""
    set_connector_flags(
        authorization=1, anonymous_viewer=0, open_catalog_metadata=True, closed_networks=[3, 7]
    )

    async with AsyncClient(transport=ASGITransport(app=v1), base_url="http://test") as ac:
        # Closed network, STAC, unauthenticated: 404.
        res_closed_stac_anon = await ac.get("/connector/stac/7")
        assert res_closed_stac_anon.status_code == 404

        # Closed network, DCAT, unauthenticated: 404.
        res_closed_dcat_anon = await ac.get("/connector/dcat/7")
        assert res_closed_dcat_anon.status_code == 404

        # Closed network, authenticated: 200 on both.
        res_closed_stac_auth = await ac.get("/connector/stac/7", headers=valid_auth_headers)
        assert res_closed_stac_auth.status_code == 200

        res_closed_dcat_auth = await ac.get("/connector/dcat/7", headers=valid_auth_headers)
        assert res_closed_dcat_auth.status_code == 200

        # Unclosed network 1, unauthenticated: 200 on both (OPEN_CATALOG_METADATA=True).
        res_unclosed_stac_anon = await ac.get("/connector/stac/1")
        assert res_unclosed_stac_anon.status_code == 200

        res_unclosed_dcat_anon = await ac.get("/connector/dcat/1")
        assert res_unclosed_dcat_anon.status_code == 200


@pytest.mark.asyncio
async def test_root_catalog_link_leak_check(set_connector_flags):
    """Root catalog link leak check (STAC & DCAT): with closed network 7,
    STAC root links and the DCAT root graph must not contain network 7."""
    set_connector_flags(closed_networks=[7])

    # Sample harvested network catalog containing network 1 and closed network 7.
    net_cat = HarvestedNetworkCatalog(
        harvested_at="2026-08-11T12:00:00Z",
        orphan_things=[],
        networks=[
            HarvestedNetwork(id=1, name="Network 1"),
            HarvestedNetwork(id=7, name="Network 7 (Closed)"),
        ],
        things_by_network={
            1: [HarvestedThing(id=10, name="Thing 10", description="", locations=[], datastreams=[], properties={})],
            7: [HarvestedThing(id=70, name="Thing 70", description="", locations=[], datastreams=[], properties={})],
        },
    )

    # STAC check.
    stac_res = stac_transformer.build_stac_catalog_with_networks(net_cat)
    stac_root_cat = stac_res["catalog"]
    assert 7 not in stac_root_cat["network_ids"]
    hrefs = [link["href"] for link in stac_root_cat["links"]]
    assert not any("/7" in href for href in hrefs)

    # DCAT check.
    dcat_res = dcat_transformer.build_dcat_catalog_with_networks(net_cat)
    dcat_root_graph = dcat_res["root"]
    turtle_output = dcat_root_graph.serialize(format="turtle")
    assert "/dcat/7" not in turtle_output


@pytest.mark.asyncio
async def test_connector_summary_endpoint_closed_network(mock_cache, monkeypatch, set_connector_flags):
    """GET /connector: closed network id is absent from dcat_network_ids."""
    set_connector_flags(closed_networks=[7])

    # Mock Redis returning raw network_ids [1, 7, 2].
    monkeypatch.setattr("app.v1.connector.cache.redis.get", lambda key: b'[1, 7, 2]' if key == "dcat:meta:network_ids" else None)

    dcat_meta = get_dcat_metadata()
    assert 7 not in dcat_meta["network_ids"]
    assert dcat_meta["network_ids"] == [1, 2]

    # Same check through the route handler.
    async with AsyncClient(transport=ASGITransport(app=v1), base_url="http://test") as ac:
        res = await ac.get("/connector")
        assert res.status_code == 200
        data = res.json()
        assert 7 not in data["dcat_network_ids"]


@pytest.mark.asyncio
async def test_garbage_token_returns_401(mock_cache, set_connector_flags):
    """Garbage/expired token on a gated route returns 401 (not treated as no-token)."""
    set_connector_flags(authorization=1, anonymous_viewer=0, open_catalog_metadata=True)

    async with AsyncClient(transport=ASGITransport(app=v1), base_url="http://test") as ac:
        invalid_headers = {"Authorization": "Bearer invalid.garbage.jwt.token"}
        res = await ac.get("/connector/dcat/orphan", headers=invalid_headers)
        assert res.status_code == 401
        assert res.json()["detail"] == "Could not validate credentials"
