"""
Regression test for the DCAT root scope's closed-network reveal.

build_dcat_catalog_with_networks() builds two root graphs every harvest
cycle: "root" (closed Networks omitted from hasPart/dcat:catalog) and
"root_all" (closed Networks included). This mirrors stac_transformer.py's
closed_network_ids field on the STAC root Catalog, and exists so an
authenticated caller can discover a closed Network from /dcat/root the
same way they already can from /stac -- see auth_gate.py's docstring:
"network_id in CATALOG_CLOSED_NETWORKS -> 404 (hidden)" only applies to
*unauthenticated* callers.

"root_all" was previously built by the transformer but never written to
Redis or served -- api.py's dcat_root/dcat_root_ttl always returned the
public "root" scope regardless of who was asking, so an authenticated
caller had no way to discover a closed Network from /dcat/root at all
(even though they could still reach it directly by id). This test pins
the fixed behavior in place.
"""

from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

from app.oauth import create_access_token
from app.v1.api import v1


_PUBLIC_TTL = "@prefix dcat: <http://www.w3.org/ns/dcat#> . # public, closed nets hidden"
_PUBLIC_JSONLD = '{"@type": "dcat:Catalog", "scope": "public"}'
_ALL_TTL = "@prefix dcat: <http://www.w3.org/ns/dcat#> . # all, closed nets included"
_ALL_JSONLD = '{"@type": "dcat:Catalog", "scope": "all"}'


@pytest.fixture
def mock_dcat_root_scopes(monkeypatch):
    """Distinguishable public vs. all-networks content for /dcat/root[.ttl]."""
    monkeypatch.setattr("app.v1.connector.api.get_dcat_root", AsyncMock(return_value=_PUBLIC_TTL))
    monkeypatch.setattr("app.v1.connector.api.get_dcat_root_jsonld", AsyncMock(return_value=_PUBLIC_JSONLD))
    monkeypatch.setattr("app.v1.connector.api.get_dcat_root_all", AsyncMock(return_value=_ALL_TTL))
    monkeypatch.setattr("app.v1.connector.api.get_dcat_root_all_jsonld", AsyncMock(return_value=_ALL_JSONLD))


@pytest.fixture
def valid_auth_headers(monkeypatch):
    """Same pattern as test_auth.py: a real, verifiable Bearer token."""
    token, _ = create_access_token({"sub": "admin", "role": "admin"})
    monkeypatch.setattr(
        "app.oauth.get_user_from_db",
        AsyncMock(return_value={"id": 1, "username": "admin", "role": "admin", "uri": ""}),
    )
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_anonymous_gets_public_scope(mock_dcat_root_scopes, set_connector_flags):
    """No token: /dcat/root and /dcat/root.ttl serve the closed-networks-hidden scope."""
    set_connector_flags(authorization=0, anonymous_viewer=0)

    async with AsyncClient(transport=ASGITransport(app=v1), base_url="http://test") as ac:
        res_jsonld = await ac.get("/connector/dcat/root")
        assert res_jsonld.status_code == 200
        assert res_jsonld.json() == {"@type": "dcat:Catalog", "scope": "public"}

        res_ttl = await ac.get("/connector/dcat/root.ttl")
        assert res_ttl.status_code == 200
        assert "closed nets hidden" in res_ttl.text


@pytest.mark.asyncio
async def test_authenticated_gets_all_networks_scope(mock_dcat_root_scopes, valid_auth_headers, set_connector_flags):
    """Valid token: /dcat/root and /dcat/root.ttl serve the closed-networks-included
    scope -- the same reveal rule stac_root already applies via closed_network_ids."""
    set_connector_flags(authorization=0, anonymous_viewer=0)

    async with AsyncClient(transport=ASGITransport(app=v1), base_url="http://test") as ac:
        res_jsonld = await ac.get("/connector/dcat/root", headers=valid_auth_headers)
        assert res_jsonld.status_code == 200
        assert res_jsonld.json() == {"@type": "dcat:Catalog", "scope": "all"}

        res_ttl = await ac.get("/connector/dcat/root.ttl", headers=valid_auth_headers)
        assert res_ttl.status_code == 200
        assert "closed nets included" in res_ttl.text


@pytest.mark.asyncio
async def test_authenticated_falls_back_when_no_all_scope_cached(monkeypatch, valid_auth_headers, set_connector_flags):
    """NETWORK=0 never writes a 'root:all' scope (nothing to hide in a flat
    catalog with no Networks). An authenticated caller must still get the
    plain root back, not a 503 -- the fallback in dcat_root/dcat_root_ttl
    exists for exactly this case."""
    set_connector_flags(authorization=0, anonymous_viewer=0)
    monkeypatch.setattr("app.v1.connector.api.get_dcat_root", AsyncMock(return_value=_PUBLIC_TTL))
    monkeypatch.setattr("app.v1.connector.api.get_dcat_root_jsonld", AsyncMock(return_value=_PUBLIC_JSONLD))
    monkeypatch.setattr("app.v1.connector.api.get_dcat_root_all", AsyncMock(return_value=None))
    monkeypatch.setattr("app.v1.connector.api.get_dcat_root_all_jsonld", AsyncMock(return_value=None))

    async with AsyncClient(transport=ASGITransport(app=v1), base_url="http://test") as ac:
        res_jsonld = await ac.get("/connector/dcat/root", headers=valid_auth_headers)
        assert res_jsonld.status_code == 200
        assert res_jsonld.json() == {"@type": "dcat:Catalog", "scope": "public"}

        res_ttl = await ac.get("/connector/dcat/root.ttl", headers=valid_auth_headers)
        assert res_ttl.status_code == 200
        assert "closed nets hidden" in res_ttl.text
