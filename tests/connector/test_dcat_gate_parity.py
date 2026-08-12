"""
Regression test for the bug where /dcat/orphan and /dcat/{network_id}
were wired to a separate "deep_gate" that skipped OPEN_CATALOG_METADATA
entirely and always returned 401 in strict mode, unlike every STAC route
(including /stac/{network_id}) and unlike /dcat/root -- all of which
respect OPEN_CATALOG_METADATA as the docs describe. With
CATALOG_CLOSED_NETWORKS empty, that made every DCAT network/orphan
request permanently gated no matter what OPEN_CATALOG_METADATA said.

Two layers:
- test_all_gated_routes_share_one_gate: structural check on the actual
  FastAPI route table, so a future PR that reintroduces a second gate
  function fails here even before any HTTP request is made.
- test_stac_and_dcat_network_routes_match_under_every_flag_combo: black-box
  parity check that the equivalent STAC/DCAT route pair behaves the same
  under every AUTHORIZATION/ANONYMOUS_VIEWER/OPEN_CATALOG_METADATA
  combination.
"""

import pytest
from httpx import ASGITransport, AsyncClient

from app.v1.api import v1 as root_router
from app.v1.connector.api import v1 as connector_router
from app.v1.connector.auth_gate import gate


def _connector_routes():
    """All routes mounted under /connector that carry a gate dependency."""
    return [r for r in connector_router.routes if r.dependant.dependencies]


def test_all_gated_routes_share_one_gate():
    """Every gated connector route -- STAC or DCAT, root or network-scoped
    -- must resolve to the same auth_gate.gate function. A second gate
    function (however named) reintroduces exactly this bug."""
    routes = _connector_routes()
    assert routes, "expected at least one gated connector route"

    for route in routes:
        callables = [dep.call for dep in route.dependant.dependencies]
        assert gate in callables, (
            f"{route.path} does not depend on auth_gate.gate "
            f"(found: {[c.__name__ for c in callables]})"
        )


@pytest.fixture
def mock_all_cache_reads(monkeypatch):
    """Stub every cache read the STAC/DCAT routes touch, so this test only
    exercises gating, never real Redis or harvested content."""
    from unittest.mock import AsyncMock

    for name, value in [
        ("get_catalog", {"id": "test-cat", "type": "Catalog"}),
        ("get_collection", {"id": "thing-1", "type": "Collection"}),
        ("get_network_catalog", {"id": "network-1", "type": "Catalog"}),
        ("get_dcat_root", "@prefix dcat: <http://www.w3.org/ns/dcat#> ."),
        ("get_dcat_root_jsonld", '{"@type": "dcat:Catalog"}'),
        ("get_dcat_orphan", "@prefix dcat: <http://www.w3.org/ns/dcat#> ."),
        ("get_dcat_orphan_jsonld", '{"@type": "dcat:Catalog"}'),
        ("get_dcat_network", "@prefix dcat: <http://www.w3.org/ns/dcat#> ."),
        ("get_dcat_network_jsonld", '{"@type": "dcat:Catalog"}'),
    ]:
        monkeypatch.setattr(f"app.v1.connector.api.{name}", AsyncMock(return_value=value))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "authorization,anonymous_viewer,open_catalog_metadata",
    [
        (0, 0, True),   # authorization fully disabled
        (1, 1, True),   # anonymous viewer: strict mode bypassed
        (1, 0, True),   # strict mode, open metadata -- the bug's exact repro case
        (1, 0, False),  # strict mode, closed metadata
    ],
)
async def test_stac_and_dcat_network_routes_match_under_every_flag_combo(
    mock_all_cache_reads, set_connector_flags, authorization, anonymous_viewer, open_catalog_metadata
):
    """For every flag combination, an unclosed network's STAC route and its
    DCAT route must return the same status -- there is no code-level reason
    for DCAT network/orphan content to be gated any differently than the
    equivalent STAC content."""
    set_connector_flags(
        authorization=authorization,
        anonymous_viewer=anonymous_viewer,
        open_catalog_metadata=open_catalog_metadata,
        closed_networks=[],
    )

    async with AsyncClient(transport=ASGITransport(app=root_router), base_url="http://test") as ac:
        res_stac_network = await ac.get("/connector/stac/1")
        res_dcat_network = await ac.get("/connector/dcat/1")
        res_dcat_orphan = await ac.get("/connector/dcat/orphan")

        assert res_stac_network.status_code == res_dcat_network.status_code, (
            f"/connector/stac/1 -> {res_stac_network.status_code} but "
            f"/connector/dcat/1 -> {res_dcat_network.status_code} under "
            f"AUTHORIZATION={authorization}, ANONYMOUS_VIEWER={anonymous_viewer}, "
            f"OPEN_CATALOG_METADATA={open_catalog_metadata}"
        )
        assert res_stac_network.status_code == res_dcat_orphan.status_code
