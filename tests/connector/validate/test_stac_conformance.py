"""
test_stac_conformance.py -- validates the connector's live STAC 1.0 catalog
output using pystac's reference validator.

Requires a reachable connector instance -- see conftest.py. Run alone with:
    pytest -v tests/connector/validate/test_stac_conformance.py
"""

from __future__ import annotations

import pytest

pystac = pytest.importorskip("pystac")
from pystac.errors import STACValidationError  # noqa: E402


@pytest.fixture(scope="module")
def stac_catalog(connector_base_url):
    # connector_base_url is the connector router root (.../connector); the
    # STAC root Catalog itself is served at /stac under that, per api.py's
    # stac_root route.
    catalog_url = f"{connector_base_url}/stac"
    try:
        return pystac.Catalog.from_file(catalog_url)
    except Exception as exc:  # network/href issues, not just validation
        pytest.skip(f"Could not load STAC catalog from {catalog_url}: {exc}")


def test_stac_catalog_has_collections_and_items(stac_catalog):
    collections = list(stac_catalog.get_all_collections())
    items = list(stac_catalog.get_items(recursive=True))
    assert collections, "Expected at least one STAC Collection"
    assert items, "Expected at least one STAC Item"


def test_stac_catalog_validates(stac_catalog):
    try:
        stac_catalog.validate_all()
    except STACValidationError as exc:
        pytest.fail(f"STAC validation failed:\n{exc}")