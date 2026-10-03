"""
OGC SensorThings API v1.1 — Sensing Core (c01): service root & status-code tests.

Covers:
  req/service/root-uri     §9.1  service root document + serverSettings
  req/request-data/status-code  §9.3.1  valid GET → 200
"""
from __future__ import annotations

import pytest

from client import format_id

pytestmark = pytest.mark.c01

COLLECTION_NAMES = [
    "Things",
    "Locations",
    "HistoricalLocations",
    "Datastreams",
    "Sensors",
    "ObservedProperties",
    "Observations",
    "FeaturesOfInterest",
]

# Conformance URIs that MUST appear in serverSettings.conformance (18-088 §8, §9.2).
# istSOS4 declares the granular per-entity/per-aspect URIs rather than the
# top-level class URIs.  The set below matches exactly what the server declares
# for the Sensing Core:  req/datamodel/<entity>/{properties,relations} for all
# 8 entities, plus the control-information, resource-path, and the newly added
# request-data/status-code and request-data/query-status-code requirements.
CORE_CONFORMANCE_URIS = {
    # ---- per-entity data-model (properties) ----
    "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/thing/properties",
    "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/location/properties",
    "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/historical-location/properties",
    "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/datastream/properties",
    "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/sensor/properties",
    "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/observed-property/properties",
    "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/observation/properties",
    "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/feature-of-interest/properties",
    # ---- per-entity data-model (relations) ----
    "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/thing/relations",
    "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/location/relations",
    "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/historical-location/relations",
    "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/datastream/relations",
    "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/sensor/relations",
    "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/observed-property/relations",
    "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/observation/relations",
    "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/feature-of-interest/relations",
    # ---- control information ----
    "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/entity-control-information/common-control-information",
    # ---- resource path ----
    "http://www.opengis.net/spec/iot_sensing/1.1/req/resource-path/resource-path-to-entities",
    # ---- request-data (newly declared) ----
    "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/status-code",
    "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/query-status-code",
}


# ============================================================================
# 1. Service root document
# ============================================================================

class TestServiceRoot:
    """req/service/root-uri — the service root document.

    18-088 §9.1: A SensorThings API service SHALL expose a service root URI.
    The response MUST contain a `value` array with one entry per entity set.
    In v1.1 the response MUST also contain a `serverSettings` object with a
    `conformance` array listing the conformance class URIs.
    """

    def test_service_root_status_200(self, client):
        """req/service/root — GET / returns HTTP 200."""
        resp = client.get("")
        assert resp.status_code == 200, (
            f"Expected 200 for service root, got {resp.status_code}"
        )

    def test_service_root_has_value_array(self, client):
        """req/service/root — response body contains a `value` array."""
        data = client.get_json("")
        assert "value" in data, "Service root must have a 'value' key"
        assert isinstance(data["value"], list), "'value' must be a JSON array"

    def test_service_root_value_has_all_8_collections(self, client):
        """req/service/root — `value[]` contains entries for all 8 entity sets."""
        data = client.get_json("")
        names = {entry["name"] for entry in data["value"]}
        missing = set(COLLECTION_NAMES) - names
        assert not missing, (
            f"Service root 'value' is missing collection entries: {missing}"
        )

    def test_service_root_collection_entries_have_name_and_url(self, client):
        """req/service/root — each entry in `value[]` has 'name' and 'url'."""
        data = client.get_json("")
        for entry in data["value"]:
            assert "name" in entry, f"Entry missing 'name': {entry}"
            assert "url" in entry, f"Entry missing 'url': {entry}"
            assert entry["url"].startswith("http"), (
                f"Collection URL should be absolute: {entry['url']}"
            )

    def test_service_root_collection_urls_are_reachable(self, client):
        """req/service/root — each collection URL in value[] returns 200."""
        data = client.get_json("")
        for entry in data["value"]:
            if entry["name"] not in COLLECTION_NAMES:
                continue
            resp = client.get(entry["url"])
            assert resp.status_code == 200, (
                f"Collection URL {entry['url']} returned {resp.status_code}"
            )

    def test_service_root_has_server_settings(self, client):
        """req/service/server-settings — v1.1 root has 'serverSettings' object."""
        data = client.get_json("")
        assert "serverSettings" in data, (
            "Service root must have 'serverSettings' (required in v1.1)"
        )
        assert isinstance(data["serverSettings"], dict)

    def test_service_root_conformance_array(self, client):
        """req/service/server-settings — serverSettings.conformance is an array."""
        data = client.get_json("")
        conf = data.get("serverSettings", {}).get("conformance")
        assert conf is not None, "serverSettings.conformance must be present"
        assert isinstance(conf, list), "serverSettings.conformance must be a JSON array"

    def test_service_root_conformance_lists_core_uris(self, client):
        """req/datamodel/entity-control-information, req/resource-path —
        serverSettings.conformance MUST list all Sensing Core requirement URIs.
        """
        data = client.get_json("")
        conf = set(data.get("serverSettings", {}).get("conformance", []))
        missing = CORE_CONFORMANCE_URIS - conf
        assert not missing, (
            "serverSettings.conformance is missing required Sensing Core URIs:\n"
            + "\n".join(f"  {u}" for u in sorted(missing))
        )


# ============================================================================
# 19. req/request-data/status-code — valid requests return HTTP 200
# ============================================================================

class TestRequestDataStatusCode:
    """req/request-data/status-code — a successful GET request MUST return
    HTTP 200 (18-088 §9.3.1, now declared in serverSettings.conformance).

    This class provides explicit named coverage for the declared conformance URI.
    The service root (200), all 8 collections (200), and entity-by-id (200) are
    all assertions required by the specification.

    Existing tests in TestServiceRoot, TestCollectionEndpoints, and TestEntityById
    also exercise 200 responses; this class consolidates them under the explicit
    req-id for traceability in the COVERAGE_MATRIX.
    """

    def test_service_root_returns_200(self, client):
        """req/request-data/status-code — GET / (service root) returns HTTP 200."""
        resp = client.get("")
        assert resp.status_code == 200, (
            f"req/request-data/status-code: service root must return 200, "
            f"got {resp.status_code}"
        )

    @pytest.mark.parametrize("collection", COLLECTION_NAMES)
    def test_collection_get_returns_200(self, client, collection):
        """req/request-data/status-code — GET /<collection> returns HTTP 200."""
        resp = client.get(collection)
        assert resp.status_code == 200, (
            f"req/request-data/status-code: GET /{collection} must return 200, "
            f"got {resp.status_code}"
        )

    def test_entity_by_id_returns_200(self, client, seed):
        """req/request-data/status-code — GET /Things(<id>) returns HTTP 200."""
        resp = client.get(f"Things({format_id(seed.thing_id)})")
        assert resp.status_code == 200, (
            f"req/request-data/status-code: entity-by-id must return 200, "
            f"got {resp.status_code}"
        )

    def test_navigation_link_returns_200(self, client, seed):
        """req/request-data/status-code — GET /Things(<id>)/Datastreams returns 200."""
        resp = client.get(f"Things({format_id(seed.thing_id)})/Datastreams")
        assert resp.status_code == 200, (
            f"req/request-data/status-code: navigation endpoint must return 200, "
            f"got {resp.status_code}"
        )

    def test_many_to_one_nav_returns_200(self, client, seed):
        """req/request-data/status-code — GET /Datastreams(<id>)/Thing returns 200."""
        resp = client.get(f"Datastreams({format_id(seed.ds1.id)})/Thing")
        assert resp.status_code == 200, (
            f"req/request-data/status-code: many-to-one nav must return 200, "
            f"got {resp.status_code}"
        )
