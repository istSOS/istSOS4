"""
OGC SensorThings API v1.1 — Sensing Core (c01) conformance tests.

Target:   istSOS4 at http://localhost:8018/v4/v1.1  (override: STA_BASE_URL)
Standard: OGC 18-088 — SensorThings API Part 1: Sensing v1.1
          https://docs.ogc.org/is/18-088/18-088.html

Requirement namespaces used throughout:
  req/datamodel/...        §8  data model (entity properties & control info)
  req/resource-path/...    §9.2 resource path addressing
  req/request-data/...     §9.3 system query options
  req/service/...          §9.1 service document & serverSettings

Seed (entitiesDefault.json — loaded verbatim by the `seed` fixture):
  Thing "thing name 1"
    Location "location name 1"  Point(-117.05, 51.05)  application/vnd.geo+json
    Datastream DS1 "datastream name 1"  Lumen, ObsProp "Luminous Flux"
                   Sensor "sensor name 1",  results [3, 4]
    Datastream DS2 "datastream name 2"  Centigrade, ObsProp "Tempretaure"
                   Sensor "sensor name 2",  results [5, 6]
  4 Observations total; FeatureOfInterest auto-generated from the Location.

FROST parity: Capability1Tests.java + Capability1CoreOnlyTests.java (v2.x).
Run:
    pytest tests/conformance -m c01 -q
"""

from __future__ import annotations

import pytest

from client import (
    entity_id,
    format_id,
    id_from_self_link,
    self_link,
)

import sample_data

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Module-level fixtures (supplementing conftest session fixtures)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def hl_id(client, seed):
    """First HistoricalLocation id linked to the seed Thing (navigated at runtime).

    The server MUST create a HistoricalLocation when a Thing is linked to a
    Location (18-088 §8.2.2).
    """
    hl_list = client.values(
        f"Things({format_id(seed.thing_id)})/HistoricalLocations"
    )
    assert hl_list, (
        "seed Thing has no HistoricalLocations; "
        "server MUST create one when a Thing is linked to a Location"
    )
    return entity_id(hl_list[0])


@pytest.fixture(scope="module")
def obs_id(seed):
    """First Observation id from DS1 (results [3, 4])."""
    assert seed.ds1.observation_ids, "seed DS1 has no observations"
    return seed.ds1.observation_ids[0]


@pytest.fixture(scope="module")
def foi_id(seed):
    """First FeatureOfInterest id from the seed dataset."""
    assert seed.foi_ids, "seed has no FeaturesOfInterest"
    return seed.foi_ids[0]


# ============================================================================
# 1. Service root document
# ============================================================================

@pytest.mark.c01
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
# 2. Each collection GET → 200 with value[]
# ============================================================================

@pytest.mark.c01
class TestCollectionEndpoints:
    """req/resource-path/resource-path-to-entities (Usage 1) — the eight entity-set
    endpoints MUST all return HTTP 200 with a 'value' array.

    FROST parity: Capability1Tests.readEntitiesAndCheckResponse()
    """

    @pytest.mark.parametrize("collection", COLLECTION_NAMES)
    def test_collection_returns_200(self, client, collection):
        """req/resource-path — GET /<collection> returns HTTP 200."""
        resp = client.get(collection)
        assert resp.status_code == 200, (
            f"GET /{collection} expected 200, got {resp.status_code}"
        )

    @pytest.mark.parametrize("collection", COLLECTION_NAMES)
    def test_collection_has_value_array(self, client, collection):
        """req/resource-path — GET /<collection> body has 'value' array."""
        data = client.get_json(collection)
        assert "value" in data, f"/{collection} response missing 'value'"
        assert isinstance(data["value"], list), f"/{collection} 'value' must be a list"


# ============================================================================
# 3. Control information per entity
# ============================================================================

@pytest.mark.c01
class TestControlInformation:
    """req/datamodel/entity-control-information/common-control-information —
    every entity MUST carry @iot.id and absolute @iot.selfLink; every navigable
    relation MUST expose a <nav>@iot.navigationLink annotation.

    18-088 §8.1.1: 'Each entity SHALL have a unique identifier … returned as
    @iot.id'.  The selfLink SHALL be an absolute URL.

    FROST parity: Capability1Tests.readEntitiesAndCheckResponse() +
                  readEntityAndCheckResponse()
    """

    # ---- Thing ----
    def test_thing_has_iot_id_and_self_link(self, client, seed):
        """req/datamodel/entity-control-information — Thing has @iot.id, @iot.selfLink."""
        e = client.by_id("Things", seed.thing_id)
        assert "@iot.id" in e, "Thing missing @iot.id"
        assert "@iot.selfLink" in e, "Thing missing @iot.selfLink"
        assert e["@iot.selfLink"].startswith("http"), (
            "Thing @iot.selfLink must be absolute"
        )
        assert e["@iot.id"] == seed.thing_id

    def test_thing_navigation_links(self, client, seed):
        """req/datamodel/entity-control-information — Thing navigation links present."""
        e = client.by_id("Things", seed.thing_id)
        for nav in ("Locations", "HistoricalLocations", "Datastreams"):
            key = f"{nav}@iot.navigationLink"
            assert key in e, f"Thing missing navigation link: {key}"
            assert e[key].startswith("http"), f"{key} must be absolute"

    # ---- Location ----
    def test_location_has_iot_id_and_self_link(self, client, seed):
        """req/datamodel/entity-control-information — Location control info."""
        e = client.by_id("Locations", seed.location_id)
        assert "@iot.id" in e
        assert "@iot.selfLink" in e
        assert e["@iot.selfLink"].startswith("http")

    def test_location_navigation_links(self, client, seed):
        """req/datamodel/entity-control-information — Location navigation links."""
        e = client.by_id("Locations", seed.location_id)
        for nav in ("Things", "HistoricalLocations"):
            assert f"{nav}@iot.navigationLink" in e, (
                f"Location missing navigation link: {nav}@iot.navigationLink"
            )

    # ---- HistoricalLocation ----
    def test_historical_location_has_iot_id_and_self_link(self, client, hl_id):
        """req/datamodel/entity-control-information — HistoricalLocation control info."""
        e = client.by_id("HistoricalLocations", hl_id)
        assert "@iot.id" in e
        assert "@iot.selfLink" in e
        assert e["@iot.selfLink"].startswith("http")

    def test_historical_location_navigation_links(self, client, hl_id):
        """req/datamodel/entity-control-information — HistoricalLocation nav links."""
        e = client.by_id("HistoricalLocations", hl_id)
        for nav in ("Thing", "Locations"):
            assert f"{nav}@iot.navigationLink" in e, (
                f"HistoricalLocation missing: {nav}@iot.navigationLink"
            )

    # ---- Datastream (DS1) ----
    def test_datastream_has_iot_id_and_self_link(self, client, seed):
        """req/datamodel/entity-control-information — Datastream control info."""
        e = client.by_id("Datastreams", seed.ds1.id)
        assert "@iot.id" in e
        assert "@iot.selfLink" in e
        assert e["@iot.selfLink"].startswith("http")

    def test_datastream_navigation_links(self, client, seed):
        """req/datamodel/entity-control-information — Datastream navigation links."""
        e = client.by_id("Datastreams", seed.ds1.id)
        for nav in ("Thing", "Sensor", "ObservedProperty", "Observations"):
            assert f"{nav}@iot.navigationLink" in e, (
                f"Datastream missing: {nav}@iot.navigationLink"
            )

    # ---- Sensor (from DS1) ----
    def test_sensor_has_iot_id_and_self_link(self, client, seed):
        """req/datamodel/entity-control-information — Sensor control info."""
        e = client.by_id("Sensors", seed.ds1.sensor_id)
        assert "@iot.id" in e
        assert "@iot.selfLink" in e
        assert e["@iot.selfLink"].startswith("http")

    def test_sensor_navigation_links(self, client, seed):
        """req/datamodel/entity-control-information — Sensor navigation links."""
        e = client.by_id("Sensors", seed.ds1.sensor_id)
        assert "Datastreams@iot.navigationLink" in e

    # ---- ObservedProperty (from DS1) ----
    def test_observed_property_has_iot_id_and_self_link(self, client, seed):
        """req/datamodel/entity-control-information — ObservedProperty control info."""
        e = client.by_id("ObservedProperties", seed.ds1.observed_property_id)
        assert "@iot.id" in e
        assert "@iot.selfLink" in e
        assert e["@iot.selfLink"].startswith("http")

    def test_observed_property_navigation_links(self, client, seed):
        """req/datamodel/entity-control-information — ObservedProperty nav links."""
        e = client.by_id("ObservedProperties", seed.ds1.observed_property_id)
        assert "Datastreams@iot.navigationLink" in e

    # ---- Observation (from DS1) ----
    def test_observation_has_iot_id_and_self_link(self, client, obs_id):
        """req/datamodel/entity-control-information — Observation control info."""
        e = client.by_id("Observations", obs_id)
        assert "@iot.id" in e
        assert "@iot.selfLink" in e
        assert e["@iot.selfLink"].startswith("http")

    def test_observation_navigation_links(self, client, obs_id):
        """req/datamodel/entity-control-information — Observation navigation links."""
        e = client.by_id("Observations", obs_id)
        for nav in ("Datastream", "FeatureOfInterest"):
            assert f"{nav}@iot.navigationLink" in e, (
                f"Observation missing: {nav}@iot.navigationLink"
            )

    # ---- FeatureOfInterest ----
    def test_foi_has_iot_id_and_self_link(self, client, foi_id):
        """req/datamodel/entity-control-information — FeatureOfInterest control info."""
        e = client.by_id("FeaturesOfInterest", foi_id)
        assert "@iot.id" in e
        assert "@iot.selfLink" in e
        assert e["@iot.selfLink"].startswith("http")

    def test_foi_navigation_links(self, client, foi_id):
        """req/datamodel/entity-control-information — FeatureOfInterest nav links."""
        e = client.by_id("FeaturesOfInterest", foi_id)
        assert "Observations@iot.navigationLink" in e

    # ---- selfLink resolves to same entity ----
    def test_self_link_resolves_to_same_entity(self, client, seed):
        """req/datamodel/entity-control-information — @iot.selfLink resolves correctly."""
        e = client.by_id("Things", seed.thing_id)
        e2 = client.follow_self_link(e)
        assert entity_id(e2) == entity_id(e)
        assert e2["name"] == e["name"]

    # ---- control info on collection items (not just by-id) ----
    def test_collection_items_have_control_info(self, client, seed):
        """req/datamodel/entity-control-information — entities in collection have
        @iot.id and @iot.selfLink.
        """
        data = client.collection("Things")
        for item in data["value"]:
            assert "@iot.id" in item, f"Collection item missing @iot.id: {item}"
            assert "@iot.selfLink" in item, (
                f"Collection item missing @iot.selfLink: {item}"
            )


# ============================================================================
# 4. Mandatory properties per entity type
# ============================================================================

@pytest.mark.c01
class TestMandatoryProperties:
    """req/datamodel/<entity> — each entity type MUST expose its mandatory
    properties as defined in 18-088 §8 entity tables.

    FROST parity: Capability1Tests.readEntitiesAndCheckResponse() +
                  readEntityAndCheckResponse() property checks.
    """

    def test_thing_mandatory_properties(self, client, seed):
        """req/datamodel/thing/properties — Thing MUST have name, description."""
        e = client.by_id("Things", seed.thing_id)
        for prop in ("name", "description"):
            assert prop in e, f"Thing missing mandatory property '{prop}'"
        assert e["name"] == sample_data.THING_NAME
        assert e["name"] == seed.thing_name

    def test_location_mandatory_properties(self, client, seed):
        """req/datamodel/location/properties — Location MUST have name, description,
        encodingType, location.
        """
        e = client.by_id("Locations", seed.location_id)
        for prop in ("name", "description", "encodingType", "location"):
            assert prop in e, f"Location missing mandatory property '{prop}'"
        assert e["name"] == sample_data.LOCATION_NAME
        assert e["encodingType"] == sample_data.LOCATION_ENCODING

    def test_historical_location_mandatory_properties(self, client, hl_id):
        """req/datamodel/historical-location/properties — HistoricalLocation MUST have time."""
        e = client.by_id("HistoricalLocations", hl_id)
        assert "time" in e, "HistoricalLocation missing mandatory property 'time'"
        assert e["time"], "HistoricalLocation 'time' must not be empty"

    def test_datastream_mandatory_properties_ds1(self, client, seed):
        """req/datamodel/datastream/properties — Datastream MUST have name, description,
        unitOfMeasurement, observationType.
        """
        e = client.by_id("Datastreams", seed.ds1.id)
        for prop in ("name", "description", "unitOfMeasurement", "observationType"):
            assert prop in e, f"Datastream DS1 missing mandatory property '{prop}'"
        assert e["name"] == sample_data.DS1_NAME

    def test_datastream_mandatory_properties_ds2(self, client, seed):
        """req/datamodel/datastream/properties — DS2 also has mandatory properties."""
        e = client.by_id("Datastreams", seed.ds2.id)
        for prop in ("name", "description", "unitOfMeasurement", "observationType"):
            assert prop in e, f"Datastream DS2 missing mandatory property '{prop}'"
        assert e["name"] == sample_data.DS2_NAME

    def test_sensor_mandatory_properties(self, client, seed):
        """req/datamodel/sensor/properties — Sensor MUST have name, description,
        encodingType, metadata.
        """
        e = client.by_id("Sensors", seed.ds1.sensor_id)
        for prop in ("name", "description", "encodingType", "metadata"):
            assert prop in e, f"Sensor missing mandatory property '{prop}'"
        assert e["name"] == sample_data.DS1_SENSOR

    def test_observed_property_mandatory_properties(self, client, seed):
        """req/datamodel/observed-property/properties — ObservedProperty MUST have
        name, definition, description.
        """
        e = client.by_id("ObservedProperties", seed.ds1.observed_property_id)
        for prop in ("name", "definition", "description"):
            assert prop in e, f"ObservedProperty missing mandatory property '{prop}'"
        assert e["name"] == sample_data.DS1_OBSERVED_PROPERTY

    def test_observation_mandatory_properties(self, client, obs_id):
        """req/datamodel/observation/properties — Observation MUST have phenomenonTime and result."""
        e = client.by_id("Observations", obs_id)
        assert "phenomenonTime" in e, "Observation missing mandatory 'phenomenonTime'"
        assert "result" in e, "Observation missing mandatory 'result'"

    def test_foi_mandatory_properties(self, client, foi_id):
        """req/datamodel/feature-of-interest/properties — FeatureOfInterest MUST have
        name, description, encodingType, feature.
        """
        e = client.by_id("FeaturesOfInterest", foi_id)
        for prop in ("name", "description", "encodingType", "feature"):
            assert prop in e, f"FeatureOfInterest missing mandatory property '{prop}'"


# ============================================================================
# 5. Entity by id
# ============================================================================

@pytest.mark.c01
class TestEntityById:
    """req/resource-path/resource-path-to-entities (Usage 2) — addressing a single
    entity by id MUST return 200 and the entity document.

    FROST parity: Capability1Tests.readEntityAndCheckResponse()
    """

    def test_thing_by_id_status_200(self, client, seed):
        """req/resource-path — GET /Things(<id>) returns 200."""
        resp = client.get(f"Things({format_id(seed.thing_id)})")
        assert resp.status_code == 200, (
            f"Expected 200, got {resp.status_code}: {resp.text[:200]}"
        )

    def test_thing_by_id_identity(self, client, seed):
        """req/resource-path — entity fetched by id has expected @iot.id and name."""
        e = client.by_id("Things", seed.thing_id)
        assert e["@iot.id"] == seed.thing_id
        assert e["name"] == seed.thing_name

    def test_self_link_consistency(self, client, seed):
        """req/datamodel/entity-control-information — @iot.selfLink resolves to
        the same entity.
        """
        e = client.by_id("Things", seed.thing_id)
        e2 = client.follow_self_link(e)
        assert entity_id(e2) == entity_id(e)
        assert e2["name"] == e["name"]

    def test_location_by_id(self, client, seed):
        """req/resource-path — GET /Locations(<id>) returns 200."""
        resp = client.get(f"Locations({format_id(seed.location_id)})")
        assert resp.status_code == 200
        assert resp.json()["@iot.id"] == seed.location_id

    def test_datastream_by_id_ds1(self, client, seed):
        """req/resource-path — GET /Datastreams(<DS1 id>) returns 200."""
        resp = client.get(f"Datastreams({format_id(seed.ds1.id)})")
        assert resp.status_code == 200
        assert resp.json()["@iot.id"] == seed.ds1.id

    def test_datastream_by_id_ds2(self, client, seed):
        """req/resource-path — GET /Datastreams(<DS2 id>) returns 200."""
        resp = client.get(f"Datastreams({format_id(seed.ds2.id)})")
        assert resp.status_code == 200
        assert resp.json()["@iot.id"] == seed.ds2.id

    def test_sensor_by_id(self, client, seed):
        """req/resource-path — GET /Sensors(<id>) returns 200."""
        resp = client.get(f"Sensors({format_id(seed.ds1.sensor_id)})")
        assert resp.status_code == 200
        assert resp.json()["@iot.id"] == seed.ds1.sensor_id

    def test_observed_property_by_id(self, client, seed):
        """req/resource-path — GET /ObservedProperties(<id>) returns 200."""
        resp = client.get(
            f"ObservedProperties({format_id(seed.ds1.observed_property_id)})"
        )
        assert resp.status_code == 200
        assert resp.json()["@iot.id"] == seed.ds1.observed_property_id

    def test_observation_by_id(self, client, obs_id):
        """req/resource-path — GET /Observations(<id>) returns 200."""
        resp = client.get(f"Observations({format_id(obs_id)})")
        assert resp.status_code == 200
        assert resp.json()["@iot.id"] == obs_id

    def test_foi_by_id(self, client, foi_id):
        """req/resource-path — GET /FeaturesOfInterest(<id>) returns 200."""
        resp = client.get(f"FeaturesOfInterest({format_id(foi_id)})")
        assert resp.status_code == 200
        assert resp.json()["@iot.id"] == foi_id

    def test_historical_location_by_id(self, client, hl_id):
        """req/resource-path — GET /HistoricalLocations(<id>) returns 200."""
        resp = client.get(f"HistoricalLocations({format_id(hl_id)})")
        assert resp.status_code == 200
        assert resp.json()["@iot.id"] == hl_id


# ============================================================================
# 6. Property access  /Entity(<id>)/<propName>
# ============================================================================

@pytest.mark.c01
class TestPropertyAccess:
    """req/resource-path/resource-path-to-entities (Usage 4) — addressing a named
    property returns a JSON object { "<propName>": <value> } containing ONLY that
    property key.

    FROST parity: Capability1Tests.readPropertyOfEntityAndCheckResponse()
    """

    def test_thing_name_property(self, client, seed):
        """req/resource-path — GET /Things(<id>)/name → {\"name\": ...}."""
        resp = client.get(f"Things({format_id(seed.thing_id)})/name")
        assert resp.status_code == 200
        data = resp.json()
        assert "name" in data
        assert data["name"] == seed.thing_name

    def test_thing_description_property(self, client, seed):
        """req/resource-path — GET /Things(<id>)/description → {\"description\": ...}."""
        resp = client.get(f"Things({format_id(seed.thing_id)})/description")
        assert resp.status_code == 200
        data = resp.json()
        assert "description" in data

    def test_datastream_name_property_ds1(self, client, seed):
        """req/resource-path — GET /Datastreams(<DS1>)/name → {\"name\": ...}."""
        resp = client.get(f"Datastreams({format_id(seed.ds1.id)})/name")
        assert resp.status_code == 200
        data = resp.json()
        assert "name" in data
        assert data["name"] == seed.ds1.name

    def test_datastream_unit_of_measurement_property(self, client, seed):
        """req/resource-path — GET /Datastreams(<DS1>)/unitOfMeasurement returns object."""
        resp = client.get(
            f"Datastreams({format_id(seed.ds1.id)})/unitOfMeasurement"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "unitOfMeasurement" in data
        assert isinstance(data["unitOfMeasurement"], dict)

    def test_observation_result_property(self, client, seed, obs_id):
        """req/resource-path — GET /Observations(<id>)/result → {\"result\": ...}."""
        resp = client.get(f"Observations({format_id(obs_id)})/result")
        assert resp.status_code == 200
        data = resp.json()
        assert "result" in data
        assert data["result"] in seed.ds1.results

    def test_location_encoding_type_property(self, client, seed):
        """req/resource-path — GET /Locations(<id>)/encodingType."""
        resp = client.get(f"Locations({format_id(seed.location_id)})/encodingType")
        assert resp.status_code == 200
        data = resp.json()
        assert "encodingType" in data

    def test_sensor_metadata_property(self, client, seed):
        """req/resource-path — GET /Sensors(<id>)/metadata."""
        resp = client.get(f"Sensors({format_id(seed.ds1.sensor_id)})/metadata")
        assert resp.status_code == 200
        data = resp.json()
        assert "metadata" in data

    def test_property_response_contains_only_requested_key(self, client, seed):
        """req/resource-path — property response contains only the requested key
        (no @iot.id, no sibling properties).
        """
        resp = client.get(f"Things({format_id(seed.thing_id)})/name")
        data = resp.json()
        assert "name" in data
        assert "@iot.id" not in data, (
            "Property response must not include @iot.id"
        )
        assert "description" not in data, (
            "Property response must not include other properties"
        )

    def test_observed_property_definition_property(self, client, seed):
        """req/resource-path — GET /ObservedProperties(<id>)/definition."""
        resp = client.get(
            f"ObservedProperties({format_id(seed.ds1.observed_property_id)})/definition"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "definition" in data


# ============================================================================
# 7. $value — raw scalar result
# ============================================================================

@pytest.mark.c01
class TestDollarValue:
    """req/resource-path/resource-path-to-entities (Usage 5) — addressing
    /<entity>(<id>)/<prop>/$value returns the raw value of the property
    (text/plain for strings, plain numeric text for numbers), NOT a JSON object.

    18-088 §9.2 Usage 5: 'the raw value of the specified property SHALL be
    returned'.

    FROST parity: Capability1Tests.readPropertyOfEntityAndCheckResponse()
    """

    def test_thing_name_dollar_value_status(self, client, seed):
        """req/resource-path — GET /Things(<id>)/name/$value returns 200."""
        resp = client.get(f"Things({format_id(seed.thing_id)})/name/$value")
        assert resp.status_code == 200, (
            f"Expected 200, got {resp.status_code}: {resp.text[:200]}"
        )

    def test_thing_name_dollar_value_is_raw_scalar(self, client, seed):
        """req/resource-path — /Things(<id>)/name/$value returns raw string."""
        resp = client.get(f"Things({format_id(seed.thing_id)})/name/$value")
        assert resp.status_code == 200
        # Raw text: no surrounding JSON quotes
        assert resp.text == seed.thing_name, (
            f"Expected raw '{seed.thing_name}', got: {resp.text!r}"
        )

    def test_thing_name_dollar_value_content_type(self, client, seed):
        """req/resource-path — $value response has text/plain Content-Type."""
        resp = client.get(f"Things({format_id(seed.thing_id)})/name/$value")
        ct = resp.headers.get("content-type", "")
        assert "text/plain" in ct, (
            f"$value response should be text/plain, got Content-Type: {ct}"
        )

    def test_datastream_name_dollar_value(self, client, seed):
        """req/resource-path — /Datastreams(<DS1>)/name/$value → raw string."""
        resp = client.get(f"Datastreams({format_id(seed.ds1.id)})/name/$value")
        assert resp.status_code == 200
        assert resp.text == seed.ds1.name, (
            f"Expected raw '{seed.ds1.name}', got: {resp.text!r}"
        )

    def test_sensor_name_dollar_value(self, client, seed):
        """req/resource-path — /Sensors(<id>)/name/$value → raw string."""
        resp = client.get(f"Sensors({format_id(seed.ds1.sensor_id)})/name/$value")
        assert resp.status_code == 200
        assert resp.text == seed.ds1.sensor_name, (
            f"Expected raw '{seed.ds1.sensor_name}', got: {resp.text!r}"
        )

    def test_observed_property_name_dollar_value(self, client, seed):
        """req/resource-path — /ObservedProperties(<id>)/name/$value → raw string."""
        resp = client.get(
            f"ObservedProperties({format_id(seed.ds1.observed_property_id)})/name/$value"
        )
        assert resp.status_code == 200
        assert resp.text == seed.ds1.observed_property_name, (
            f"Expected raw '{seed.ds1.observed_property_name}', got: {resp.text!r}"
        )

    def test_location_name_dollar_value(self, client, seed):
        """req/resource-path — /Locations(<id>)/name/$value → raw string."""
        resp = client.get(f"Locations({format_id(seed.location_id)})/name/$value")
        assert resp.status_code == 200
        assert resp.text == seed.location_name, (
            f"Expected raw '{seed.location_name}', got: {resp.text!r}"
        )

    def test_ds2_name_dollar_value(self, client, seed):
        """req/resource-path — /Datastreams(<DS2>)/name/$value → raw string."""
        resp = client.get(f"Datastreams({format_id(seed.ds2.id)})/name/$value")
        assert resp.status_code == 200
        assert resp.text == seed.ds2.name, (
            f"Expected raw '{seed.ds2.name}', got: {resp.text!r}"
        )


# ============================================================================
# 8. One-to-many navigation
# ============================================================================

@pytest.mark.c01
class TestNavigationOneToMany:
    """req/resource-path/resource-path-to-entities — navigating from a single
    entity to its related collection endpoint MUST return 200 and a `value[]`.

    One-to-many relations (18-088 §8 entity tables):
      Thing      → Datastreams (2 in seed), Locations, HistoricalLocations
      Datastream → Observations (2 per DS)
      Location   → Things, HistoricalLocations
      Sensor     → Datastreams
      ObservedProperty → Datastreams
      FeatureOfInterest → Observations

    FROST parity: Capability1Tests.checkResourcePaths()
    """

    def test_thing_to_datastreams_contains_both(self, client, seed):
        """req/resource-path — /Things(<id>)/Datastreams contains DS1 and DS2."""
        resp = client.get(f"Things({format_id(seed.thing_id)})/Datastreams")
        assert resp.status_code == 200
        data = resp.json()
        assert "value" in data
        ids = [entity_id(e) for e in data["value"]]
        for ds_id in seed.datastream_ids:
            assert ds_id in ids, (
                f"Datastream {ds_id} not found in Things/<id>/Datastreams"
            )

    def test_thing_to_locations(self, client, seed):
        """req/resource-path — /Things(<id>)/Locations contains seed Location."""
        resp = client.get(f"Things({format_id(seed.thing_id)})/Locations")
        assert resp.status_code == 200
        data = resp.json()
        assert "value" in data
        ids = [entity_id(e) for e in data["value"]]
        assert seed.location_id in ids

    def test_thing_to_historical_locations(self, client, seed, hl_id):
        """req/resource-path — /Things(<id>)/HistoricalLocations contains seed HL."""
        resp = client.get(
            f"Things({format_id(seed.thing_id)})/HistoricalLocations"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "value" in data
        ids = [entity_id(e) for e in data["value"]]
        assert hl_id in ids

    def test_ds1_to_observations(self, client, seed):
        """req/resource-path — /Datastreams(<DS1>)/Observations contains DS1 obs."""
        resp = client.get(
            f"Datastreams({format_id(seed.ds1.id)})/Observations"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "value" in data
        ids = [entity_id(e) for e in data["value"]]
        for oid in seed.ds1.observation_ids:
            assert oid in ids, f"Obs {oid} not in DS1/Observations"

    def test_ds2_to_observations(self, client, seed):
        """req/resource-path — /Datastreams(<DS2>)/Observations contains DS2 obs."""
        resp = client.get(
            f"Datastreams({format_id(seed.ds2.id)})/Observations"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "value" in data
        ids = [entity_id(e) for e in data["value"]]
        for oid in seed.ds2.observation_ids:
            assert oid in ids, f"Obs {oid} not in DS2/Observations"

    def test_ds1_observations_do_not_include_ds2_obs(self, client, seed):
        """req/resource-path — DS1/Observations must NOT contain DS2 observations
        (navigation is scoped to the addressed Datastream).
        """
        resp = client.get(
            f"Datastreams({format_id(seed.ds1.id)})/Observations"
        )
        ids = [entity_id(e) for e in resp.json()["value"]]
        for oid in seed.ds2.observation_ids:
            assert oid not in ids, (
                f"DS2 observation {oid} must not appear in DS1/Observations"
            )

    def test_location_to_things(self, client, seed):
        """req/resource-path — /Locations(<id>)/Things contains seed Thing."""
        resp = client.get(f"Locations({format_id(seed.location_id)})/Things")
        assert resp.status_code == 200
        data = resp.json()
        assert "value" in data
        ids = [entity_id(e) for e in data["value"]]
        assert seed.thing_id in ids

    def test_location_to_historical_locations(self, client, seed, hl_id):
        """req/resource-path — /Locations(<id>)/HistoricalLocations."""
        resp = client.get(
            f"Locations({format_id(seed.location_id)})/HistoricalLocations"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "value" in data
        ids = [entity_id(e) for e in data["value"]]
        assert hl_id in ids

    def test_sensor_ds1_to_datastreams(self, client, seed):
        """req/resource-path — /Sensors(<DS1 sensor>)/Datastreams → DS1."""
        resp = client.get(
            f"Sensors({format_id(seed.ds1.sensor_id)})/Datastreams"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "value" in data
        ids = [entity_id(e) for e in data["value"]]
        assert seed.ds1.id in ids

    def test_sensor_ds2_to_datastreams(self, client, seed):
        """req/resource-path — /Sensors(<DS2 sensor>)/Datastreams → DS2."""
        resp = client.get(
            f"Sensors({format_id(seed.ds2.sensor_id)})/Datastreams"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "value" in data
        ids = [entity_id(e) for e in data["value"]]
        assert seed.ds2.id in ids

    def test_observed_property_ds1_to_datastreams(self, client, seed):
        """req/resource-path — /ObservedProperties(<DS1 OP>)/Datastreams → DS1."""
        resp = client.get(
            f"ObservedProperties({format_id(seed.ds1.observed_property_id)})"
            f"/Datastreams"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "value" in data
        ids = [entity_id(e) for e in data["value"]]
        assert seed.ds1.id in ids

    def test_foi_to_observations(self, client, seed, foi_id):
        """req/resource-path — /FeaturesOfInterest(<id>)/Observations."""
        resp = client.get(
            f"FeaturesOfInterest({format_id(foi_id)})/Observations"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "value" in data
        obs_ids = [entity_id(e) for e in data["value"]]
        matched = [oid for oid in seed.all_observation_ids if oid in obs_ids]
        assert matched, (
            "None of the seed observations found in FeaturesOfInterest/<id>/Observations"
        )


# ============================================================================
# 9. Many-to-one navigation
# ============================================================================

@pytest.mark.c01
class TestNavigationManyToOne:
    """req/resource-path/resource-path-to-entities — navigating from an entity to
    a single related entity MUST return 200 and a single entity document (not a
    collection).

    Many-to-one relations (18-088 §8):
      Datastream → Thing, Sensor, ObservedProperty
      Observation → Datastream, FeatureOfInterest
      HistoricalLocation → Thing

    FROST parity: Capability1Tests.checkResourcePaths()
    """

    def test_ds1_to_thing(self, client, seed):
        """req/resource-path — /Datastreams(<DS1>)/Thing → seed Thing."""
        resp = client.get(f"Datastreams({format_id(seed.ds1.id)})/Thing")
        assert resp.status_code == 200
        data = resp.json()
        assert "@iot.id" in data, "Many-to-one must return single entity"
        assert "value" not in data, "Many-to-one must NOT return 'value' array"
        assert data["@iot.id"] == seed.thing_id

    def test_ds2_to_thing(self, client, seed):
        """req/resource-path — /Datastreams(<DS2>)/Thing → same seed Thing."""
        resp = client.get(f"Datastreams({format_id(seed.ds2.id)})/Thing")
        assert resp.status_code == 200
        data = resp.json()
        assert "@iot.id" in data
        assert data["@iot.id"] == seed.thing_id

    def test_ds1_to_sensor(self, client, seed):
        """req/resource-path — /Datastreams(<DS1>)/Sensor → DS1 sensor."""
        resp = client.get(f"Datastreams({format_id(seed.ds1.id)})/Sensor")
        assert resp.status_code == 200
        data = resp.json()
        assert "@iot.id" in data
        assert "value" not in data
        assert data["@iot.id"] == seed.ds1.sensor_id

    def test_ds2_to_sensor(self, client, seed):
        """req/resource-path — /Datastreams(<DS2>)/Sensor → DS2 sensor."""
        resp = client.get(f"Datastreams({format_id(seed.ds2.id)})/Sensor")
        assert resp.status_code == 200
        data = resp.json()
        assert data["@iot.id"] == seed.ds2.sensor_id

    def test_ds1_to_observed_property(self, client, seed):
        """req/resource-path — /Datastreams(<DS1>)/ObservedProperty → DS1 OP."""
        resp = client.get(
            f"Datastreams({format_id(seed.ds1.id)})/ObservedProperty"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "@iot.id" in data
        assert "value" not in data
        assert data["@iot.id"] == seed.ds1.observed_property_id

    def test_ds2_to_observed_property(self, client, seed):
        """req/resource-path — /Datastreams(<DS2>)/ObservedProperty → DS2 OP."""
        resp = client.get(
            f"Datastreams({format_id(seed.ds2.id)})/ObservedProperty"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["@iot.id"] == seed.ds2.observed_property_id

    def test_observation_to_datastream(self, client, seed, obs_id):
        """req/resource-path — /Observations(<id>)/Datastream → DS1."""
        resp = client.get(f"Observations({format_id(obs_id)})/Datastream")
        assert resp.status_code == 200
        data = resp.json()
        assert "@iot.id" in data
        assert "value" not in data
        assert data["@iot.id"] == seed.ds1.id

    def test_observation_to_foi(self, client, seed, obs_id):
        """req/resource-path — /Observations(<id>)/FeatureOfInterest → auto FOI."""
        resp = client.get(f"Observations({format_id(obs_id)})/FeatureOfInterest")
        assert resp.status_code == 200
        data = resp.json()
        assert "@iot.id" in data
        assert "value" not in data
        assert data["@iot.id"] in seed.foi_ids

    def test_historical_location_to_thing(self, client, seed, hl_id):
        """req/resource-path — /HistoricalLocations(<id>)/Thing → seed Thing."""
        resp = client.get(f"HistoricalLocations({format_id(hl_id)})/Thing")
        assert resp.status_code == 200
        data = resp.json()
        assert "@iot.id" in data
        assert "value" not in data
        assert data["@iot.id"] == seed.thing_id


# ============================================================================
# 10. Deep resource paths
# ============================================================================

@pytest.mark.c01
class TestDeepResourcePaths:
    """req/resource-path/resource-path-to-entities — the spec allows multi-hop
    resource paths (18-088 §9.2 recursive composition).

    FROST parity: Capability1Tests.checkResourcePaths() (up to 4 levels).
    """

    def test_things_ds1_observations(self, client, seed):
        """req/resource-path — /Things(<id>)/Datastreams(<DS1>)/Observations."""
        path = (
            f"Things({format_id(seed.thing_id)})"
            f"/Datastreams({format_id(seed.ds1.id)})"
            f"/Observations"
        )
        resp = client.get(path)
        assert resp.status_code == 200
        data = resp.json()
        assert "value" in data
        ids = [entity_id(e) for e in data["value"]]
        for oid in seed.ds1.observation_ids:
            assert oid in ids

    def test_things_ds2_observations(self, client, seed):
        """req/resource-path — /Things(<id>)/Datastreams(<DS2>)/Observations."""
        path = (
            f"Things({format_id(seed.thing_id)})"
            f"/Datastreams({format_id(seed.ds2.id)})"
            f"/Observations"
        )
        resp = client.get(path)
        assert resp.status_code == 200
        data = resp.json()
        assert "value" in data
        ids = [entity_id(e) for e in data["value"]]
        for oid in seed.ds2.observation_ids:
            assert oid in ids

    def test_ds1_thing_locations(self, client, seed):
        """req/resource-path — /Datastreams(<DS1>)/Thing/Locations."""
        path = f"Datastreams({format_id(seed.ds1.id)})/Thing/Locations"
        resp = client.get(path)
        assert resp.status_code == 200
        data = resp.json()
        assert "value" in data
        ids = [entity_id(e) for e in data["value"]]
        assert seed.location_id in ids

    def test_observation_ds_observed_property(self, client, seed, obs_id):
        """req/resource-path — /Observations(<id>)/Datastream/ObservedProperty."""
        path = f"Observations({format_id(obs_id)})/Datastream/ObservedProperty"
        resp = client.get(path)
        assert resp.status_code == 200
        data = resp.json()
        assert "@iot.id" in data
        assert data["@iot.id"] == seed.ds1.observed_property_id

    def test_observation_ds_thing(self, client, seed, obs_id):
        """req/resource-path — /Observations(<id>)/Datastream/Thing."""
        path = f"Observations({format_id(obs_id)})/Datastream/Thing"
        resp = client.get(path)
        assert resp.status_code == 200
        data = resp.json()
        assert "@iot.id" in data
        assert data["@iot.id"] == seed.thing_id

    def test_thing_ds1_sensor(self, client, seed):
        """req/resource-path — /Things(<id>)/Datastreams(<DS1>)/Sensor."""
        path = (
            f"Things({format_id(seed.thing_id)})"
            f"/Datastreams({format_id(seed.ds1.id)})"
            f"/Sensor"
        )
        resp = client.get(path)
        assert resp.status_code == 200
        data = resp.json()
        assert "@iot.id" in data
        assert data["@iot.id"] == seed.ds1.sensor_id

    def test_thing_ds1_observed_property(self, client, seed):
        """req/resource-path — /Things(<id>)/Datastreams(<DS1>)/ObservedProperty."""
        path = (
            f"Things({format_id(seed.thing_id)})"
            f"/Datastreams({format_id(seed.ds1.id)})"
            f"/ObservedProperty"
        )
        resp = client.get(path)
        assert resp.status_code == 200
        data = resp.json()
        assert "@iot.id" in data
        assert data["@iot.id"] == seed.ds1.observed_property_id

    def test_ds1_thing_historical_locations(self, client, seed, hl_id):
        """req/resource-path — /Datastreams(<DS1>)/Thing/HistoricalLocations."""
        path = (
            f"Datastreams({format_id(seed.ds1.id)})/Thing/HistoricalLocations"
        )
        resp = client.get(path)
        assert resp.status_code == 200
        data = resp.json()
        assert "value" in data
        ids = [entity_id(e) for e in data["value"]]
        assert hl_id in ids


# ============================================================================
# 11. $ref — association links
# ============================================================================

@pytest.mark.c01
class TestAssociationLinks:
    """req/resource-path/resource-path-to-entities (Usage 7) — /<navLink>/$ref
    returns only selfLinks of the related entity/entities.

    One-to-many: { "value": [{"@iot.selfLink": ...}] }
    Many-to-one: { "@iot.selfLink": ... }

    18-088 §9.2 associationLink: 'A URL that returns the self-links of related
    entities … <assocLink> = <navLink>/$ref'.

    FROST parity: Capability1Tests.checkResourcePaths() ($ref branches).
    """

    def test_thing_datastreams_ref_is_collection(self, client, seed):
        """req/resource-path — /Things(<id>)/Datastreams/$ref → array of selfLinks."""
        resp = client.get(f"Things({format_id(seed.thing_id)})/Datastreams/$ref")
        assert resp.status_code == 200
        data = resp.json()
        assert "value" in data, "$ref for 1-to-many must return {'value': [...]}"
        for entry in data["value"]:
            assert "@iot.selfLink" in entry
            non_ref = [k for k in entry if k != "@iot.selfLink"]
            assert not non_ref, (
                f"$ref entry must contain ONLY '@iot.selfLink', found: {non_ref}"
            )
        # Both seed datastreams must appear
        self_links = [e["@iot.selfLink"] for e in data["value"]]
        for ds_id in seed.datastream_ids:
            assert any(format_id(ds_id) in sl for sl in self_links), (
                f"Datastream {ds_id} selfLink missing from Things/$ref"
            )

    def test_observation_datastream_ref_is_single(self, client, seed, obs_id):
        """req/resource-path — /Observations(<id>)/Datastream/$ref → single selfLink."""
        resp = client.get(f"Observations({format_id(obs_id)})/Datastream/$ref")
        assert resp.status_code == 200
        data = resp.json()
        assert "@iot.selfLink" in data
        assert "value" not in data, "$ref for many-to-one must NOT have 'value' array"
        assert format_id(seed.ds1.id) in data["@iot.selfLink"]

    def test_ds1_observations_ref_collection(self, client, seed):
        """req/resource-path — /Datastreams(<DS1>)/Observations/$ref → array."""
        resp = client.get(
            f"Datastreams({format_id(seed.ds1.id)})/Observations/$ref"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "value" in data
        self_links = [e["@iot.selfLink"] for e in data["value"]]
        for oid in seed.ds1.observation_ids:
            assert any(format_id(oid) in sl for sl in self_links), (
                f"Observation {oid} missing from DS1/Observations/$ref"
            )

    def test_ds2_observations_ref_collection(self, client, seed):
        """req/resource-path — /Datastreams(<DS2>)/Observations/$ref → array."""
        resp = client.get(
            f"Datastreams({format_id(seed.ds2.id)})/Observations/$ref"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "value" in data
        self_links = [e["@iot.selfLink"] for e in data["value"]]
        for oid in seed.ds2.observation_ids:
            assert any(format_id(oid) in sl for sl in self_links)

    def test_observation_foi_ref_is_single(self, client, seed, obs_id):
        """req/resource-path — /Observations(<id>)/FeatureOfInterest/$ref → single."""
        resp = client.get(
            f"Observations({format_id(obs_id)})/FeatureOfInterest/$ref"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "@iot.selfLink" in data
        assert "value" not in data

    def test_ds1_thing_ref_is_single(self, client, seed):
        """req/resource-path — /Datastreams(<DS1>)/Thing/$ref → single selfLink."""
        resp = client.get(f"Datastreams({format_id(seed.ds1.id)})/Thing/$ref")
        assert resp.status_code == 200
        data = resp.json()
        assert "@iot.selfLink" in data
        assert format_id(seed.thing_id) in data["@iot.selfLink"]

    def test_ds1_sensor_ref_is_single(self, client, seed):
        """req/resource-path — /Datastreams(<DS1>)/Sensor/$ref → single selfLink."""
        resp = client.get(f"Datastreams({format_id(seed.ds1.id)})/Sensor/$ref")
        assert resp.status_code == 200
        data = resp.json()
        assert "@iot.selfLink" in data
        assert format_id(seed.ds1.sensor_id) in data["@iot.selfLink"]

    def test_ds1_observed_property_ref_is_single(self, client, seed):
        """req/resource-path — /Datastreams(<DS1>)/ObservedProperty/$ref → single."""
        resp = client.get(
            f"Datastreams({format_id(seed.ds1.id)})/ObservedProperty/$ref"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "@iot.selfLink" in data
        assert format_id(seed.ds1.observed_property_id) in data["@iot.selfLink"]

    def test_location_things_ref_collection(self, client, seed):
        """req/resource-path — /Locations(<id>)/Things/$ref → array."""
        resp = client.get(f"Locations({format_id(seed.location_id)})/Things/$ref")
        assert resp.status_code == 200
        data = resp.json()
        assert "value" in data
        self_links = [e["@iot.selfLink"] for e in data["value"]]
        assert any(format_id(seed.thing_id) in sl for sl in self_links)


# ============================================================================
# 12. Nested property + $value on a related entity
# ============================================================================

@pytest.mark.c01
class TestNestedPropertyAccess:
    """req/resource-path/resource-path-to-entities — property access and $value
    work at the end of a multi-hop navigation path.
    """

    def test_ds1_thing_name(self, client, seed):
        """req/resource-path — /Datastreams(<DS1>)/Thing/name → {\"name\": ...}."""
        resp = client.get(f"Datastreams({format_id(seed.ds1.id)})/Thing/name")
        assert resp.status_code == 200
        data = resp.json()
        assert "name" in data
        assert data["name"] == seed.thing_name

    def test_ds1_thing_name_dollar_value(self, client, seed):
        """req/resource-path — /Datastreams(<DS1>)/Thing/name/$value → raw scalar."""
        resp = client.get(
            f"Datastreams({format_id(seed.ds1.id)})/Thing/name/$value"
        )
        assert resp.status_code == 200
        assert resp.text == seed.thing_name, (
            f"Expected raw '{seed.thing_name}', got: {resp.text!r}"
        )

    def test_observation_ds_name(self, client, seed, obs_id):
        """req/resource-path — /Observations(<id>)/Datastream/name → DS1 name."""
        resp = client.get(
            f"Observations({format_id(obs_id)})/Datastream/name"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "name" in data
        assert data["name"] == seed.ds1.name

    def test_ds1_sensor_name(self, client, seed):
        """req/resource-path — /Datastreams(<DS1>)/Sensor/name → sensor name."""
        resp = client.get(f"Datastreams({format_id(seed.ds1.id)})/Sensor/name")
        assert resp.status_code == 200
        data = resp.json()
        assert "name" in data
        assert data["name"] == seed.ds1.sensor_name

    def test_ds1_observed_property_name(self, client, seed):
        """req/resource-path — /Datastreams(<DS1>)/ObservedProperty/name."""
        resp = client.get(
            f"Datastreams({format_id(seed.ds1.id)})/ObservedProperty/name"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "name" in data
        assert data["name"] == seed.ds1.observed_property_name

    def test_ds2_thing_name_dollar_value(self, client, seed):
        """req/resource-path — /Datastreams(<DS2>)/Thing/name/$value → raw scalar."""
        resp = client.get(
            f"Datastreams({format_id(seed.ds2.id)})/Thing/name/$value"
        )
        assert resp.status_code == 200
        assert resp.text == seed.thing_name


# ============================================================================
# 13. @iot.count and @iot.nextLink semantics
# ============================================================================

@pytest.mark.c01
class TestCountAndPagination:
    """req/request-data/count — $count=true adds @iot.count.
    req/request-data/top — $top limits results per page.
    req/request-data/pagination — @iot.nextLink is present when more results
    exist; following it yields the next page without overlap/gaps.

    All assertions scoped to seed datastreams (DB is not empty — never assert
    whole-collection counts).

    FROST parity: Capability1Tests (pagination).
    """

    def test_count_true_adds_iot_count_ds1(self, client, seed):
        """req/request-data/count — $count=true → @iot.count present (DS1 obs)."""
        resp = client.get(
            f"Datastreams({format_id(seed.ds1.id)})/Observations",
            params={"$count": "true"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "@iot.count" in data, "$count=true must add @iot.count"

    def test_count_equals_ds1_observation_count(self, client, seed):
        """req/request-data/count — @iot.count matches DS1 observation count (2)."""
        resp = client.get(
            f"Datastreams({format_id(seed.ds1.id)})/Observations",
            params={"$count": "true"},
        )
        data = resp.json()
        assert data["@iot.count"] == len(seed.ds1.observation_ids), (
            f"@iot.count={data['@iot.count']} != expected {len(seed.ds1.observation_ids)}"
        )

    def test_count_equals_ds2_observation_count(self, client, seed):
        """req/request-data/count — @iot.count matches DS2 observation count (2)."""
        resp = client.get(
            f"Datastreams({format_id(seed.ds2.id)})/Observations",
            params={"$count": "true"},
        )
        data = resp.json()
        assert data["@iot.count"] == len(seed.ds2.observation_ids)

    def test_count_false_omits_iot_count(self, client, seed):
        """req/request-data/count — $count=false → no @iot.count."""
        resp = client.get(
            f"Datastreams({format_id(seed.ds1.id)})/Observations",
            params={"$count": "false"},
        )
        data = resp.json()
        assert "@iot.count" not in data, "$count=false must omit @iot.count"

    def test_top_limits_result_count(self, client, seed):
        """req/request-data/top — $top=1 returns at most 1 entity from DS1."""
        resp = client.get(
            f"Datastreams({format_id(seed.ds1.id)})/Observations",
            params={"$top": "1"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["value"]) <= 1, (
            f"$top=1 should return at most 1 entity, got {len(data['value'])}"
        )

    def test_next_link_present_when_results_exceed_top(self, client, seed):
        """req/request-data/pagination — @iot.nextLink present when $top < total.
        DS1 has 2 observations; $top=1 must produce nextLink.
        """
        resp = client.get(
            f"Datastreams({format_id(seed.ds1.id)})/Observations",
            params={"$top": "1"},
        )
        data = resp.json()
        assert "@iot.nextLink" in data, (
            "@iot.nextLink must be present when $top (1) < total (2)"
        )

    def test_next_link_resolves_to_next_page(self, client, seed):
        """req/request-data/pagination — following @iot.nextLink returns further results
        without overlap.
        """
        resp1 = client.get(
            f"Datastreams({format_id(seed.ds1.id)})/Observations",
            params={"$top": "1"},
        )
        data1 = resp1.json()
        assert "@iot.nextLink" in data1
        nxt = data1["@iot.nextLink"]

        resp2 = client.get(nxt)
        assert resp2.status_code == 200, (
            f"Following @iot.nextLink returned {resp2.status_code}"
        )
        data2 = resp2.json()
        assert "value" in data2
        assert len(data2["value"]) > 0, "@iot.nextLink page is empty"

        ids1 = {entity_id(e) for e in data1["value"]}
        ids2 = {entity_id(e) for e in data2["value"]}
        overlap = ids1 & ids2
        assert not overlap, (
            f"Pages overlap — same ids on both pages: {overlap}"
        )

    def test_count_true_with_top_reflects_total_not_page(self, client, seed):
        """req/request-data/count — @iot.count reflects total regardless of $top."""
        resp = client.get(
            f"Datastreams({format_id(seed.ds1.id)})/Observations",
            params={"$count": "true", "$top": "1"},
        )
        data = resp.json()
        assert "@iot.count" in data
        assert data["@iot.count"] == len(seed.ds1.observation_ids), (
            f"@iot.count must be total ({len(seed.ds1.observation_ids)}), "
            f"not page size; got {data['@iot.count']}"
        )

    def test_no_next_link_when_all_results_fit(self, client, seed):
        """req/request-data/pagination — @iot.nextLink absent when $top ≥ total."""
        # DS1 has 2 obs; $top=100 fetches all → no nextLink
        resp = client.get(
            f"Datastreams({format_id(seed.ds1.id)})/Observations",
            params={"$top": "100"},
        )
        data = resp.json()
        assert "@iot.nextLink" not in data, (
            "@iot.nextLink must be absent when all results fit on one page"
        )


# ============================================================================
# 14. Error handling
# ============================================================================

@pytest.mark.c01
class TestErrorHandling:
    """req/resource-path — invalid resource paths must return appropriate HTTP
    error codes.

    18-088 §9.2 / OData 4.0 §9.5.1:
      - Non-existent entity id → 404
      - Property that does not exist on the entity → 404
      - Unknown collection / entity-set → 404

    FROST parity: Capability1Tests.readNonexistentEntity()
    """

    @pytest.mark.parametrize("path", [
        "Things(999999999)",
        "Locations(999999999)",
        "Datastreams(999999999)",
        "Observations(999999999)",
        "Sensors(999999999)",
        "ObservedProperties(999999999)",
        "FeaturesOfInterest(999999999)",
        "HistoricalLocations(999999999)",
    ])
    def test_nonexistent_entity_id_returns_404(self, client, path):
        """req/resource-path — GET /<entity>(max_int) → 404 for non-existent id."""
        resp = client.get(path)
        assert resp.status_code == 404, (
            f"Non-existent entity must return 404, got {resp.status_code} for {path}"
        )

    def test_unknown_property_returns_404(self, client, seed):
        """req/resource-path — /Things(<id>)/nosuchprop → 404.

        18-088 §9.2 / OData §9.5.1: a request to a property that does not exist
        on the entity MUST return 404, not 5xx.
        """
        resp = client.get(f"Things({format_id(seed.thing_id)})/nosuchprop")
        assert resp.status_code == 404, (
            f"Non-existent property must return 404, got {resp.status_code}"
        )

    def test_unknown_collection_returns_404(self, client):
        """req/resource-path — /NonExistentCollection → 404.

        An unknown entity-set name in the resource path must produce a 404,
        not a 5xx server error.
        """
        resp = client.get("NonExistentCollection")
        assert resp.status_code == 404, (
            f"Unknown collection must return 404, got {resp.status_code}"
        )

    def test_property_of_nonexistent_entity_returns_404(self, client):
        """req/resource-path — /Things(999999999)/name → 404 (entity not found)."""
        resp = client.get("Things(999999999)/name")
        assert resp.status_code == 404, (
            f"Property access on non-existent entity must return 404, "
            f"got {resp.status_code}"
        )

    def test_dollar_value_of_nonexistent_entity_returns_404(self, client):
        """req/resource-path — /Things(999999999)/name/$value → 404."""
        resp = client.get("Things(999999999)/name/$value")
        assert resp.status_code == 404, (
            f"$value on non-existent entity must return 404, got {resp.status_code}"
        )

    def test_navigation_from_nonexistent_entity_returns_empty_or_404(self, client):
        """req/resource-path — /Things(999999999)/Datastreams — navigation from a
        non-existent parent.

        18-088 §9.2 does not explicitly mandate that navigation from a non-existent
        parent returns 404 (only entity-by-id is explicit about 404). OData 4.0 allows
        an empty collection here. This test accepts either 404 OR 200 with an empty
        collection ({"value": []}).
        """
        resp = client.get("Things(999999999)/Datastreams")
        if resp.status_code == 200:
            data = resp.json()
            # If 200, the body must be an empty collection (not real data)
            assert "value" in data
            assert data["value"] == [], (
                "Navigation from non-existent parent: 200 response must be empty collection"
            )
        else:
            assert resp.status_code == 404, (
                f"Navigation from non-existent parent: expected 200 (empty) or 404, "
                f"got {resp.status_code}"
            )


# ============================================================================
# 15. Bidirectional navigability (round-trip cross-checks)
# ============================================================================

@pytest.mark.c01
class TestBidirectionalNavigation:
    """req/resource-path/resource-path-to-entities — all relations defined in
    18-088 §8 must be navigable in BOTH directions. Round-trip: start at A,
    navigate to B, navigate back from B to A; verify identity.
    """

    def test_thing_ds1_roundtrip(self, client, seed):
        """Thing→DS1→Thing identity."""
        thing_via_ds = client.nav(
            f"Datastreams({format_id(seed.ds1.id)})/Thing"
        )
        assert thing_via_ds["@iot.id"] == seed.thing_id
        ds_ids = [
            entity_id(e) for e in client.values(
                f"Things({format_id(seed.thing_id)})/Datastreams"
            )
        ]
        assert seed.ds1.id in ds_ids

    def test_thing_ds2_roundtrip(self, client, seed):
        """Thing→DS2→Thing identity (second Datastream)."""
        thing_via_ds = client.nav(
            f"Datastreams({format_id(seed.ds2.id)})/Thing"
        )
        assert thing_via_ds["@iot.id"] == seed.thing_id
        ds_ids = [
            entity_id(e) for e in client.values(
                f"Things({format_id(seed.thing_id)})/Datastreams"
            )
        ]
        assert seed.ds2.id in ds_ids

    def test_ds1_sensor_roundtrip(self, client, seed):
        """DS1→Sensor→Datastreams includes DS1."""
        sensor = client.nav(f"Datastreams({format_id(seed.ds1.id)})/Sensor")
        assert sensor["@iot.id"] == seed.ds1.sensor_id
        ds_ids = [
            entity_id(e) for e in client.values(
                f"Sensors({format_id(seed.ds1.sensor_id)})/Datastreams"
            )
        ]
        assert seed.ds1.id in ds_ids

    def test_ds1_observed_property_roundtrip(self, client, seed):
        """DS1→ObservedProperty→Datastreams includes DS1."""
        op = client.nav(
            f"Datastreams({format_id(seed.ds1.id)})/ObservedProperty"
        )
        assert op["@iot.id"] == seed.ds1.observed_property_id
        ds_ids = [
            entity_id(e) for e in client.values(
                f"ObservedProperties({format_id(seed.ds1.observed_property_id)})"
                f"/Datastreams"
            )
        ]
        assert seed.ds1.id in ds_ids

    def test_ds1_observation_roundtrip(self, client, seed, obs_id):
        """DS1→Observations→Datastream→DS1 identity."""
        ds = client.nav(f"Observations({format_id(obs_id)})/Datastream")
        assert ds["@iot.id"] == seed.ds1.id
        obs_ids = [
            entity_id(e) for e in client.values(
                f"Datastreams({format_id(seed.ds1.id)})/Observations"
            )
        ]
        assert obs_id in obs_ids

    def test_observation_foi_roundtrip(self, client, seed, obs_id, foi_id):
        """Observation→FOI→Observations includes seed observation."""
        foi = client.nav(f"Observations({format_id(obs_id)})/FeatureOfInterest")
        linked_foi_id = foi["@iot.id"]
        assert linked_foi_id in seed.foi_ids
        obs_ids = [
            entity_id(e) for e in client.values(
                f"FeaturesOfInterest({format_id(linked_foi_id)})/Observations"
            )
        ]
        assert obs_id in obs_ids

    def test_thing_location_roundtrip(self, client, seed):
        """Thing→Locations→Things round-trip."""
        loc_ids = [
            entity_id(e) for e in client.values(
                f"Things({format_id(seed.thing_id)})/Locations"
            )
        ]
        assert seed.location_id in loc_ids
        thing_ids = [
            entity_id(e) for e in client.values(
                f"Locations({format_id(seed.location_id)})/Things"
            )
        ]
        assert seed.thing_id in thing_ids

    def test_thing_historical_location_roundtrip(self, client, seed, hl_id):
        """Thing→HistoricalLocations→Thing round-trip."""
        hl_ids = [
            entity_id(e) for e in client.values(
                f"Things({format_id(seed.thing_id)})/HistoricalLocations"
            )
        ]
        assert hl_id in hl_ids
        thing_from_hl = client.nav(
            f"HistoricalLocations({format_id(hl_id)})/Thing"
        )
        assert thing_from_hl["@iot.id"] == seed.thing_id

    def test_location_historical_location_roundtrip(self, client, seed, hl_id):
        """Location→HistoricalLocations→Locations round-trip."""
        hl_ids = [
            entity_id(e) for e in client.values(
                f"Locations({format_id(seed.location_id)})/HistoricalLocations"
            )
        ]
        assert hl_id in hl_ids
        loc_ids = [
            entity_id(e) for e in client.values(
                f"HistoricalLocations({format_id(hl_id)})/Locations"
            )
        ]
        assert seed.location_id in loc_ids

    def test_ds2_sensor_roundtrip(self, client, seed):
        """DS2→Sensor→Datastreams includes DS2."""
        sensor = client.nav(f"Datastreams({format_id(seed.ds2.id)})/Sensor")
        assert sensor["@iot.id"] == seed.ds2.sensor_id
        ds_ids = [
            entity_id(e) for e in client.values(
                f"Sensors({format_id(seed.ds2.sensor_id)})/Datastreams"
            )
        ]
        assert seed.ds2.id in ds_ids

    def test_ds2_observed_property_roundtrip(self, client, seed):
        """DS2→ObservedProperty→Datastreams includes DS2."""
        op = client.nav(
            f"Datastreams({format_id(seed.ds2.id)})/ObservedProperty"
        )
        assert op["@iot.id"] == seed.ds2.observed_property_id
        ds_ids = [
            entity_id(e) for e in client.values(
                f"ObservedProperties({format_id(seed.ds2.observed_property_id)})"
                f"/Datastreams"
            )
        ]
        assert seed.ds2.id in ds_ids


# ============================================================================
# 16. Object-type $value (FROST parity: checkGetPropertyValueOfEntity)
# ============================================================================

@pytest.mark.c01
class TestDollarValueObjectProperties:
    """req/resource-path/resource-path-to-entities (Usage 5) — for properties
    whose JSON type is 'object' (location, feature, unitOfMeasurement, properties),
    $value MUST return a JSON object (body starts with '{').

    For scalar/string properties, $value MUST NOT contain '{'.

    FROST parity: Capability1Tests.checkGetPropertyValueOfEntity() — the FROST
    test distinguishes object vs non-object jsonType and checks accordingly.
    (EntityType.java: location=object, feature=object, unitOfMeasurement=object,
    properties=object; name/description/encodingType/observationType=string.)
    """

    def test_location_location_dollar_value_is_json_object(self, client, seed):
        """req/resource-path — Location/location/$value → JSON object (starts with '{')."""
        resp = client.get(f"Locations({format_id(seed.location_id)})/location/$value")
        assert resp.status_code == 200, (
            f"Expected 200, got {resp.status_code}"
        )
        body = resp.text.strip()
        assert body.startswith("{"), (
            f"Location/location/$value must return a JSON object (starts with '{{'), "
            f"got: {body[:80]!r}"
        )

    def test_ds1_unit_of_measurement_dollar_value_is_json_object(self, client, seed):
        """req/resource-path — Datastreams/unitOfMeasurement/$value → JSON object."""
        resp = client.get(
            f"Datastreams({format_id(seed.ds1.id)})/unitOfMeasurement/$value"
        )
        assert resp.status_code == 200, (
            f"Expected 200, got {resp.status_code}"
        )
        body = resp.text.strip()
        assert body.startswith("{"), (
            f"unitOfMeasurement/$value must be a JSON object, got: {body[:80]!r}"
        )

    def test_ds2_unit_of_measurement_dollar_value_is_json_object(self, client, seed):
        """req/resource-path — DS2/unitOfMeasurement/$value → JSON object."""
        resp = client.get(
            f"Datastreams({format_id(seed.ds2.id)})/unitOfMeasurement/$value"
        )
        assert resp.status_code == 200
        body = resp.text.strip()
        assert body.startswith("{"), (
            f"DS2 unitOfMeasurement/$value must be a JSON object, got: {body[:80]!r}"
        )

    def test_foi_feature_dollar_value_is_json_object(self, client, foi_id):
        """req/resource-path — FeaturesOfInterest/feature/$value → JSON object."""
        resp = client.get(
            f"FeaturesOfInterest({format_id(foi_id)})/feature/$value"
        )
        assert resp.status_code == 200, (
            f"Expected 200, got {resp.status_code}"
        )
        body = resp.text.strip()
        assert body.startswith("{"), (
            f"feature/$value must be a JSON object, got: {body[:80]!r}"
        )

    def test_thing_properties_dollar_value_is_json_object_or_absent(self, client, seed):
        """req/resource-path — Thing/properties/$value → JSON object (if present).

        'properties' is optional; if absent the response is 204 (or 404 from some
        servers). If present it must be a JSON object.
        """
        resp = client.get(f"Things({format_id(seed.thing_id)})/properties/$value")
        if resp.status_code in (204, 404):
            return  # null/absent property — acceptable per spec (204) or server choice
        assert resp.status_code == 200, (
            f"Unexpected status {resp.status_code} for properties/$value"
        )
        body = resp.text.strip()
        assert body.startswith("{"), (
            f"Thing/properties/$value must be a JSON object, got: {body[:80]!r}"
        )

    def test_string_property_dollar_value_does_not_contain_braces(self, client, seed):
        """req/resource-path — $value for string properties must NOT be a JSON object.

        FROST parity: checkGetPropertyValueOfEntity checks that non-object properties
        have indexOf('{') == -1 in the response body.
        """
        # name is a string property for all entity types
        resp = client.get(f"Things({format_id(seed.thing_id)})/name/$value")
        assert resp.status_code == 200
        assert "{" not in resp.text, (
            f"String $value must not contain '{{', got: {resp.text!r}"
        )

    def test_observation_result_dollar_value_is_scalar(self, client, seed, obs_id):
        """req/resource-path — Observation/result/$value for numeric result → no '{}'.

        FROST parity: result has jsonType='any'; for numeric results the body must
        NOT start with '{' (it is a plain number).
        """
        resp = client.get(f"Observations({format_id(obs_id)})/result/$value")
        assert resp.status_code == 200
        body = resp.text.strip()
        # Seed result is integer (3 or 4) — plain numeric text
        assert "{" not in body, (
            f"Numeric result/$value must not be a JSON object, got: {body!r}"
        )
        # Confirm it's parseable as a number
        try:
            float(body)
        except ValueError:
            raise AssertionError(
                f"result/$value for numeric result must be parseable as a number, got: {body!r}"
            )

    def test_ds1_observed_area_dollar_value_is_json_object_or_absent(self, client, seed):
        """req/resource-path — Datastreams/observedArea/$value — optional property.

        'observedArea' is computed; it may be null if no observations have
        geometries. Accept 200 (JSON object), 204 (null per spec), or 404
        (server-specific null handling). If 200, body must start with '{'.
        """
        resp = client.get(
            f"Datastreams({format_id(seed.ds1.id)})/observedArea/$value"
        )
        if resp.status_code in (204, 404):
            return  # null/absent — acceptable
        assert resp.status_code == 200
        assert resp.text.strip().startswith("{"), (
            f"observedArea/$value (when present) must be JSON object, got: {resp.text[:60]!r}"
        )


# ============================================================================
# 17. Property response contains exactly one key (FROST parity)
# ============================================================================

@pytest.mark.c01
class TestPropertyResponseSize:
    """req/resource-path/resource-path-to-entities — when accessing a named
    property of an entity, the response JSON object MUST contain exactly ONE
    key (the property name).  No other entity properties or control info
    annotations should leak into the response.

    FROST parity: Capability1Tests.checkGetPropertyOfEntity() asserts
    `assertEquals(1, entity.size(), message)`.
    """

    @pytest.mark.parametrize("entity,id_attr,prop", [
        ("Things",            "thing_id",   "name"),
        ("Things",            "thing_id",   "description"),
        ("Locations",         "location_id","name"),
        ("Locations",         "location_id","encodingType"),
        ("Locations",         "location_id","location"),
    ])
    def test_property_response_has_exactly_one_key_simple(
        self, client, seed, entity, id_attr, prop
    ):
        """req/resource-path — GET /<entity>(<id>)/<prop> response has exactly 1 key."""
        eid = getattr(seed, id_attr)
        resp = client.get(f"{entity}({format_id(eid)})/{prop}")
        if resp.status_code == 204:
            return  # null property — no JSON body
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1, (
            f"Property response for {entity}/{prop} must have exactly 1 key, "
            f"got {len(data)}: {list(data.keys())}"
        )
        assert prop in data, (
            f"Property response must contain key '{prop}', got: {list(data.keys())}"
        )

    def test_datastream_name_response_has_one_key(self, client, seed):
        """req/resource-path — Datastreams/<id>/name response has exactly 1 key."""
        resp = client.get(f"Datastreams({format_id(seed.ds1.id)})/name")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1 and "name" in data

    def test_datastream_unit_of_measurement_response_has_one_key(self, client, seed):
        """req/resource-path — Datastreams/<id>/unitOfMeasurement has exactly 1 key."""
        resp = client.get(
            f"Datastreams({format_id(seed.ds1.id)})/unitOfMeasurement"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1 and "unitOfMeasurement" in data

    def test_sensor_metadata_response_has_one_key(self, client, seed):
        """req/resource-path — Sensors/<id>/metadata response has exactly 1 key."""
        resp = client.get(f"Sensors({format_id(seed.ds1.sensor_id)})/metadata")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1 and "metadata" in data

    def test_observed_property_definition_response_has_one_key(self, client, seed):
        """req/resource-path — ObservedProperties/<id>/definition has exactly 1 key."""
        resp = client.get(
            f"ObservedProperties({format_id(seed.ds1.observed_property_id)})/definition"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1 and "definition" in data

    def test_observation_result_response_has_one_key(self, client, obs_id):
        """req/resource-path — Observations/<id>/result response has exactly 1 key."""
        resp = client.get(f"Observations({format_id(obs_id)})/result")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1 and "result" in data

    def test_observation_phenomenon_time_response_has_one_key(self, client, obs_id):
        """req/resource-path — Observations/<id>/phenomenonTime has exactly 1 key."""
        resp = client.get(f"Observations({format_id(obs_id)})/phenomenonTime")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1 and "phenomenonTime" in data

    def test_foi_feature_response_has_one_key(self, client, foi_id):
        """req/resource-path — FeaturesOfInterest/<id>/feature has exactly 1 key."""
        resp = client.get(f"FeaturesOfInterest({format_id(foi_id)})/feature")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1 and "feature" in data

    def test_historical_location_time_response_has_one_key(self, client, hl_id):
        """req/resource-path — HistoricalLocations/<id>/time has exactly 1 key."""
        resp = client.get(f"HistoricalLocations({format_id(hl_id)})/time")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1 and "time" in data


# ============================================================================
# 18. Datamodel entity relations — bidirectional navigability (per declared URI)
# ============================================================================

@pytest.mark.c01
class TestDatamodelEntityRelations:
    """Explicit coverage for the 8 declared req/datamodel/<entity>/relations URIs.

    18-088 §8 defines navigable relations for each entity.  For each entity we
    verify that EVERY outgoing navigation link can be followed and that the
    target entity also exposes the reverse link back to the source.  Tests are
    scoped to the seed ids so they are concurrency-safe and database-agnostic.

    Note: the exhaustive one-direction tests live in TestNavigationOneToMany /
    TestNavigationManyToOne; the round-trips in TestBidirectionalNavigation.
    This class collects ALL relations for each entity under one docstring
    containing the canonical req-id so the COVERAGE_MATRIX can be populated.
    """

    # ---- Thing (req/datamodel/thing/relations) ----
    def test_thing_relations(self, client, seed, hl_id):
        """req/datamodel/thing/relations — Thing's Locations, HistoricalLocations,
        and Datastreams relations are navigable in both directions.

        Forward:  Thing → Locations, HistoricalLocations, Datastreams
        Reverse:  Location → Things, HistoricalLocation → Thing,
                  Datastream → Thing
        """
        tid = seed.thing_id

        # Thing → Locations  (forward)
        loc_ids = [entity_id(e) for e in client.values(
            f"Things({format_id(tid)})/Locations"
        )]
        assert seed.location_id in loc_ids, (
            "Thing/Locations must include seed Location"
        )

        # Location → Things  (reverse)
        thing_ids_via_loc = [entity_id(e) for e in client.values(
            f"Locations({format_id(seed.location_id)})/Things"
        )]
        assert tid in thing_ids_via_loc, (
            "Location/Things must include seed Thing (reverse of Thing→Locations)"
        )

        # Thing → HistoricalLocations  (forward)
        hl_ids = [entity_id(e) for e in client.values(
            f"Things({format_id(tid)})/HistoricalLocations"
        )]
        assert hl_id in hl_ids, "Thing/HistoricalLocations must include seed HL"

        # HistoricalLocation → Thing  (reverse)
        thing_via_hl = client.nav(f"HistoricalLocations({format_id(hl_id)})/Thing")
        assert thing_via_hl["@iot.id"] == tid, (
            "HistoricalLocation/Thing must equal seed Thing (reverse of Thing→HistoricalLocations)"
        )

        # Thing → Datastreams  (forward)
        ds_ids = [entity_id(e) for e in client.values(
            f"Things({format_id(tid)})/Datastreams"
        )]
        for dsid in seed.datastream_ids:
            assert dsid in ds_ids, f"Thing/Datastreams missing DS {dsid}"

        # Datastream → Thing  (reverse)
        thing_via_ds = client.nav(f"Datastreams({format_id(seed.ds1.id)})/Thing")
        assert thing_via_ds["@iot.id"] == tid, (
            "Datastream/Thing must equal seed Thing (reverse of Thing→Datastreams)"
        )

    # ---- Location (req/datamodel/location/relations) ----
    def test_location_relations(self, client, seed, hl_id):
        """req/datamodel/location/relations — Location's Things and HistoricalLocations
        relations are navigable in both directions.

        Forward:  Location → Things, HistoricalLocations
        Reverse:  Thing → Locations, HistoricalLocation → Locations
        """
        lid = seed.location_id
        tid = seed.thing_id

        # Location → Things  (forward)
        thing_ids = [entity_id(e) for e in client.values(
            f"Locations({format_id(lid)})/Things"
        )]
        assert tid in thing_ids

        # Thing → Locations  (reverse)
        loc_ids = [entity_id(e) for e in client.values(
            f"Things({format_id(tid)})/Locations"
        )]
        assert lid in loc_ids

        # Location → HistoricalLocations  (forward)
        hl_ids_from_loc = [entity_id(e) for e in client.values(
            f"Locations({format_id(lid)})/HistoricalLocations"
        )]
        assert hl_id in hl_ids_from_loc

        # HistoricalLocation → Locations  (reverse)
        loc_ids_from_hl = [entity_id(e) for e in client.values(
            f"HistoricalLocations({format_id(hl_id)})/Locations"
        )]
        assert lid in loc_ids_from_hl

    # ---- HistoricalLocation (req/datamodel/historical-location/relations) ----
    def test_historical_location_relations(self, client, seed, hl_id):
        """req/datamodel/historical-location/relations — HistoricalLocation's Thing
        and Locations relations are navigable in both directions.

        Forward:  HistoricalLocation → Thing, Locations
        Reverse:  Thing → HistoricalLocations, Location → HistoricalLocations
        """
        # HistoricalLocation → Thing  (forward)
        thing = client.nav(f"HistoricalLocations({format_id(hl_id)})/Thing")
        assert thing["@iot.id"] == seed.thing_id

        # Thing → HistoricalLocations  (reverse)
        hl_ids = [entity_id(e) for e in client.values(
            f"Things({format_id(seed.thing_id)})/HistoricalLocations"
        )]
        assert hl_id in hl_ids

        # HistoricalLocation → Locations  (forward)
        loc_ids = [entity_id(e) for e in client.values(
            f"HistoricalLocations({format_id(hl_id)})/Locations"
        )]
        assert seed.location_id in loc_ids

        # Location → HistoricalLocations  (reverse)
        hl_ids_from_loc = [entity_id(e) for e in client.values(
            f"Locations({format_id(seed.location_id)})/HistoricalLocations"
        )]
        assert hl_id in hl_ids_from_loc

    # ---- Datastream (req/datamodel/datastream/relations) ----
    def test_datastream_relations(self, client, seed, obs_id):
        """req/datamodel/datastream/relations — Datastream's Thing, Sensor,
        ObservedProperty, and Observations relations are navigable in both
        directions.

        Forward:  Datastream → Thing, Sensor, ObservedProperty, Observations
        Reverse:  Thing → Datastreams, Sensor → Datastreams,
                  ObservedProperty → Datastreams, Observation → Datastream
        """
        ds1_id = seed.ds1.id

        # Datastream → Thing  (forward)
        thing = client.nav(f"Datastreams({format_id(ds1_id)})/Thing")
        assert thing["@iot.id"] == seed.thing_id

        # Thing → Datastreams  (reverse)
        ds_ids = [entity_id(e) for e in client.values(
            f"Things({format_id(seed.thing_id)})/Datastreams"
        )]
        assert ds1_id in ds_ids

        # Datastream → Sensor  (forward)
        sensor = client.nav(f"Datastreams({format_id(ds1_id)})/Sensor")
        assert sensor["@iot.id"] == seed.ds1.sensor_id

        # Sensor → Datastreams  (reverse)
        ds_from_sensor = [entity_id(e) for e in client.values(
            f"Sensors({format_id(seed.ds1.sensor_id)})/Datastreams"
        )]
        assert ds1_id in ds_from_sensor

        # Datastream → ObservedProperty  (forward)
        op = client.nav(f"Datastreams({format_id(ds1_id)})/ObservedProperty")
        assert op["@iot.id"] == seed.ds1.observed_property_id

        # ObservedProperty → Datastreams  (reverse)
        ds_from_op = [entity_id(e) for e in client.values(
            f"ObservedProperties({format_id(seed.ds1.observed_property_id)})/Datastreams"
        )]
        assert ds1_id in ds_from_op

        # Datastream → Observations  (forward)
        obs_ids = [entity_id(e) for e in client.values(
            f"Datastreams({format_id(ds1_id)})/Observations"
        )]
        for oid in seed.ds1.observation_ids:
            assert oid in obs_ids

        # Observation → Datastream  (reverse)
        ds_from_obs = client.nav(f"Observations({format_id(obs_id)})/Datastream")
        assert ds_from_obs["@iot.id"] == ds1_id

    # ---- Sensor (req/datamodel/sensor/relations) ----
    def test_sensor_relations(self, client, seed):
        """req/datamodel/sensor/relations — Sensor's Datastreams relation is
        navigable in both directions.

        Forward:  Sensor → Datastreams
        Reverse:  Datastream → Sensor
        """
        s1_id = seed.ds1.sensor_id
        ds1_id = seed.ds1.id

        # Sensor → Datastreams  (forward)
        ds_ids = [entity_id(e) for e in client.values(
            f"Sensors({format_id(s1_id)})/Datastreams"
        )]
        assert ds1_id in ds_ids

        # Datastream → Sensor  (reverse)
        sensor = client.nav(f"Datastreams({format_id(ds1_id)})/Sensor")
        assert sensor["@iot.id"] == s1_id

        # Verify second sensor/datastream pair as well
        s2_id = seed.ds2.sensor_id
        ds2_id = seed.ds2.id
        ds_ids2 = [entity_id(e) for e in client.values(
            f"Sensors({format_id(s2_id)})/Datastreams"
        )]
        assert ds2_id in ds_ids2
        sensor2 = client.nav(f"Datastreams({format_id(ds2_id)})/Sensor")
        assert sensor2["@iot.id"] == s2_id

    # ---- ObservedProperty (req/datamodel/observed-property/relations) ----
    def test_observed_property_relations(self, client, seed):
        """req/datamodel/observed-property/relations — ObservedProperty's Datastreams
        relation is navigable in both directions.

        Forward:  ObservedProperty → Datastreams
        Reverse:  Datastream → ObservedProperty
        """
        op1_id = seed.ds1.observed_property_id
        ds1_id = seed.ds1.id

        # ObservedProperty → Datastreams  (forward)
        ds_ids = [entity_id(e) for e in client.values(
            f"ObservedProperties({format_id(op1_id)})/Datastreams"
        )]
        assert ds1_id in ds_ids

        # Datastream → ObservedProperty  (reverse)
        op = client.nav(f"Datastreams({format_id(ds1_id)})/ObservedProperty")
        assert op["@iot.id"] == op1_id

        # Also check DS2/OP2
        op2_id = seed.ds2.observed_property_id
        ds2_id = seed.ds2.id
        ds_ids2 = [entity_id(e) for e in client.values(
            f"ObservedProperties({format_id(op2_id)})/Datastreams"
        )]
        assert ds2_id in ds_ids2
        op2 = client.nav(f"Datastreams({format_id(ds2_id)})/ObservedProperty")
        assert op2["@iot.id"] == op2_id

    # ---- Observation (req/datamodel/observation/relations) ----
    def test_observation_relations(self, client, seed, obs_id, foi_id):
        """req/datamodel/observation/relations — Observation's Datastream and
        FeatureOfInterest relations are navigable in both directions.

        Forward:  Observation → Datastream, FeatureOfInterest
        Reverse:  Datastream → Observations, FeatureOfInterest → Observations
        """
        # Observation → Datastream  (forward)
        ds = client.nav(f"Observations({format_id(obs_id)})/Datastream")
        assert ds["@iot.id"] == seed.ds1.id

        # Datastream → Observations  (reverse)
        obs_ids = [entity_id(e) for e in client.values(
            f"Datastreams({format_id(seed.ds1.id)})/Observations"
        )]
        assert obs_id in obs_ids

        # Observation → FeatureOfInterest  (forward)
        foi = client.nav(f"Observations({format_id(obs_id)})/FeatureOfInterest")
        linked_foi_id = foi["@iot.id"]
        assert linked_foi_id in seed.foi_ids

        # FeatureOfInterest → Observations  (reverse)
        obs_ids_from_foi = [entity_id(e) for e in client.values(
            f"FeaturesOfInterest({format_id(linked_foi_id)})/Observations"
        )]
        assert obs_id in obs_ids_from_foi

    # ---- FeatureOfInterest (req/datamodel/feature-of-interest/relations) ----
    def test_foi_relations(self, client, seed, obs_id, foi_id):
        """req/datamodel/feature-of-interest/relations — FeatureOfInterest's
        Observations relation is navigable in both directions.

        Forward:  FeatureOfInterest → Observations
        Reverse:  Observation → FeatureOfInterest
        """
        # FeatureOfInterest → Observations  (forward)
        obs_ids = [entity_id(e) for e in client.values(
            f"FeaturesOfInterest({format_id(foi_id)})/Observations"
        )]
        # At least one seed observation must be linked to this FOI
        matched = [oid for oid in seed.all_observation_ids if oid in obs_ids]
        assert matched, (
            f"FeaturesOfInterest/{foi_id}/Observations must include at least "
            "one seed observation"
        )

        # Observation → FeatureOfInterest  (reverse)
        # Navigate from first matched seed obs back to a FOI
        foi_via_obs = client.nav(
            f"Observations({format_id(obs_id)})/FeatureOfInterest"
        )
        assert foi_via_obs["@iot.id"] in seed.foi_ids, (
            "Observation/FeatureOfInterest must reference a seed FOI "
            "(reverse of FeatureOfInterest→Observations)"
        )


# ============================================================================
# 19. req/request-data/status-code — valid requests return HTTP 200
# ============================================================================

@pytest.mark.c01
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


# ============================================================================
# 20. req/request-data/query-status-code — malformed queries return 400
# ============================================================================

@pytest.mark.c01
class TestQueryStatusCode:
    """req/request-data/query-status-code — a request with an invalid system
    query option MUST result in a 4xx response (18-088 §9.3.1).

    istSOS4 returns HTTP 400 with a structured JSON error body containing 'code',
    'type', and 'message' keys.  This class verifies all declared malformed-query
    cases return 400 (not 500) with a parseable error body.

    The server was verified live to return 400 for all cases below:
      - $filter=name eq (incomplete expression, syntax error)
      - $orderby=nosuchprop asc (unknown property)
      - $top=-5 (negative integer)
      - $skip=-1 (negative integer)
      - $filter=bogus(name) (unknown function)
    """

    def _assert_400_with_error_body(self, resp, case_label: str) -> None:
        """Assert status 400 and structured JSON error body."""
        assert resp.status_code == 400, (
            f"req/request-data/query-status-code: {case_label} must return 400, "
            f"got {resp.status_code}; body: {resp.text[:200]}"
        )
        # Must be parseable JSON (not a raw stacktrace)
        try:
            body = resp.json()
        except Exception:
            raise AssertionError(
                f"req/request-data/query-status-code: {case_label} 400 response "
                f"must have a JSON body; got: {resp.text[:200]}"
            )
        # Body must have an error indicator — at minimum one of code/message/error
        has_error_shape = (
            "code" in body
            or "message" in body
            or "error" in body
        )
        assert has_error_shape, (
            f"req/request-data/query-status-code: {case_label} error body must "
            f"contain 'code', 'message', or 'error'; got keys: {list(body.keys())}"
        )

    def test_bad_filter_syntax_returns_400(self, client, seed):
        """req/request-data/query-status-code — $filter with incomplete expression
        returns 400 with a structured error body.

        Malformed: $filter=name eq  (missing RHS operand — syntax error)
        """
        resp = client.get("Things", params={"$filter": "name eq"})
        self._assert_400_with_error_body(resp, "$filter=name eq (syntax error)")

    def test_orderby_unknown_property_returns_400(self, client, seed):
        """req/request-data/query-status-code — $orderby on a nonexistent property
        returns 400 with a structured error body.

        Malformed: $orderby=nosuchprop asc
        """
        resp = client.get("Things", params={"$orderby": "nosuchprop asc"})
        self._assert_400_with_error_body(
            resp, "$orderby=nosuchprop asc (unknown property)"
        )

    def test_negative_top_returns_400(self, client, seed):
        """req/request-data/query-status-code — negative $top returns 400 with a
        structured error body.

        Malformed: $top=-5
        """
        resp = client.get("Things", params={"$top": "-5"})
        self._assert_400_with_error_body(resp, "$top=-5 (negative value)")

    def test_negative_skip_returns_400(self, client, seed):
        """req/request-data/query-status-code — negative $skip returns 400 with a
        structured error body.

        Malformed: $skip=-1
        """
        resp = client.get("Things", params={"$skip": "-1"})
        self._assert_400_with_error_body(resp, "$skip=-1 (negative value)")

    def test_filter_unknown_function_returns_400(self, client, seed):
        """req/request-data/query-status-code — $filter with an unknown function
        returns 400 with a structured error body.

        Malformed: $filter=bogus(name)
        """
        resp = client.get("Things", params={"$filter": "bogus(name)"})
        self._assert_400_with_error_body(
            resp, "$filter=bogus(name) (unknown function)"
        )

    def test_400_body_is_not_stacktrace(self, client, seed):
        """req/request-data/query-status-code — the 400 error body must be a
        structured JSON error, not an HTML/text stacktrace.

        This test is the semantic complement of the per-case 400 checks above:
        it asserts the body starts with '{' (JSON) and not '<' (HTML traceback)
        or a raw Python traceback.
        """
        resp = client.get("Things", params={"$filter": "name eq"})
        assert resp.status_code == 400
        body = resp.text.strip()
        assert body.startswith("{"), (
            f"req/request-data/query-status-code: 400 body must be JSON "
            f"(starts with '{{'), not a stacktrace; got: {body[:80]!r}"
        )
        assert "<html" not in body.lower(), (
            "400 error body must not be an HTML page (stacktrace)"
        )
        assert "Traceback" not in body, (
            "400 error body must not contain a raw Python traceback"
        )
