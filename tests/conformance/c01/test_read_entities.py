"""
OGC SensorThings API v1.1 — Sensing Core (c01): entity read tests.

Covers:
  req/resource-path/resource-path-to-entities   §9.2 collection & entity-by-id
  req/datamodel/entity-control-information/...  §8.1 @iot.id, @iot.selfLink, nav links
  req/datamodel/<entity>/properties             §8   mandatory properties
  req/datamodel/<entity>/relations              §8   bidirectional navigability
"""
from __future__ import annotations

import pytest

import sample_data
from client import entity_id, format_id

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


# ============================================================================
# 2. Each collection GET → 200 with value[]
# ============================================================================

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
# 18. Datamodel entity relations — bidirectional navigability (per declared URI)
# ============================================================================

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
