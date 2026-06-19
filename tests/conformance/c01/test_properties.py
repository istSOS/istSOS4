"""
OGC SensorThings API v1.1 — Sensing Core (c01): property access tests.

Covers:
  req/resource-path/resource-path-to-entities  §9.2 Usage 4 /<prop>
                                                         Usage 5 /<prop>/$value
"""
from __future__ import annotations

import pytest

from client import format_id

pytestmark = pytest.mark.c01


# ============================================================================
# 6. Property access  /Entity(<id>)/<propName>
# ============================================================================

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
# 12. Nested property + $value on a related entity
# ============================================================================

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
# 16. Object-type $value (FROST parity: checkGetPropertyValueOfEntity)
# ============================================================================

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
