"""
OGC SensorThings API v1.1 — Sensing Core (c01): $ref association-link tests.

Covers:
  req/resource-path/resource-path-to-entities  §9.2 Usage 7 /<navLink>/$ref
"""
from __future__ import annotations

import pytest

from client import entity_id, format_id

pytestmark = pytest.mark.c01


# ============================================================================
# 11. $ref — association links
# ============================================================================

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
