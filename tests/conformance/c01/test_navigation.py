"""
OGC SensorThings API v1.1 — Sensing Core (c01): navigation tests.

Covers:
  req/resource-path/resource-path-to-entities  §9.2 one-to-many, many-to-one,
                                                     deep paths, round-trips
  req/request-data/count      §9.3 $count=true
  req/request-data/top        §9.3 $top
  req/request-data/pagination §9.3 @iot.nextLink
"""
from __future__ import annotations

import pytest

from client import entity_id, format_id

pytestmark = pytest.mark.c01


# ============================================================================
# 8. One-to-many navigation
# ============================================================================

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
# 13. @iot.count and @iot.nextLink semantics
# ============================================================================

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
# 15. Bidirectional navigability (round-trip cross-checks)
# ============================================================================

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
