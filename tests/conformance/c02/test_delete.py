"""
test_delete.py -- OGC SensorThings API v1.1 c02 DELETE tests.

Standard:  OGC 18-088 §10.4  (https://docs.ogc.org/is/18-088/18-088.html)
Req namespace: req/create-update-delete/

Coverage:
  DELETE 12 – DELETE each entity type → 200/204; subsequent GET → 404
  DELETE 13 – Cascade: Datastream→Observations; Thing→Datastreams+HistoricalLocations
  DELETE 14 – DELETE non-existent entity → 404
"""

from __future__ import annotations

import pytest

import sample_data
from client import entity_id, format_id, id_from_self_link
from c02.conftest import _create_datastream_tree

pytestmark = pytest.mark.c02


# ===========================================================================
# DELETE 12 – DELETE each entity type → 200/204; subsequent GET → 404
#   req/create-update-delete/delete-entity
# ===========================================================================

class TestDelete12EachType:
    """Delete one entity of each type and verify it's gone (404).

    18-088 §10.4 req/create-update-delete/delete-entity:
    'Upon successful completion, the response shall contain a HTTP 200 or 204
    status code.'  A subsequent GET must return 404.
    """

    @pytest.mark.c02
    def test_delete_thing(self, client, unique_name):
        """req/create-update-delete/delete-entity — DELETE Thing."""
        tag = unique_name("del-thing")
        resp = client.create("Things", sample_data.minimal_thing(tag))
        assert resp.status_code == 201
        url = client.location_of(resp)

        del_resp = client.delete(url)
        assert del_resp.status_code in (200, 204), (
            f"DELETE Thing must return 200 or 204, got {del_resp.status_code}"
        )
        get_resp = client.get(url)
        assert get_resp.status_code == 404, (
            f"GET after DELETE must return 404, got {get_resp.status_code}"
        )

    @pytest.mark.c02
    def test_delete_location(self, client, unique_name):
        """req/create-update-delete/delete-entity — DELETE Location."""
        tag = unique_name("del-loc")
        resp = client.create("Locations", sample_data.minimal_location(tag))
        assert resp.status_code == 201
        url = client.location_of(resp)

        del_resp = client.delete(url)
        assert del_resp.status_code in (200, 204)
        assert client.get(url).status_code == 404

    @pytest.mark.c02
    def test_delete_sensor(self, client, unique_name):
        """req/create-update-delete/delete-entity — DELETE Sensor."""
        tag = unique_name("del-sensor")
        resp = client.create("Sensors", sample_data.minimal_sensor(tag))
        assert resp.status_code == 201
        url = client.location_of(resp)

        del_resp = client.delete(url)
        assert del_resp.status_code in (200, 204)
        assert client.get(url).status_code == 404

    @pytest.mark.c02
    def test_delete_observed_property(self, client, unique_name):
        """req/create-update-delete/delete-entity — DELETE ObservedProperty."""
        tag = unique_name("del-op")
        resp = client.create("ObservedProperties", sample_data.minimal_observed_property(tag))
        assert resp.status_code == 201
        url = client.location_of(resp)

        del_resp = client.delete(url)
        assert del_resp.status_code in (200, 204)
        assert client.get(url).status_code == 404

    @pytest.mark.c02
    def test_delete_datastream(self, client, unique_name, cleanup):
        """req/create-update-delete/delete-entity — DELETE Datastream."""
        tag = unique_name("del-ds")
        tree = _create_datastream_tree(client, unique_name, cleanup)
        ds_url = tree["ds_url"]

        del_resp = client.delete(ds_url)
        assert del_resp.status_code in (200, 204), (
            f"DELETE Datastream must return 200 or 204, got {del_resp.status_code}"
        )
        assert client.get(ds_url).status_code == 404

    @pytest.mark.c02
    def test_delete_observation(self, client, unique_name, cleanup):
        """req/create-update-delete/delete-entity — DELETE Observation."""
        tag = unique_name("del-obs")
        tree = _create_datastream_tree(client, unique_name, cleanup)
        ds_id = tree["ds_id"]

        obs_resp = client.create(
            "Observations",
            sample_data.minimal_observation(tag, ds_id),
        )
        assert obs_resp.status_code == 201
        obs_url = client.location_of(obs_resp)
        obs_id = id_from_self_link(obs_url)

        # Track auto-FoI
        foi_resp = client.get(
            f"{client.base_url}/Observations({format_id(obs_id)})/FeatureOfInterest"
        )
        if foi_resp.status_code == 200:
            cleanup(
                f"{client.base_url}/FeaturesOfInterest({format_id(entity_id(foi_resp.json()))})"
            )

        del_resp = client.delete(obs_url)
        assert del_resp.status_code in (200, 204), (
            f"DELETE Observation must return 200 or 204, got {del_resp.status_code}"
        )
        assert client.get(obs_url).status_code == 404

    @pytest.mark.c02
    def test_delete_feature_of_interest(self, client, unique_name):
        """req/create-update-delete/delete-entity — DELETE FeatureOfInterest."""
        tag = unique_name("del-foi")
        resp = client.create("FeaturesOfInterest", sample_data.minimal_feature_of_interest(tag))
        assert resp.status_code == 201
        url = client.location_of(resp)

        del_resp = client.delete(url)
        assert del_resp.status_code in (200, 204)
        assert client.get(url).status_code == 404

    @pytest.mark.c02
    def test_delete_historical_location(self, client, unique_name, cleanup):
        """req/create-update-delete/delete-entity — DELETE HistoricalLocation."""
        tag = unique_name("del-hl")
        thing_resp = client.create("Things", sample_data.minimal_thing(tag))
        assert thing_resp.status_code == 201
        thing_url = client.location_of(thing_resp)
        t_id = id_from_self_link(thing_url)
        cleanup(thing_url)

        hl_resp = client.create(
            "HistoricalLocations",
            sample_data.minimal_historical_location(tag, t_id),
        )
        assert hl_resp.status_code == 201
        hl_url = client.location_of(hl_resp)

        del_resp = client.delete(hl_url)
        assert del_resp.status_code in (200, 204), (
            f"DELETE HistoricalLocation must return 200 or 204, got {del_resp.status_code}"
        )
        assert client.get(hl_url).status_code == 404


# ===========================================================================
# DELETE 13 – Cascade delete
#   18-088 §10.4
# ===========================================================================

@pytest.mark.c02
def test_cascade_delete_datastream_removes_observations(client, unique_name, cleanup):
    """18-088 §10.4 — Deleting a Datastream must cascade to its Observations.

    After DELETE /Datastreams(<id>), all Observations in that Datastream
    must return 404.
    """
    tag = unique_name("casc-ds")
    tree = _create_datastream_tree(client, unique_name, cleanup)
    ds_id = tree["ds_id"]
    ds_url = tree["ds_url"]

    # Create two Observations
    obs_ids = []
    foi_ids = []
    for i, pt in enumerate(["2023-01-10T00:00:00Z", "2023-01-11T00:00:00Z"]):
        obs_resp = client.create(
            "Observations",
            {"phenomenonTime": pt, "result": float(i), "Datastream": {"@iot.id": ds_id}},
        )
        assert obs_resp.status_code == 201
        obs_url = client.location_of(obs_resp)
        obs_id = id_from_self_link(obs_url)
        obs_ids.append(obs_id)

        foi_r = client.get(
            f"{client.base_url}/Observations({format_id(obs_id)})/FeatureOfInterest"
        )
        if foi_r.status_code == 200:
            foi_ids.append(entity_id(foi_r.json()))

    # Delete the Datastream
    del_resp = client.delete(ds_url)
    assert del_resp.status_code in (200, 204), (
        f"DELETE Datastream must return 200 or 204, got {del_resp.status_code}"
    )

    # Each Observation must now be 404
    for obs_id in obs_ids:
        obs_r = client.get(f"{client.base_url}/Observations({format_id(obs_id)})")
        assert obs_r.status_code == 404, (
            f"Observation({obs_id}) must return 404 after its Datastream is deleted "
            f"(cascade); got {obs_r.status_code}"
        )

    # Cleanup non-cascade entities
    for foi_id in foi_ids:
        cleanup(f"{client.base_url}/FeaturesOfInterest({format_id(foi_id)})")


@pytest.mark.c02
def test_cascade_delete_thing_removes_datastreams_and_histlocs(client, unique_name, cleanup):
    """18-088 §10.4 — Deleting a Thing must cascade to its Datastreams and HistoricalLocations.

    After DELETE /Things(<id>):
      - All Datastreams belonging to the Thing must return 404.
      - All Observations in those Datastreams must return 404.
      - All HistoricalLocations of the Thing must return 404.
    Locations, Sensors, ObservedProperties are NOT cascade-deleted.
    """
    tag = unique_name("casc-thing")
    tree = _create_datastream_tree(client, unique_name, cleanup)
    thing_url = tree["thing_url"]
    t_id = tree["thing_id"]
    ds_id = tree["ds_id"]
    ds_url = tree["ds_url"]
    loc_id = tree["location_id"]
    s_id = tree["sensor_id"]
    op_id = tree["op_id"]

    # Record existing HistoricalLocations (auto-created when Location is linked)
    hl_data = client.nav(f"Things({format_id(t_id)})/HistoricalLocations")
    hl_ids = [entity_id(h) for h in hl_data.get("value", [])]

    # Create an Observation so we can check it cascades too
    obs_resp = client.create(
        "Observations",
        {
            "phenomenonTime": "2023-04-01T00:00:00Z",
            "result": 77.0,
            "Datastream": {"@iot.id": ds_id},
        },
    )
    assert obs_resp.status_code == 201
    obs_url = client.location_of(obs_resp)
    obs_id = id_from_self_link(obs_url)

    foi_r = client.get(
        f"{client.base_url}/Observations({format_id(obs_id)})/FeatureOfInterest"
    )
    foi_id = None
    if foi_r.status_code == 200:
        foi_id = entity_id(foi_r.json())

    # Delete the Thing
    del_resp = client.delete(thing_url)
    assert del_resp.status_code in (200, 204), (
        f"DELETE Thing must return 200 or 204, got {del_resp.status_code}"
    )

    # Datastream must be gone
    assert client.get(ds_url).status_code == 404, (
        "Datastream must return 404 after its Thing is deleted (cascade)"
    )

    # Observation must be gone
    assert client.get(obs_url).status_code == 404, (
        "Observation must return 404 after its Datastream's Thing is deleted (cascade)"
    )

    # HistoricalLocations must be gone
    for hl_id in hl_ids:
        r = client.get(f"{client.base_url}/HistoricalLocations({format_id(hl_id)})")
        assert r.status_code == 404, (
            f"HistoricalLocation({hl_id}) must return 404 after its Thing is deleted (cascade)"
        )

    # Location, Sensor, ObservedProperty must NOT be cascade-deleted (18-088 §10.4)
    loc_url = f"{client.base_url}/Locations({format_id(loc_id)})"
    s_url = f"{client.base_url}/Sensors({format_id(s_id)})"
    op_url = f"{client.base_url}/ObservedProperties({format_id(op_id)})"
    assert client.get(loc_url).status_code == 200, (
        "Location must survive Thing deletion (18-088 §10.4: not cascade-deleted)"
    )
    assert client.get(s_url).status_code == 200, (
        "Sensor must survive Thing→Datastream deletion (18-088 §10.4: not cascade-deleted)"
    )
    assert client.get(op_url).status_code == 200, (
        "ObservedProperty must survive Thing→Datastream deletion (not cascade-deleted)"
    )

    # Cleanup non-cascade entities (Location, Sensor, ObservedProperty remain)
    if foi_id is not None:
        cleanup(f"{client.base_url}/FeaturesOfInterest({format_id(foi_id)})")
    # thing_url was already deleted; cleanup fixture tolerates the 404


# ===========================================================================
# DELETE 14 – DELETE non-existent entity → 404
#   req/create-update-delete/delete-entity
# ===========================================================================

@pytest.mark.c02
def test_delete_nonexistent_thing(client):
    """req/create-update-delete/delete-entity — DELETE non-existent Thing → 404."""
    resp = client.delete("Things(999999999)")
    assert resp.status_code == 404, (
        f"DELETE on non-existent Thing must return 404, got {resp.status_code}"
    )


@pytest.mark.c02
def test_delete_nonexistent_location(client):
    """req/create-update-delete/delete-entity — DELETE non-existent Location → 404."""
    resp = client.delete("Locations(999999999)")
    assert resp.status_code == 404, (
        f"DELETE on non-existent Location must return 404, got {resp.status_code}"
    )


@pytest.mark.c02
def test_delete_nonexistent_datastream(client):
    """req/create-update-delete/delete-entity — DELETE non-existent Datastream → 404."""
    resp = client.delete("Datastreams(999999999)")
    assert resp.status_code == 404, (
        f"DELETE on non-existent Datastream must return 404, got {resp.status_code}"
    )


@pytest.mark.c02
def test_delete_nonexistent_observation(client):
    """req/create-update-delete/delete-entity — DELETE non-existent Observation → 404."""
    resp = client.delete("Observations(999999999)")
    assert resp.status_code == 404, (
        f"DELETE on non-existent Observation must return 404, got {resp.status_code}"
    )


# ===========================================================================
# DELETE 15 – Set-to-null / unlink (covered by PATCH relation test above)
#
# 18-088 §10.4 does not explicitly mandate a "set-to-null" operation beyond
# PATCH-based re-linking (UPDATE-9). The PATCH relation test (test_patch_relation
# _relink_sensor) covers unlinking/re-linking at the relation level.
# ===========================================================================
