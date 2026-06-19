"""
test_deep_insert.py -- OGC SensorThings API v1.1 c02 deep-insert + defaulting tests.

Standard:  OGC 18-088 §10.2  (https://docs.ogc.org/is/18-088/18-088.html)
Req namespace: req/create-update-delete/

Coverage:
  CREATE 2  – Deep insert (Thing→Location, Datastream→Sensor+ObservedProperty+Observations)
  CREATE 5  – FeatureOfInterest auto-generation from Thing's Location
  CREATE 6  – phenomenonTime / resultTime defaulting
"""

from __future__ import annotations

import pytest

import sample_data
from client import entity_id, format_id, id_from_self_link
from c02.conftest import _create_datastream_tree

pytestmark = pytest.mark.c02


# ===========================================================================
# CREATE 2 – Deep insert
#   req/create-update-delete/deep-insert
#   req/create-update-delete/deep-insert-status-code
# ===========================================================================

@pytest.mark.c02
def test_deep_insert(client, unique_name, cleanup):
    """req/create-update-delete/deep-insert + deep-insert-status-code.

    POST a Thing with nested Locations and a nested Datastream (containing
    inline Sensor, ObservedProperty, and Observations) in one request.
    The server shall return 201 and the whole tree must be created and linked.
    """
    # deep_insert_tree() now loads entitiesDefault.json verbatim (no tag arg).
    tree_payload = sample_data.deep_insert_tree()

    resp = client.create("Things", tree_payload)

    assert resp.status_code == 201, (
        f"Deep insert should return 201, got {resp.status_code}: {resp.text[:400]}"
    )
    thing_url = client.location_of(resp)
    assert thing_url.startswith("http"), "Location header must be an absolute URL"

    # Navigate the created tree (2 Datastreams per entitiesDefault.json)
    thing = client.nav(
        thing_url,
        params={"$expand": "Locations,Datastreams($expand=Sensor,ObservedProperty,Observations)"},
    )

    # Thing
    assert "@iot.id" in thing
    assert thing["name"] == tree_payload["name"]

    # Locations
    locations = thing.get("Locations", [])
    assert len(locations) >= 1, "Nested Location must be created"
    loc_id = entity_id(locations[0])

    # Datastreams (entitiesDefault has 2)
    datastreams = thing.get("Datastreams", [])
    assert len(datastreams) == 2, "entitiesDefault.json deep insert must create 2 Datastreams"

    sensor_ids = []
    op_ids = []
    for ds in datastreams:
        assert "@iot.id" in ds
        assert "Sensor" in ds, "Sensor must be created via deep insert"
        assert "ObservedProperty" in ds, "ObservedProperty must be created via deep insert"
        sensor_ids.append(entity_id(ds["Sensor"]))
        op_ids.append(entity_id(ds["ObservedProperty"]))
        observations = ds.get("Observations", [])
        assert len(observations) == 2, (
            f"Each Datastream in entitiesDefault.json must have 2 Observations, "
            f"got {len(observations)}"
        )

    # Register non-cascade entities for cleanup; Thing cascade handles DS+Obs+HistLocs
    cleanup(thing_url, f"{client.base_url}/Locations({format_id(loc_id)})")
    for sid in sensor_ids:
        cleanup(f"{client.base_url}/Sensors({format_id(sid)})")
    for oid in op_ids:
        cleanup(f"{client.base_url}/ObservedProperties({format_id(oid)})")

    # FoI auto-generated per Datastream — collect unique ids and clean up
    foi_seen = set()
    for ds in datastreams:
        for obs in ds.get("Observations", []):
            obs_id = entity_id(obs)
            foi_resp = client.get(
                f"{client.base_url}/Observations({format_id(obs_id)})/FeatureOfInterest"
            )
            if foi_resp.status_code == 200:
                foi_id = entity_id(foi_resp.json())
                if foi_id not in foi_seen:
                    foi_seen.add(foi_id)
                    cleanup(f"{client.base_url}/FeaturesOfInterest({format_id(foi_id)})")


# ===========================================================================
# CREATE 5 – FeatureOfInterest auto-generation from Thing's Location
#   18-088 §10.2.2.3
# ===========================================================================

@pytest.mark.c02
def test_foi_auto_generation(client, unique_name, cleanup):
    """18-088 §10.2.2.3 — FeatureOfInterest auto-generated from Thing's Location.

    When POSTing an Observation without a FeatureOfInterest, the server must
    automatically create/link one derived from the Thing's current Location.
    """
    tag = unique_name("foi-auto")
    tree = _create_datastream_tree(client, unique_name, cleanup)
    ds_id = tree["ds_id"]

    # POST Observation without FeatureOfInterest
    obs_payload = {
        "phenomenonTime": "2023-03-15T12:00:00Z",
        "result": 23.5,
        "Datastream": {"@iot.id": ds_id},
    }
    resp = client.create("Observations", obs_payload)

    assert resp.status_code == 201, (
        f"Expected 201 for Observation without FoI, got {resp.status_code}: {resp.text[:300]}"
    )
    obs_url = client.location_of(resp)
    obs_id = id_from_self_link(obs_url)
    cleanup(obs_url)

    # Verify FeatureOfInterest is auto-created and accessible
    foi_url = f"{client.base_url}/Observations({format_id(obs_id)})/FeatureOfInterest"
    foi_resp = client.get(foi_url)
    assert foi_resp.status_code == 200, (
        f"FeatureOfInterest should be auto-generated; GET returned {foi_resp.status_code}"
    )
    foi = foi_resp.json()
    assert "@iot.id" in foi, "Auto-generated FoI must have @iot.id"
    assert "@iot.selfLink" in foi, "Auto-generated FoI must have @iot.selfLink"
    foi_id = entity_id(foi)
    cleanup(f"{client.base_url}/FeaturesOfInterest({format_id(foi_id)})")

    # Auto-FoI should match the Thing's Location geometry
    loc_url = tree["location_url"]
    loc = client.nav(loc_url)
    assert foi["feature"] == loc["location"], (
        "Auto-generated FoI feature must match the Thing's Location geometry"
    )

    # POST a second Observation without FoI; server must reuse the same FoI
    obs2_payload = {
        "phenomenonTime": "2023-03-16T12:00:00Z",
        "result": 24.0,
        "Datastream": {"@iot.id": ds_id},
    }
    resp2 = client.create("Observations", obs2_payload)
    assert resp2.status_code == 201
    obs2_url = client.location_of(resp2)
    obs2_id = id_from_self_link(obs2_url)
    cleanup(obs2_url)

    foi2_resp = client.get(
        f"{client.base_url}/Observations({format_id(obs2_id)})/FeatureOfInterest"
    )
    assert foi2_resp.status_code == 200
    foi2_id = entity_id(foi2_resp.json())
    assert foi2_id == foi_id, (
        "Second Observation without FoI for same Datastream/Location should reuse the same FoI"
    )


# ===========================================================================
# CREATE 6 – phenomenonTime / resultTime defaulting
#   18-088 §10.2.2.3
# ===========================================================================

@pytest.mark.c02
def test_phenomenon_time_defaulting(client, unique_name, cleanup):
    """18-088 §10.2.2.3 — phenomenonTime defaults to server current time when omitted.

    When an Observation is POSTed without phenomenonTime, the server shall
    default it (to resultTime if given, otherwise to server's current time).
    resultTime shall default to null if not provided.
    """
    tag = unique_name("ptdefault")
    tree = _create_datastream_tree(client, unique_name, cleanup)
    ds_id = tree["ds_id"]

    # POST Observation omitting phenomenonTime (and resultTime)
    resp = client.create(
        "Observations",
        {"result": 42.0, "Datastream": {"@iot.id": ds_id}},
    )

    assert resp.status_code == 201, (
        f"POST Observation without phenomenonTime should succeed; got {resp.status_code}: {resp.text[:300]}"
    )
    obs_url = client.location_of(resp)
    obs_id = id_from_self_link(obs_url)
    cleanup(obs_url)

    obs = client.nav(obs_url)
    # phenomenonTime must have been set (not null)
    assert obs.get("phenomenonTime") is not None, (
        "phenomenonTime must be defaulted to a non-null time when omitted in POST"
    )
    # resultTime should be null when not provided
    assert obs.get("resultTime") is None, (
        "resultTime should default to null when not provided in POST"
    )

    foi_resp = client.get(
        f"{client.base_url}/Observations({format_id(obs_id)})/FeatureOfInterest"
    )
    if foi_resp.status_code == 200:
        cleanup(
            f"{client.base_url}/FeaturesOfInterest({format_id(entity_id(foi_resp.json()))})"
        )


@pytest.mark.c02
def test_result_time_stored_when_provided(client, unique_name, cleanup):
    """18-088 §8.3.2 — resultTime is stored when explicitly provided in POST."""
    tag = unique_name("rtset")
    tree = _create_datastream_tree(client, unique_name, cleanup)
    ds_id = tree["ds_id"]

    result_time = "2023-07-01T15:30:00Z"
    resp = client.create(
        "Observations",
        {
            "phenomenonTime": "2023-07-01T14:00:00Z",
            "resultTime": result_time,
            "result": 88.0,
            "Datastream": {"@iot.id": ds_id},
        },
    )

    assert resp.status_code == 201, (
        f"Expected 201, got {resp.status_code}: {resp.text[:300]}"
    )
    obs_url = client.location_of(resp)
    obs_id = id_from_self_link(obs_url)
    cleanup(obs_url)

    obs = client.nav(obs_url)
    stored_rt = obs.get("resultTime", "")
    assert stored_rt is not None and stored_rt != "", (
        f"resultTime must be stored when provided; got: {stored_rt!r}"
    )
    # Value must match (server may normalise to Z or +00:00)
    assert "2023-07-01T15:30:00" in (stored_rt or ""), (
        f"Stored resultTime {stored_rt!r} does not match provided {result_time!r}"
    )

    foi_resp = client.get(
        f"{client.base_url}/Observations({format_id(obs_id)})/FeatureOfInterest"
    )
    if foi_resp.status_code == 200:
        cleanup(
            f"{client.base_url}/FeaturesOfInterest({format_id(entity_id(foi_resp.json()))})"
        )
