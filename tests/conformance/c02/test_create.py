"""
test_create.py -- OGC SensorThings API v1.1 c02 CREATE tests.

Standard:  OGC 18-088 §10.2  (https://docs.ogc.org/is/18-088/18-088.html)
Req namespace: req/create-update-delete/

Coverage:
  CREATE 1  – POST each of the 8 entity collections → 201 + Location header + GET round-trip
  CREATE 3  – Create with link to existing entity via {"@iot.id": <id>}
  CREATE 4  – POST to a navigation link (e.g. /Things(<id>)/Locations)
  SPECIAL   – HistoricalLocation auto-creation on Location assignment
              (req/create-update-delete/historical-location-auto-creation, 18-088 §10.2.2.2)
"""

from __future__ import annotations

import pytest

import sample_data
from client import entity_id, format_id, id_from_self_link
from c02.conftest import _create_datastream_tree

pytestmark = pytest.mark.c02


# ===========================================================================
# CREATE 1 – POST minimal valid entity into each of the 8 collections
#   req/create-update-delete/create-entity
# ===========================================================================

class TestCreate1MinimalEntities:
    """POST a minimal valid payload to each entity collection.

    18-088 §10.2 req/create-update-delete/create-entity:
    "Upon successful completion, the response shall contain a HTTP 201 Created
    status code. The response SHALL contain a Location HTTP header that contains
    the selfLink of the newly created entity."
    """

    @pytest.mark.c02
    def test_post_thing(self, client, unique_name, cleanup):
        """req/create-update-delete/create-entity — POST minimal Thing."""
        tag = unique_name("thing")
        payload = sample_data.minimal_thing(tag)

        resp = client.create("Things", payload)

        assert resp.status_code == 201, (
            f"Expected 201, got {resp.status_code}: {resp.text[:300]}"
        )
        loc_hdr = resp.headers.get("location", "")
        assert loc_hdr.startswith("http"), (
            f"Location header missing or not absolute: {loc_hdr!r}"
        )

        cleanup(loc_hdr)
        entity = client.nav(loc_hdr)
        assert "@iot.id" in entity, "Missing @iot.id in GET response"
        assert "@iot.selfLink" in entity, "Missing @iot.selfLink in GET response"
        assert entity["name"] == payload["name"]
        assert entity["description"] == payload["description"]

    @pytest.mark.c02
    def test_post_location(self, client, unique_name, cleanup):
        """req/create-update-delete/create-entity — POST minimal Location."""
        tag = unique_name("loc")
        payload = sample_data.minimal_location(tag)

        resp = client.create("Locations", payload)

        assert resp.status_code == 201, (
            f"Expected 201, got {resp.status_code}: {resp.text[:300]}"
        )
        loc_hdr = resp.headers.get("location", "")
        assert loc_hdr.startswith("http")

        cleanup(loc_hdr)
        entity = client.nav(loc_hdr)
        assert "@iot.id" in entity
        assert "@iot.selfLink" in entity
        assert entity["name"] == payload["name"]
        assert entity["encodingType"] == payload["encodingType"]

    @pytest.mark.c02
    def test_post_sensor(self, client, unique_name, cleanup):
        """req/create-update-delete/create-entity — POST minimal Sensor."""
        tag = unique_name("sensor")
        payload = sample_data.minimal_sensor(tag)

        resp = client.create("Sensors", payload)

        assert resp.status_code == 201, (
            f"Expected 201, got {resp.status_code}: {resp.text[:300]}"
        )
        loc_hdr = resp.headers.get("location", "")
        assert loc_hdr.startswith("http")

        cleanup(loc_hdr)
        entity = client.nav(loc_hdr)
        assert "@iot.id" in entity
        assert "@iot.selfLink" in entity
        assert entity["name"] == payload["name"]
        assert entity["encodingType"] == payload["encodingType"]

    @pytest.mark.c02
    def test_post_observed_property(self, client, unique_name, cleanup):
        """req/create-update-delete/create-entity — POST minimal ObservedProperty."""
        tag = unique_name("op")
        payload = sample_data.minimal_observed_property(tag)

        resp = client.create("ObservedProperties", payload)

        assert resp.status_code == 201, (
            f"Expected 201, got {resp.status_code}: {resp.text[:300]}"
        )
        loc_hdr = resp.headers.get("location", "")
        assert loc_hdr.startswith("http")

        cleanup(loc_hdr)
        entity = client.nav(loc_hdr)
        assert "@iot.id" in entity
        assert "@iot.selfLink" in entity
        assert entity["name"] == payload["name"]
        assert entity["definition"] == payload["definition"]

    @pytest.mark.c02
    def test_post_datastream(self, client, unique_name, cleanup):
        """req/create-update-delete/create-entity — POST minimal Datastream with existing links."""
        tag = unique_name("ds")
        tree = _create_datastream_tree(client, unique_name, cleanup)

        # The Datastream was already created as part of the setup tree; verify it.
        ds_url = tree["ds_url"]
        entity = client.nav(ds_url)
        assert "@iot.id" in entity
        assert "@iot.selfLink" in entity
        assert "Datastream" in ds_url

    @pytest.mark.c02
    def test_post_observation(self, client, unique_name, cleanup):
        """req/create-update-delete/create-entity — POST minimal Observation."""
        tag = unique_name("obs")
        tree = _create_datastream_tree(client, unique_name, cleanup)
        ds_id = tree["ds_id"]

        payload = sample_data.minimal_observation(tag, ds_id, result=7.5)
        resp = client.create("Observations", payload)

        assert resp.status_code == 201, (
            f"Expected 201, got {resp.status_code}: {resp.text[:300]}"
        )
        loc_hdr = resp.headers.get("location", "")
        assert loc_hdr.startswith("http")

        obs_id = id_from_self_link(loc_hdr)
        # Register FoI for cleanup (auto-generated; not cascade-deleted)
        cleanup(loc_hdr)
        entity = client.nav(loc_hdr)
        assert "@iot.id" in entity
        assert "@iot.selfLink" in entity
        assert entity["result"] == payload["result"]

        # FoI auto-generated — register for cleanup
        foi = client.nav(f"{client.base_url}/Observations({format_id(obs_id)})/FeatureOfInterest")
        cleanup(f"{client.base_url}/FeaturesOfInterest({format_id(entity_id(foi))})")

    @pytest.mark.c02
    def test_post_feature_of_interest(self, client, unique_name, cleanup):
        """req/create-update-delete/create-entity — POST minimal FeatureOfInterest."""
        tag = unique_name("foi")
        payload = sample_data.minimal_feature_of_interest(tag)

        resp = client.create("FeaturesOfInterest", payload)

        assert resp.status_code == 201, (
            f"Expected 201, got {resp.status_code}: {resp.text[:300]}"
        )
        loc_hdr = resp.headers.get("location", "")
        assert loc_hdr.startswith("http")

        cleanup(loc_hdr)
        entity = client.nav(loc_hdr)
        assert "@iot.id" in entity
        assert "@iot.selfLink" in entity
        assert entity["name"] == payload["name"]
        assert entity["encodingType"] == payload["encodingType"]

    @pytest.mark.c02
    def test_post_historical_location(self, client, unique_name, cleanup):
        """req/create-update-delete/create-entity — POST minimal HistoricalLocation."""
        tag = unique_name("hl")

        # Need an existing Thing
        thing_resp = client.create("Things", sample_data.minimal_thing(tag))
        assert thing_resp.status_code == 201
        thing_url = client.location_of(thing_resp)
        t_id = id_from_self_link(thing_url)
        cleanup(thing_url)

        payload = sample_data.minimal_historical_location(tag, t_id)
        resp = client.create("HistoricalLocations", payload)

        assert resp.status_code == 201, (
            f"Expected 201, got {resp.status_code}: {resp.text[:300]}"
        )
        loc_hdr = resp.headers.get("location", "")
        assert loc_hdr.startswith("http")

        cleanup(loc_hdr)
        entity = client.nav(loc_hdr)
        assert "@iot.id" in entity
        assert "@iot.selfLink" in entity
        assert entity["time"] == payload["time"]
        # Navigation links expected
        assert "Locations@iot.navigationLink" in entity
        assert "Thing@iot.navigationLink" in entity


# ===========================================================================
# CREATE 3 – Create with link to existing entity via {"@iot.id": <id>}
#   req/create-update-delete/link-to-existing-entities
# ===========================================================================

@pytest.mark.c02
def test_create_with_existing_link(client, unique_name, cleanup, seed):
    """req/create-update-delete/link-to-existing-entities.

    POST a Datastream referencing existing Thing, Sensor, ObservedProperty by id.
    The server must accept {"@iot.id": <existing-id>} references and link them.
    """
    tag = unique_name("link")

    # Create a fresh Thing to link
    thing_resp = client.create("Things", sample_data.minimal_thing(tag))
    assert thing_resp.status_code == 201
    thing_url = client.location_of(thing_resp)
    t_id = id_from_self_link(thing_url)
    cleanup(thing_url)

    # Create fresh Sensor and ObservedProperty
    s_resp = client.create("Sensors", sample_data.minimal_sensor(tag))
    assert s_resp.status_code == 201
    s_url = client.location_of(s_resp)
    s_id = id_from_self_link(s_url)
    cleanup(s_url)

    op_resp = client.create("ObservedProperties", sample_data.minimal_observed_property(tag))
    assert op_resp.status_code == 201
    op_url = client.location_of(op_resp)
    op_id = id_from_self_link(op_url)
    cleanup(op_url)

    # Create Datastream linking to the above via @iot.id
    ds_payload = sample_data.minimal_datastream(tag, t_id, s_id, op_id)
    ds_resp = client.create("Datastreams", ds_payload)

    assert ds_resp.status_code == 201, (
        f"Expected 201, got {ds_resp.status_code}: {ds_resp.text[:300]}"
    )
    ds_url = client.location_of(ds_resp)
    cleanup(ds_url)

    # Verify links are correctly established
    ds = client.nav(ds_url, params={"$expand": "Thing,Sensor,ObservedProperty"})
    assert entity_id(ds["Thing"]) == t_id, "Datastream must link to the specified Thing"
    assert entity_id(ds["Sensor"]) == s_id, "Datastream must link to the specified Sensor"
    assert entity_id(ds["ObservedProperty"]) == op_id, (
        "Datastream must link to the specified ObservedProperty"
    )


@pytest.mark.c02
def test_create_observation_with_existing_foi_link(client, unique_name, cleanup):
    """req/create-update-delete/link-to-existing-entities — POST Observation linking
    an existing Datastream AND an existing FeatureOfInterest by @iot.id.

    The server must accept the existing FoI reference and must NOT auto-generate a
    new one; the Observation must be navigable to the specified FoI.
    """
    tag = unique_name("obs-link")
    tree = _create_datastream_tree(client, unique_name, cleanup)
    ds_id = tree["ds_id"]

    # Create a standalone FeatureOfInterest to link explicitly
    foi_payload = sample_data.minimal_feature_of_interest(tag)
    foi_resp = client.create("FeaturesOfInterest", foi_payload)
    assert foi_resp.status_code == 201
    foi_url = client.location_of(foi_resp)
    foi_id = id_from_self_link(foi_url)
    cleanup(foi_url)

    # POST Observation with both Datastream and FoI links
    obs_payload = {
        "phenomenonTime": "2015-03-03T00:00:00Z",
        "result": 3,
        "Datastream": {"@iot.id": ds_id},
        "FeatureOfInterest": {"@iot.id": foi_id},
    }
    obs_resp = client.create("Observations", obs_payload)
    assert obs_resp.status_code == 201, (
        f"POST Observation with explicit FoI link must return 201; "
        f"got {obs_resp.status_code}: {obs_resp.text[:300]}"
    )
    obs_url = client.location_of(obs_resp)
    obs_id = id_from_self_link(obs_url)
    cleanup(obs_url)

    # Verify FoI link resolves to the one we specified (not an auto-generated one)
    linked_foi = client.nav(
        f"{client.base_url}/Observations({format_id(obs_id)})/FeatureOfInterest"
    )
    assert entity_id(linked_foi) == foi_id, (
        "Observation must be linked to the explicitly specified FeatureOfInterest, "
        f"not a new auto-generated one. Expected {foi_id}, got {entity_id(linked_foi)}"
    )

    # Verify Datastream link too
    obs_with_ds = client.nav(obs_url, params={"$expand": "Datastream"})
    assert entity_id(obs_with_ds["Datastream"]) == ds_id, (
        "Observation must be linked to the specified Datastream"
    )


# ===========================================================================
# CREATE 4 – POST to a navigation link
#   req/create-update-delete/create-related-entities
# ===========================================================================

@pytest.mark.c02
def test_post_to_navigation_link_thing_locations(client, unique_name, cleanup):
    """req/create-update-delete/create-related-entities — POST /Things(<id>)/Locations.

    A POST to a navigation link must create the entity AND link it to the parent.
    """
    tag = unique_name("nav")

    # Create a bare Thing (no Location)
    thing_resp = client.create("Things", sample_data.minimal_thing(tag))
    assert thing_resp.status_code == 201
    thing_url = client.location_of(thing_resp)
    t_id = id_from_self_link(thing_url)
    cleanup(thing_url)

    # POST a Location via the navigation link
    loc_payload = sample_data.minimal_location(tag)
    resp = client.post(
        f"Things({format_id(t_id)})/Locations",
        json=loc_payload,
    )

    assert resp.status_code == 201, (
        f"POST to navigation link should return 201, got {resp.status_code}: {resp.text[:300]}"
    )
    loc_hdr = resp.headers.get("location", "")
    assert loc_hdr.startswith("http")
    cleanup(loc_hdr)

    # Verify the Location is linked to the Thing
    thing_locs = client.nav(f"Things({format_id(t_id)})/Locations")
    ids_in_thing = [entity_id(e) for e in thing_locs.get("value", [])]
    new_loc_id = id_from_self_link(loc_hdr)
    assert new_loc_id in ids_in_thing, (
        "Newly created Location must appear in Things(<id>)/Locations navigation"
    )


@pytest.mark.c02
def test_post_to_navigation_link_datastream_observations(client, unique_name, cleanup):
    """req/create-update-delete/create-related-entities — POST /Datastreams(<id>)/Observations.

    A POST to the Datastreams navigation link must create and link the Observation.
    """
    tag = unique_name("navobs")
    tree = _create_datastream_tree(client, unique_name, cleanup)
    ds_id = tree["ds_id"]

    # POST Observation via navigation link (no Datastream key in body required)
    obs_payload = {
        "phenomenonTime": "2023-05-01T00:00:00Z",
        "result": 55.0,
    }
    resp = client.post(
        f"Datastreams({format_id(ds_id)})/Observations",
        json=obs_payload,
    )

    assert resp.status_code == 201, (
        f"POST to Datastreams nav link should return 201, got {resp.status_code}: {resp.text[:300]}"
    )
    obs_hdr = resp.headers.get("location", "")
    assert obs_hdr.startswith("http")
    obs_id = id_from_self_link(obs_hdr)
    cleanup(obs_hdr)

    # Verify the Observation is linked to the Datastream
    obs = client.nav(obs_hdr, params={"$expand": "Datastream"})
    assert entity_id(obs["Datastream"]) == ds_id, (
        "Observation created via nav link must be linked to the parent Datastream"
    )

    # Verify it appears in /Datastreams(<id>)/Observations
    ds_obs = client.nav(f"Datastreams({format_id(ds_id)})/Observations")
    obs_ids = [entity_id(o) for o in ds_obs.get("value", [])]
    assert obs_id in obs_ids, "Observation must appear in parent Datastream's Observations nav"

    # Cleanup FoI
    foi_resp = client.get(
        f"{client.base_url}/Observations({format_id(obs_id)})/FeatureOfInterest"
    )
    if foi_resp.status_code == 200:
        foi_id = entity_id(foi_resp.json())
        cleanup(f"{client.base_url}/FeaturesOfInterest({format_id(foi_id)})")


# ===========================================================================
# SPECIAL – HistoricalLocation auto-creation
#   req/create-update-delete/historical-location-auto-creation  (18-088 §10.2.2.2)
# ===========================================================================

@pytest.mark.c02
def test_historical_location_auto_creation(client, unique_name, cleanup):
    """req/create-update-delete/historical-location-auto-creation — 18-088 §10.2.2.2.

    When a Location is associated with a Thing (whether via deep insert or via a
    POST to Things(<id>)/Locations), the server shall automatically create a
    HistoricalLocation linking the Thing to that Location with a time-stamp.

    Verified behaviours:
    1. Create a Thing WITH an inline Location (deep insert) → exactly 1 HistoricalLocation
       is auto-created, linked to the first Location with a non-empty 'time'.
    2. POST a second Location to Things(<id>)/Locations → a SECOND HistoricalLocation
       is auto-created (count 1 → 2); the newest HL links the newly added Location.
    3. Cleanup: delete Thing (cascades HLs), delete both Locations explicitly.
    """
    tag = unique_name("hl-auto")

    # --- Step 1: Create Thing with inline Location (deep insert) ---------------
    loc1_payload = sample_data.minimal_location(f"{tag}-loc1")
    thing_payload = {
        **sample_data.minimal_thing(tag),
        "Locations": [loc1_payload],
    }
    t_resp = client.create("Things", thing_payload)
    assert t_resp.status_code == 201, (
        f"Deep-insert Thing+Location must return 201; got {t_resp.status_code}: {t_resp.text[:300]}"
    )
    thing_url = client.location_of(t_resp)
    t_id = id_from_self_link(thing_url)
    cleanup(thing_url)  # cascade-deletes Datastreams, HLs

    # Retrieve the auto-created Location's id from the Thing's Locations nav
    thing_with_locs = client.nav(thing_url, params={"$expand": "Locations"})
    assert len(thing_with_locs["Locations"]) == 1, (
        "Exactly 1 Location must be linked to the Thing after deep insert"
    )
    loc1_id = entity_id(thing_with_locs["Locations"][0])
    loc1_url = f"{client.base_url}/Locations({format_id(loc1_id)})"
    cleanup(loc1_url)  # Locations are NOT cascade-deleted by Thing deletion

    # Assert exactly 1 HistoricalLocation was auto-created
    hl_data = client.nav(
        f"Things({format_id(t_id)})/HistoricalLocations",
        params={"$expand": "Locations", "$count": "true"},
    )
    hl_list = hl_data.get("value", [])
    hl_count = hl_data.get("@iot.count", len(hl_list))
    assert hl_count == 1, (
        f"req/create-update-delete/historical-location-auto-creation: "
        f"1 HistoricalLocation must be auto-created on deep insert; found {hl_count}"
    )

    hl1 = hl_list[0]
    # HL must carry a non-empty 'time'
    assert hl1.get("time") not in (None, ""), (
        "Auto-created HistoricalLocation must have a non-empty 'time' property"
    )
    # HL must be linked to loc1
    hl1_loc_ids = [entity_id(l) for l in hl1.get("Locations", [])]
    assert loc1_id in hl1_loc_ids, (
        f"Auto-created HistoricalLocation must link to the initial Location "
        f"(id={loc1_id}); HL Locations: {hl1_loc_ids}"
    )

    # --- Step 2: POST a second Location via the nav link -----------------------
    loc2_payload = sample_data.minimal_location(f"{tag}-loc2")
    # Override coordinates so it's a distinct Location
    loc2_payload["location"] = {"type": "Point", "coordinates": [-110.0, 50.0]}
    nav_resp = client.post(
        f"Things({format_id(t_id)})/Locations",
        json=loc2_payload,
    )
    assert nav_resp.status_code == 201, (
        f"POST to Things nav link must return 201; got {nav_resp.status_code}: {nav_resp.text[:300]}"
    )
    loc2_url = client.location_of(nav_resp)
    loc2_id = id_from_self_link(loc2_url)
    cleanup(loc2_url)  # cleanup loc2 (also not cascade-deleted)

    # Assert a SECOND HistoricalLocation was auto-created (count 1 → 2)
    hl_data2 = client.nav(
        f"Things({format_id(t_id)})/HistoricalLocations",
        params={"$expand": "Locations", "$count": "true"},
    )
    hl_list2 = hl_data2.get("value", [])
    hl_count2 = hl_data2.get("@iot.count", len(hl_list2))
    assert hl_count2 == 2, (
        f"req/create-update-delete/historical-location-auto-creation: "
        f"A second HistoricalLocation must be auto-created when a new Location is "
        f"POSTed to Things(<id>)/Locations; found {hl_count2}"
    )

    # The newest HL must link the newly added Location (loc2)
    # Find the HL that references loc2
    loc2_hl = next(
        (
            hl for hl in hl_list2
            if loc2_id in [entity_id(l) for l in hl.get("Locations", [])]
        ),
        None,
    )
    assert loc2_hl is not None, (
        f"req/create-update-delete/historical-location-auto-creation: "
        f"The second HistoricalLocation must be linked to the new Location "
        f"(id={loc2_id}); HL Locations in all HLs: "
        f"{[[entity_id(l) for l in hl.get('Locations',[])] for hl in hl_list2]}"
    )
    assert loc2_hl.get("time") not in (None, ""), (
        "Second auto-created HistoricalLocation must have a non-empty 'time'"
    )
