"""
test_c02_cud.py -- OGC SensorThings API v1.1 Create-Update-Delete conformance.

Standard:  OGC 18-088 §10  (https://docs.ogc.org/is/18-088/18-088.html)
Req namespace: req/create-update-delete/

Coverage checklist (c02):
  CREATE 1  – POST each of the 8 entity collections → 201 + Location header + GET round-trip
  CREATE 2  – Deep insert (Thing→Location, Datastream→Sensor+ObservedProperty+Observations)
  CREATE 3  – Create with link to existing entity via {"@iot.id": <id>}
  CREATE 4  – POST to a navigation link (e.g. /Things(<id>)/Locations)
  CREATE 5  – FeatureOfInterest auto-generation from Thing's Location
  CREATE 6  – phenomenonTime / resultTime defaulting
  CREATE 7  – Validation errors (missing mandatory property, bad link, malformed JSON, unknown property)
  UPDATE 8  – PATCH scalar property on each entity type (200; GET confirms change, others untouched)
  UPDATE 9  – PATCH relation (re-link a Datastream to a different Sensor)
  UPDATE 10 – PATCH non-existent entity → 404
  UPDATE 11 – PUT full replacement (req/create-update-delete/update-entity-put, 18-088 §10.3)
  DELETE 12 – DELETE each entity type → 200/204; subsequent GET → 404
  DELETE 13 – Cascade: Datastream→Observations; Thing→Datastreams+HistoricalLocations
  DELETE 14 – DELETE non-existent entity → 404
  DELETE 15 – (set-to-null / unlink not separately specified; covered by PATCH relation test)
  SPECIAL   – HistoricalLocation auto-creation on Location assignment
              (req/create-update-delete/historical-location-auto-creation, 18-088 §10.2.2.2)

Known istSOS4 behaviours discovered during probing (reported to LEAD):
  [B1] POST body is EMPTY — spec §10.2.1 mandates only the Location header, body is optional.
       Tests therefore GET the Location header URL to verify persisted state.
  [B2] PATCH returns 200 + empty body (spec allows 200 or 204).
  [B3] DELETE returns 200 + empty body (spec allows 200 or 204).
  [B5] FoI partial PATCH (without 'feature') → FIXED by api-fixer.
  [B6] Sensor.metadata bare-string → FIXED by api-fixer (now 201).
  [B7] Malformed JSON body → 422 (FastAPI validation), not 400. Tests accept 4xx.
  [B8] phenomenonTime omitted in Observation POST → server defaults to current server time.
       Consistent with 18-088 §10.2.2.3 defaulting rules.
  [B9] JSON-Patch (application/json-patch+json) → 422 even though
       req/create-update-delete/update-entity-jsonpatch IS advertised.
       Real spec violation; covered in test_c02_jsonpatch.py.
"""

from __future__ import annotations

import pytest

import sample_data
from client import entity_id, format_id, id_from_self_link, self_link


# ---------------------------------------------------------------------------
# Cleanup fixture – tracks absolute URL strings; deletes in reverse order on
# teardown so dependents are removed before parents.  Tolerates 404 because
# cascade deletes may have already removed some entities.
# ---------------------------------------------------------------------------

@pytest.fixture
def cleanup(client):
    """Collect self-link URLs; delete all on teardown (tolerate 404/any error)."""
    links: list[str] = []

    def track(*urls: str) -> None:
        """Register one or more absolute self-link URLs for cleanup."""
        links.extend(u for u in urls if u)

    yield track

    for url in reversed(links):
        try:
            client.delete(url)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Internal setup helper – builds a minimal Thing+Location+Sensor+ObservedProperty
# +Datastream tree for tests that need a ready Datastream.
# ---------------------------------------------------------------------------

def _create_datastream_tree(client, unique_name, cleanup):
    """Create the minimal subtree required to post Observations.

    Returns a dict with keys: thing_id, location_id, sensor_id, op_id, ds_id,
    thing_url, location_url, sensor_url, op_url, ds_url.
    """
    tag = unique_name("cud")

    # Thing with inline Location (ensures HistoricalLocation + Location linkage).
    thing_payload = {
        **sample_data.minimal_thing(tag),
        "Locations": [sample_data.minimal_location(tag)],
    }
    t_resp = client.create("Things", thing_payload)
    assert t_resp.status_code == 201, (
        f"setup Thing failed: {t_resp.status_code} {t_resp.text[:300]}"
    )
    thing_url = client.location_of(t_resp)
    thing_data = client.nav(thing_url, params={"$expand": "Locations"})
    t_id = entity_id(thing_data)
    loc_id = entity_id(thing_data["Locations"][0])
    loc_url = f"{client.base_url}/Locations({format_id(loc_id)})"

    # Sensor
    s_resp = client.create("Sensors", sample_data.minimal_sensor(tag))
    assert s_resp.status_code == 201, (
        f"setup Sensor failed: {s_resp.status_code} {s_resp.text[:300]}"
    )
    sensor_url = client.location_of(s_resp)
    s_id = id_from_self_link(sensor_url)

    # ObservedProperty
    op_resp = client.create("ObservedProperties", sample_data.minimal_observed_property(tag))
    assert op_resp.status_code == 201, (
        f"setup ObservedProperty failed: {op_resp.status_code} {op_resp.text[:300]}"
    )
    op_url = client.location_of(op_resp)
    op_id = id_from_self_link(op_url)

    # Datastream
    ds_resp = client.create(
        "Datastreams",
        sample_data.minimal_datastream(tag, t_id, s_id, op_id),
    )
    assert ds_resp.status_code == 201, (
        f"setup Datastream failed: {ds_resp.status_code} {ds_resp.text[:300]}"
    )
    ds_url = client.location_of(ds_resp)
    ds_id = id_from_self_link(ds_url)

    # Register for cleanup.
    # Deletion order (reversed list, so we add in reverse-of-desired order):
    #   desired: sensor, op, loc, thing (thing cascades DS+HistLocs)
    #   added:   thing, loc, op, sensor  →  reversed: sensor, op, loc, thing ✓
    cleanup(thing_url, loc_url, op_url, sensor_url)

    return {
        "thing_id": t_id,
        "location_id": loc_id,
        "sensor_id": s_id,
        "op_id": op_id,
        "ds_id": ds_id,
        "thing_url": thing_url,
        "location_url": loc_url,
        "sensor_url": sensor_url,
        "op_url": op_url,
        "ds_url": ds_url,
    }


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


# ===========================================================================
# CREATE 7 – Validation errors
# ===========================================================================

@pytest.mark.c02
def test_validation_missing_name_thing(client, unique_name):
    """req/create-update-delete/create-entity — missing mandatory property → 400.

    Posting a Thing without the mandatory 'name' property must be rejected
    with a 400 Bad Request.
    """
    resp = client.create("Things", {"description": "no-name-thing"})
    assert resp.status_code == 400, (
        f"Missing mandatory 'name' should return 400, got {resp.status_code}: {resp.text[:300]}"
    )


@pytest.mark.c02
def test_validation_missing_name_location(client, unique_name):
    """req/create-update-delete/create-entity — missing mandatory 'name' for Location → 400."""
    resp = client.create(
        "Locations",
        {
            "description": "no-name",
            "encodingType": sample_data.GEOJSON,
            "location": sample_data.SEED_POINT,
        },
    )
    assert resp.status_code == 400, (
        f"Missing 'name' in Location should return 400, got {resp.status_code}: {resp.text[:300]}"
    )


@pytest.mark.c02
def test_validation_missing_mandatory_datastream(client, unique_name, cleanup):
    """req/create-update-delete/create-entity — Datastream without mandatory Sensor → 4xx."""
    tag = unique_name("valmissing")

    # Create Thing + ObservedProperty for a partial Datastream
    t_resp = client.create("Things", sample_data.minimal_thing(tag))
    assert t_resp.status_code == 201
    t_url = client.location_of(t_resp)
    t_id = id_from_self_link(t_url)
    cleanup(t_url)

    op_resp = client.create("ObservedProperties", sample_data.minimal_observed_property(tag))
    assert op_resp.status_code == 201
    op_url = client.location_of(op_resp)
    op_id = id_from_self_link(op_url)
    cleanup(op_url)

    # Datastream without Sensor (mandatory)
    resp = client.create(
        "Datastreams",
        {
            "name": f"{tag} DS-no-sensor",
            "description": "missing sensor",
            "unitOfMeasurement": sample_data.unit_lumen(),
            "observationType": sample_data.OM_MEASUREMENT,
            "Thing": {"@iot.id": t_id},
            "ObservedProperty": {"@iot.id": op_id},
        },
    )
    assert resp.status_code in (400, 422), (
        f"Datastream without Sensor should be rejected (4xx), got {resp.status_code}"
    )


@pytest.mark.c02
def test_validation_bad_iot_id_link(client, unique_name):
    """req/create-update-delete/create-entity — non-existent @iot.id link → 4xx.

    Referencing a non-existent entity via {"@iot.id": <id>} must be rejected.
    """
    resp = client.create(
        "Datastreams",
        {
            "name": "bad-link-ds",
            "description": "bad links",
            "unitOfMeasurement": sample_data.unit_lumen(),
            "observationType": sample_data.OM_MEASUREMENT,
            "Thing": {"@iot.id": 999999999},
            "Sensor": {"@iot.id": 999999998},
            "ObservedProperty": {"@iot.id": 999999997},
        },
    )
    assert resp.status_code in (400, 404, 409), (
        f"Non-existent @iot.id link should return 4xx, got {resp.status_code}: {resp.text[:300]}"
    )


@pytest.mark.c02
def test_validation_malformed_json(client, unique_name):
    """req/create-update-delete/create-entity — malformed JSON body → 4xx.

    Sending a syntactically invalid JSON body must result in a 4xx response.
    Note: istSOS4 (FastAPI) returns 422 for JSON parse errors [B7].
    """
    resp = client.post(
        "Things",
        content=b"{not valid json!!!}",
        headers={"content-type": "application/json"},
    )
    assert resp.status_code in (400, 422), (
        f"Malformed JSON should return 4xx, got {resp.status_code}: {resp.text[:300]}"
    )


@pytest.mark.c02
def test_validation_unknown_property(client, unique_name):
    """req/create-update-delete/create-entity — unknown/extra property → 4xx.

    Posting with an unknown property key must be rejected.
    18-088 §10.2.1 requires the server to reject unrecognised properties.
    """
    resp = client.create(
        "Things",
        {
            "name": "unknown-prop-thing",
            "description": "has an unknown property",
            "unknownExtraProperty": "should-be-rejected",
        },
    )
    assert resp.status_code in (400, 422), (
        f"Unknown property should be rejected (4xx), got {resp.status_code}: {resp.text[:300]}"
    )


@pytest.mark.c02
def test_validation_sensor_string_metadata(client, unique_name):
    """18-088 §8.2.5 — Sensor.metadata bare-string URL must be accepted.

    Per OGC 18-088 §8.2.5, for encodingType 'application/pdf' the metadata
    property is a URL (string).  The server must accept and store it as-is.

    SPEC VIOLATION: istSOS4 stores metadata as JSONB and rejects bare strings
    with 400 [B6].  This test will pass once api-fixer resolves the issue.
    req/create-update-delete/create-entity
    """
    tag = unique_name("strmeta")
    resp = client.create(
        "Sensors",
        {
            "name": f"{tag} Sensor-string-meta",
            "description": "sensor with string metadata",
            "encodingType": sample_data.SENSOR_PDF,
            "metadata": "https://example.org/sensor-spec.pdf",
        },
    )
    # Per 18-088 §8.2.5 this MUST succeed with 201.
    # istSOS4 returns 400 — routed to api-fixer.
    assert resp.status_code == 201, (
        f"SPEC VIOLATION (req/create-update-delete/create-entity, 18-088 §8.2.5): "
        f"Sensor with string metadata must return 201; got {resp.status_code}: {resp.text[:300]}"
    )
    if resp.status_code == 201:
        client.delete(client.location_of(resp))


# ===========================================================================
# UPDATE 8 – PATCH scalar property on each entity type
#   req/create-update-delete/update-entity
# ===========================================================================

@pytest.mark.c02
def test_patch_thing(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — PATCH Thing scalar property.

    PATCH must update only the specified properties and leave others unchanged.
    """
    tag = unique_name("patch-thing")
    original = sample_data.minimal_thing(tag)
    resp = client.create("Things", original)
    assert resp.status_code == 201
    url = client.location_of(resp)
    cleanup(url)

    patch_resp = client.patch(url, json={"description": "patched-description"})
    assert patch_resp.status_code in (200, 204), (
        f"PATCH should return 200 or 204, got {patch_resp.status_code}"
    )

    after = client.nav(url)
    assert after["description"] == "patched-description", "PATCH must update description"
    assert after["name"] == original["name"], "PATCH must not change unspecified properties"


@pytest.mark.c02
def test_patch_location(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — PATCH Location scalar property."""
    tag = unique_name("patch-loc")
    original = sample_data.minimal_location(tag)
    resp = client.create("Locations", original)
    assert resp.status_code == 201
    url = client.location_of(resp)
    cleanup(url)

    patch_resp = client.patch(url, json={"description": "patched-location"})
    assert patch_resp.status_code in (200, 204), (
        f"PATCH should return 200 or 204, got {patch_resp.status_code}"
    )

    after = client.nav(url)
    assert after["description"] == "patched-location"
    assert after["name"] == original["name"], "name must be unchanged"


@pytest.mark.c02
def test_patch_sensor(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — PATCH Sensor scalar property."""
    tag = unique_name("patch-sensor")
    original = sample_data.minimal_sensor(tag)
    resp = client.create("Sensors", original)
    assert resp.status_code == 201
    url = client.location_of(resp)
    cleanup(url)

    patch_resp = client.patch(url, json={"description": "patched-sensor"})
    assert patch_resp.status_code in (200, 204), (
        f"PATCH should return 200 or 204, got {patch_resp.status_code}"
    )

    after = client.nav(url)
    assert after["description"] == "patched-sensor"
    assert after["name"] == original["name"], "name must be unchanged"


@pytest.mark.c02
def test_patch_observed_property(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — PATCH ObservedProperty scalar property."""
    tag = unique_name("patch-op")
    original = sample_data.minimal_observed_property(tag)
    resp = client.create("ObservedProperties", original)
    assert resp.status_code == 201
    url = client.location_of(resp)
    cleanup(url)

    patch_resp = client.patch(url, json={"description": "patched-op"})
    assert patch_resp.status_code in (200, 204), (
        f"PATCH should return 200 or 204, got {patch_resp.status_code}"
    )

    after = client.nav(url)
    assert after["description"] == "patched-op"
    assert after["name"] == original["name"], "name must be unchanged"


@pytest.mark.c02
def test_patch_datastream(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — PATCH Datastream scalar property."""
    tag = unique_name("patch-ds")
    tree = _create_datastream_tree(client, unique_name, cleanup)
    ds_url = tree["ds_url"]
    original_ds = client.nav(ds_url)

    patch_resp = client.patch(ds_url, json={"description": "patched-datastream"})
    assert patch_resp.status_code in (200, 204), (
        f"PATCH should return 200 or 204, got {patch_resp.status_code}"
    )

    after = client.nav(ds_url)
    assert after["description"] == "patched-datastream"
    assert after["name"] == original_ds["name"], "name must be unchanged"


@pytest.mark.c02
def test_patch_observation(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — PATCH Observation scalar property (result)."""
    tag = unique_name("patch-obs")
    tree = _create_datastream_tree(client, unique_name, cleanup)
    ds_id = tree["ds_id"]

    obs_payload = sample_data.minimal_observation(tag, ds_id, result=10.0)
    obs_resp = client.create("Observations", obs_payload)
    assert obs_resp.status_code == 201
    obs_url = client.location_of(obs_resp)
    obs_id = id_from_self_link(obs_url)
    cleanup(obs_url)

    # Track auto-generated FoI
    foi_resp = client.get(
        f"{client.base_url}/Observations({format_id(obs_id)})/FeatureOfInterest"
    )
    if foi_resp.status_code == 200:
        cleanup(
            f"{client.base_url}/FeaturesOfInterest({format_id(entity_id(foi_resp.json()))})"
        )

    patch_resp = client.patch(obs_url, json={"result": 99.9})
    assert patch_resp.status_code in (200, 204), (
        f"PATCH should return 200 or 204, got {patch_resp.status_code}"
    )

    after = client.nav(obs_url)
    assert after["result"] == 99.9, "result must be updated after PATCH"
    assert after["phenomenonTime"] == obs_payload["phenomenonTime"], (
        "phenomenonTime must not change after PATCH"
    )


@pytest.mark.c02
def test_patch_feature_of_interest(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — PATCH FeatureOfInterest.

    Note: istSOS4 requires the 'feature' property even in partial PATCH bodies [B5].
    This test includes 'feature' to make the basic PATCH work. A separate test
    (test_patch_foi_partial_update) verifies the spec-required partial-update
    behaviour and is expected to fail (candidate spec violation).
    """
    tag = unique_name("patch-foi")
    original = sample_data.minimal_feature_of_interest(tag)
    resp = client.create("FeaturesOfInterest", original)
    assert resp.status_code == 201
    url = client.location_of(resp)
    cleanup(url)

    # Include 'feature' to satisfy the server's non-standard requirement
    patch_resp = client.patch(
        url,
        json={
            "description": "patched-foi",
            "feature": original["feature"],
        },
    )
    assert patch_resp.status_code in (200, 204), (
        f"PATCH should return 200 or 204, got {patch_resp.status_code}"
    )

    after = client.nav(url)
    assert after["description"] == "patched-foi"
    assert after["name"] == original["name"], "name must be unchanged"


@pytest.mark.c02
def test_patch_foi_partial_update(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — FoI PATCH with partial body must succeed.

    18-088 §10.3: "only values of those properties specified in the PATCH request
    body are updated".  Patching just 'description' must not require 'feature'.
    Fixed by api-fixer [B5]; now passes.
    """
    tag = unique_name("foi-partial")
    original = sample_data.minimal_feature_of_interest(tag)
    resp = client.create("FeaturesOfInterest", original)
    assert resp.status_code == 201
    url = client.location_of(resp)
    cleanup(url)

    # Partial PATCH — only description, no feature
    patch_resp = client.patch(url, json={"description": "partial-patch-foi"})
    assert patch_resp.status_code in (200, 204), (
        f"Partial PATCH of FoI description should return 200/204. "
        f"Got {patch_resp.status_code}: {patch_resp.text[:300]}"
    )

    after = client.nav(url)
    assert after["description"] == "partial-patch-foi", "description must be updated"
    assert after["name"] == original["name"], "name must be unchanged by partial PATCH"


@pytest.mark.c02
def test_patch_historical_location(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — PATCH HistoricalLocation scalar property."""
    tag = unique_name("patch-hl")

    thing_resp = client.create("Things", sample_data.minimal_thing(tag))
    assert thing_resp.status_code == 201
    thing_url = client.location_of(thing_resp)
    t_id = id_from_self_link(thing_url)
    cleanup(thing_url)

    hl_resp = client.create(
        "HistoricalLocations",
        sample_data.minimal_historical_location(tag, t_id, time="2022-01-01T00:00:00Z"),
    )
    assert hl_resp.status_code == 201
    hl_url = client.location_of(hl_resp)
    cleanup(hl_url)

    patch_resp = client.patch(hl_url, json={"time": "2022-06-01T00:00:00Z"})
    assert patch_resp.status_code in (200, 204), (
        f"PATCH should return 200 or 204, got {patch_resp.status_code}"
    )

    after = client.nav(hl_url)
    assert "2022-06-01" in after["time"], "time must be updated after PATCH"


# ===========================================================================
# UPDATE 9 – PATCH relation (re-link)
#   req/create-update-delete/update-entity
# ===========================================================================

@pytest.mark.c02
def test_patch_relation_relink_sensor(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — PATCH Datastream to re-link its Sensor.

    PATCH {"Sensor": {"@iot.id": <new_sensor_id>}} must update the Datastream's
    Sensor relation.
    """
    tag = unique_name("relink")
    tree = _create_datastream_tree(client, unique_name, cleanup)
    ds_url = tree["ds_url"]
    original_sensor_id = tree["sensor_id"]

    # Create a second Sensor to re-link to
    tag2 = unique_name("sensor-b")
    s2_resp = client.create("Sensors", sample_data.minimal_sensor(tag2))
    assert s2_resp.status_code == 201
    s2_url = client.location_of(s2_resp)
    s2_id = id_from_self_link(s2_url)
    cleanup(s2_url)

    # Verify original Sensor is linked
    ds_before = client.nav(ds_url, params={"$expand": "Sensor"})
    assert entity_id(ds_before["Sensor"]) == original_sensor_id

    # Re-link to Sensor 2
    patch_resp = client.patch(ds_url, json={"Sensor": {"@iot.id": s2_id}})
    assert patch_resp.status_code in (200, 204), (
        f"PATCH relation should return 200 or 204, got {patch_resp.status_code}: {patch_resp.text[:300]}"
    )

    # Verify relation changed
    ds_after = client.nav(ds_url, params={"$expand": "Sensor"})
    assert entity_id(ds_after["Sensor"]) == s2_id, (
        "Datastream must now reference the new Sensor after PATCH re-link"
    )


# ===========================================================================
# UPDATE 10 – PATCH non-existent entity → 404
#   req/create-update-delete/update-entity
# ===========================================================================

@pytest.mark.c02
def test_patch_nonexistent_thing(client):
    """req/create-update-delete/update-entity — PATCH non-existent entity → 404."""
    resp = client.patch("Things(999999999)", json={"name": "ghost"})
    assert resp.status_code == 404, (
        f"PATCH on non-existent Thing must return 404, got {resp.status_code}"
    )


@pytest.mark.c02
def test_patch_nonexistent_observation(client):
    """req/create-update-delete/update-entity — PATCH non-existent Observation → 404."""
    resp = client.patch("Observations(999999999)", json={"result": 0})
    assert resp.status_code == 404, (
        f"PATCH on non-existent Observation must return 404, got {resp.status_code}"
    )


# ===========================================================================
# UPDATE 11 – PUT full replacement
#   req/create-update-delete/update-entity-put  (18-088 §10.3)
#
# PUT is now DECLARED in serverSettings.conformance and IMPLEMENTED by istSOS4.
# All tests in this section are positive (no xfail).
# ===========================================================================

@pytest.mark.c02
def test_put_replace_thing(client, unique_name, cleanup):
    """req/create-update-delete/update-entity-put — PUT replaces an entity (full replace).

    18-088 §10.3: PUT shall perform a COMPLETE replacement of the entity with the
    provided body.  The server must return 200 or 204.  The entity's @iot.id and
    @iot.selfLink must be immutable (preserved across the replace).
    Optional properties omitted from the PUT body must be reset to null/absent
    (full replace, NOT merge — contrast with PATCH).

    Verified behaviours:
      - PUT /Things(<id>) with new name+description but WITHOUT 'properties'
      - Returns 200 or 204
      - GET after → name/description updated, properties null/absent (reset)
      - @iot.id and @iot.selfLink are unchanged (id immutable)
    """
    tag = unique_name("put")
    original_payload = {
        "name": f"{tag} original",
        "description": "original-description",
        "properties": {"key": "val", "extra": 123},
    }
    resp = client.create("Things", original_payload)
    assert resp.status_code == 201
    url = client.location_of(resp)
    cleanup(url)

    # Record id and selfLink before PUT
    before = client.nav(url)
    original_id = before["@iot.id"]
    original_self_link = before["@iot.selfLink"]

    # PUT: full replacement WITHOUT 'properties' field
    put_resp = client.put(
        url,
        json={"name": f"{tag} replaced", "description": "replaced-description"},
    )
    assert put_resp.status_code in (200, 204), (
        f"req/create-update-delete/update-entity-put: PUT should return 200 or 204; "
        f"got {put_resp.status_code}: {put_resp.text[:300]}"
    )

    after = client.nav(url)

    # Name and description must be updated
    assert after["name"] == f"{tag} replaced", (
        "PUT must update the 'name' property"
    )
    assert after["description"] == "replaced-description", (
        "PUT must update the 'description' property"
    )

    # Optional 'properties' not in PUT body → must be reset to null/absent (full replace)
    props = after.get("properties")
    assert props is None or props == {}, (
        "req/create-update-delete/update-entity-put: PUT full-replace must reset "
        f"'properties' to null/absent when not supplied; got {props!r}"
    )

    # @iot.id must be immutable across PUT
    assert after["@iot.id"] == original_id, (
        f"@iot.id must not change after PUT: was {original_id!r}, now {after['@iot.id']!r}"
    )
    # @iot.selfLink must be preserved (same absolute URL)
    assert after["@iot.selfLink"] == original_self_link, (
        f"@iot.selfLink must be preserved after PUT: "
        f"was {original_self_link!r}, now {after['@iot.selfLink']!r}"
    )


@pytest.mark.c02
def test_put_missing_mandatory_property_thing(client, unique_name, cleanup):
    """req/create-update-delete/update-entity-put — PUT Thing without mandatory 'name' → 400.

    18-088 §10.3: A PUT body missing a mandatory property must be rejected.
    The mandatory properties for Thing are: name, description.
    The server must return 400 (not 500, not 200).
    """
    tag = unique_name("put-mand-thing")
    resp = client.create(
        "Things",
        {"name": f"{tag} Thing", "description": "to-be-replaced"},
    )
    assert resp.status_code == 201
    url = client.location_of(resp)
    cleanup(url)

    # PUT WITHOUT mandatory 'name' → must be rejected
    bad_resp = client.put(url, json={"description": "no-name-put"})
    assert bad_resp.status_code == 400, (
        f"req/create-update-delete/update-entity-put: PUT Thing without 'name' must "
        f"return 400; got {bad_resp.status_code}: {bad_resp.text[:300]}"
    )
    # The error body must not be a server error (5xx content is unacceptable)
    assert bad_resp.status_code < 500, (
        f"Server must return a structured 4xx client error, not 5xx; got {bad_resp.status_code}"
    )


@pytest.mark.c02
def test_put_missing_mandatory_property_sensor(client, unique_name, cleanup):
    """req/create-update-delete/update-entity-put — PUT Sensor without mandatory 'metadata' → 400.

    18-088 §10.3 + §8.2.5: The mandatory properties for Sensor are:
    name, description, encodingType, metadata.
    PUT without 'metadata' must be rejected with 400.
    """
    tag = unique_name("put-mand-sensor")
    resp = client.create(
        "Sensors",
        sample_data.minimal_sensor(tag),
    )
    assert resp.status_code == 201
    url = client.location_of(resp)
    cleanup(url)

    # PUT WITHOUT mandatory 'metadata' → must be rejected
    bad_resp = client.put(
        url,
        json={
            "name": f"{tag} Sensor replaced",
            "description": "no-metadata",
            "encodingType": sample_data.SENSOR_PDF,
            # 'metadata' intentionally omitted
        },
    )
    assert bad_resp.status_code == 400, (
        f"req/create-update-delete/update-entity-put: PUT Sensor without 'metadata' must "
        f"return 400; got {bad_resp.status_code}: {bad_resp.text[:300]}"
    )


@pytest.mark.c02
def test_put_optional_property_reset(client, unique_name, cleanup):
    """req/create-update-delete/update-entity-put — PUT without optional property resets it.

    18-088 §10.3: PUT is COMPLETE replacement.  An optional property omitted from
    the PUT body must NOT be retained (unlike PATCH, which merges).
    This test specifically verifies that Thing.properties is reset to null/absent
    when a PUT body includes name+description but omits 'properties'.
    """
    tag = unique_name("put-opt-reset")
    resp = client.create(
        "Things",
        {
            "name": f"{tag} Thing",
            "description": "with-properties",
            "properties": {"foo": "bar", "count": 42},
        },
    )
    assert resp.status_code == 201
    url = client.location_of(resp)
    cleanup(url)

    # Confirm 'properties' is set before PUT
    before = client.nav(url)
    assert before.get("properties") not in (None, {}), (
        "Pre-condition: 'properties' must be set before PUT"
    )

    # PUT with only mandatory fields (no 'properties')
    put_resp = client.put(
        url,
        json={"name": f"{tag} Thing", "description": "without-properties"},
    )
    assert put_resp.status_code in (200, 204), (
        f"PUT must succeed; got {put_resp.status_code}: {put_resp.text[:300]}"
    )

    after = client.nav(url)
    props = after.get("properties")
    assert props is None or props == {}, (
        "req/create-update-delete/update-entity-put: PUT without 'properties' must reset "
        f"it to null/absent (full replace semantics); got {props!r}"
    )


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
