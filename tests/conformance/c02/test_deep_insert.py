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

import concurrent.futures
import threading

import pytest

import sample_data
from client import STAClient, entity_id, format_id, id_from_self_link
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
# CREATE 2b – 3-level deep-insert: nested Sensor / ObservedProperty FIELD fidelity
#   req/create-update-delete/deep-insert (18-088 §10.2.1.3)
# ===========================================================================

@pytest.mark.c02
def test_deep_insert_nested_sensor_fields(client, unique_name, cleanup):
    """req/create-update-delete/deep-insert — a Sensor (and ObservedProperty)
    nested three levels deep (Thing → Datastream → Sensor) must be created with
    ALL its fields intact and reachable via /Datastreams(<id>)/Sensor.

    Regression: Sensor.metadata (a VARCHAR column) used to be json.dumps()-ed on
    create, so a string link "Light flux sensor" was persisted — and read back —
    as the double-encoded '"Light flux sensor"'. The reference service returns
    the raw value; the OGC TEAM Engine deep-insert Sensor check flagged the
    mismatch. Covers a Sensor WITH and one WITHOUT a ``properties`` field.
    """
    tag = unique_name("d3")
    sensor_with = {
        "name": f"{tag} sensor-with",
        "description": "sensor with properties",
        "encodingType": sample_data.SENSOR_PDF,
        "metadata": "Light flux sensor",
        "properties": {"reference": "firstSensor"},
    }
    sensor_without = {
        "name": f"{tag} Acme Fluxomatic 1000",
        "description": "Acme Fluxomatic 1000",
        "encodingType": sample_data.SENSOR_PDF,
        "metadata": "Light flux sensor",
        # NO properties — reference reads this back as properties: null
    }
    op_with = {
        "name": f"{tag} Luminous Flux",
        "definition": "https://example.org/def/luminous-flux",
        "description": "observedProperty 1",
        "properties": {"reference": "firstObservedProperty"},
    }
    op_without = {
        "name": f"{tag} Tempretaure",
        "definition": "https://example.org/def/temperature",
        "description": "observedProperty 2",
    }

    def _ds(name, sensor, op):
        return {
            "name": f"{tag} {name}",
            "description": name,
            "unitOfMeasurement": sample_data.unit_lumen(),
            "observationType": sample_data.OM_MEASUREMENT,
            "Sensor": sensor,
            "ObservedProperty": op,
        }

    payload = {
        **sample_data.minimal_thing(tag),
        "Locations": [sample_data.minimal_location(tag)],
        "Datastreams": [
            _ds("ds-with", sensor_with, op_with),
            _ds("ds-without", sensor_without, op_without),
        ],
    }

    resp = client.create("Things", payload)
    assert resp.status_code == 201, (
        f"3-level deep insert should be 201, got {resp.status_code}: {resp.text[:400]}"
    )
    thing_url = client.location_of(resp)
    thing_id = id_from_self_link(thing_url)

    thing = client.nav(thing_url, params={"$expand": "Locations"})
    cleanup(thing_url)
    cleanup(f"{client.base_url}/Locations({format_id(entity_id(thing['Locations'][0]))})")

    # Map datastreams by name so the assertions are order-independent.
    ds_docs = client.nav(f"{thing_url}/Datastreams")["value"]
    assert len(ds_docs) == 2, f"expected 2 Datastreams, got {len(ds_docs)}"
    by_name = {d["name"]: d for d in ds_docs}

    expectations = [
        (f"{tag} ds-with", sensor_with, op_with, {"reference": "firstSensor"}),
        (f"{tag} ds-without", sensor_without, op_without, None),
    ]
    for ds_name, exp_sensor, exp_op, exp_sensor_props in expectations:
        ds = by_name[ds_name]
        ds_id = entity_id(ds)

        # --- Sensor: every field must round-trip via the navigation link ---
        s_resp = client.get(f"Datastreams({format_id(ds_id)})/Sensor")
        assert s_resp.status_code == 200, (
            f"GET Datastreams({ds_id})/Sensor failed: {s_resp.status_code}"
        )
        sensor = s_resp.json()
        cleanup(f"{client.base_url}/Sensors({format_id(entity_id(sensor))})")

        assert sensor["name"] == exp_sensor["name"], sensor
        assert sensor["description"] == exp_sensor["description"], sensor
        assert sensor["encodingType"] == exp_sensor["encodingType"], sensor
        # the regression: raw value, NOT double-encoded ('"Light flux sensor"')
        assert sensor["metadata"] == exp_sensor["metadata"], (
            f"nested Sensor.metadata must round-trip verbatim; "
            f"sent {exp_sensor['metadata']!r}, got {sensor['metadata']!r}"
        )
        assert sensor.get("properties") == exp_sensor_props, (
            f"nested Sensor.properties mismatch: {sensor.get('properties')!r}"
        )

        # --- ObservedProperty: fields round-trip too ---
        op_resp = client.get(f"Datastreams({format_id(ds_id)})/ObservedProperty")
        assert op_resp.status_code == 200
        op = op_resp.json()
        cleanup(f"{client.base_url}/ObservedProperties({format_id(entity_id(op))})")
        assert op["name"] == exp_op["name"], op
        assert op["definition"] == exp_op["definition"], op
        assert op["description"] == exp_op["description"], op


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
# CREATE 5b – concurrent FeatureOfInterest auto-generation (find-or-create race)
#   18-088 §10.2.2.3
# ===========================================================================

@pytest.mark.c02
def test_foi_auto_generation_concurrent(client, base_url, unique_name, cleanup):
    """18-088 §10.2.2.3 — concurrent Observation inserts for one Thing/Location
    must resolve to a SINGLE auto-generated FeatureOfInterest.

    Regression for a find-or-create (TOCTOU) race in the auto-FoI path: N
    Observations POSTed simultaneously without an explicit FeatureOfInterest for
    the same Datastream/Location all observe "no FoI yet" and would each create
    one. Without server-side serialization this yields duplicate FeaturesOfInterest
    or — when the duplicates constraint (unique_featuresOfInterest_name) is
    enabled — a unique violation that surfaces as HTTP 500. The §10.2.2.3 auto-FoI
    contract requires every such Observation to link the SAME FoI, so all N inserts
    must return 201 and resolve to exactly one FeatureOfInterest.

    Each Observation carries a DISTINCT phenomenonTime so the only contended
    resource is the auto-FoI itself (not the Observation phenomenonTime/Datastream
    uniqueness), keeping the assertion valid whether or not duplicates are allowed.
    """
    tree = _create_datastream_tree(client, unique_name, cleanup)
    ds_id = tree["ds_id"]

    n = 12
    # Barrier so all N POSTs are released at the same instant -> maximal overlap
    # in the gen_foi_id-IS-NULL window. timeout guards against a hung request.
    barrier = threading.Barrier(n, timeout=60)

    def post_one(i: int):
        # Independent client per thread (mirrors N independent API consumers).
        c = STAClient(base_url=base_url)
        try:
            payload = {
                "phenomenonTime": f"2024-05-{i + 1:02d}T00:00:00Z",
                "result": float(i),
            }
            barrier.wait()
            return c.post(
                f"Datastreams({format_id(ds_id)})/Observations", json=payload
            )
        finally:
            c.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=n) as ex:
        responses = list(ex.map(post_one, range(n)))

    # 1. Every concurrent insert succeeds: no 500, no unique-violation fallout.
    for i, resp in enumerate(responses):
        assert resp.status_code == 201, (
            f"concurrent Observation {i} expected 201, got "
            f"{resp.status_code}: {resp.text[:300]}"
        )
        loc_hdr = resp.headers.get("location", "")
        assert loc_hdr.startswith("http"), (
            f"concurrent Observation {i} missing Location header: {loc_hdr!r}"
        )
        cleanup(loc_hdr)

    # 2. All N Observations resolve to exactly ONE FeatureOfInterest (no
    #    duplicate-FoI explosion, consistent auto-FoI).
    obs_docs = client.nav(
        f"Datastreams({format_id(ds_id)})/Observations",
        params={"$expand": "FeatureOfInterest", "$top": str(n + 5)},
    )["value"]
    assert len(obs_docs) == n, (
        f"expected {n} Observations on the Datastream, got {len(obs_docs)}"
    )
    foi_ids = {
        entity_id(o["FeatureOfInterest"])
        for o in obs_docs
        if o.get("FeatureOfInterest")
    }
    assert len(foi_ids) == 1, (
        "concurrent auto-FoI generation must resolve to a single "
        f"FeatureOfInterest; got {sorted(foi_ids)}"
    )
    cleanup(f"{base_url}/FeaturesOfInterest({format_id(next(iter(foi_ids)))})")


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
