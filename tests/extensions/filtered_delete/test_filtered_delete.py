"""
test_filtered_delete.py -- OGC SensorThings API v1.1 / istSOS4 "FROST Filtered
Delete" extension conformance.

FROST FilteredDelete extension (DELETE /Observations?$filter=<expr>):
  * Bulk-deletes EXACTLY the Observations a GET /Observations?$filter=<expr>
    would return.  Success -> HTTP 200 with body {"deleted": <N>} (N = number
    deleted; an empty match set is still success -> 200 {"deleted": 0}).
  * After deleting, for each touched Datastream it RECOMPUTES phenomenonTime AND
    observedArea from the REMAINING Observations.
  * Safeguards (istSOS4 NEVER does an unbounded collection delete):
      - missing $filter  -> 400 {"...","message":"$filter is required for
        collection delete"} (never deletes the whole collection);
      - malformed $filter -> 400 (never 500).
  * The single-entity DELETE /Observations(<id>) route is separate and intact.

CRITICAL SAFETY -- the filtered delete operates over the WHOLE /Observations
collection and the shared DB is NOT empty (tests also run under `-n auto`).  So
EVERY test builds its OWN isolated subtree (Thing -> Location + Datastream(+
Sensor + ObservedProperty), uniquely tagged) and SCOPES every delete/GET filter
to that Datastream, e.g. `Datastream/@iot.id eq <D> and (<predicate>)`.  This
keeps counts exact, prevents collateral deletion, and is itself a valid
non-trivial cross-entity + `and` filter case.  All ids are resolved at runtime
(never hard-coded) and every created subtree is torn down (Thing delete cascades
its Datastream + remaining Observations; 404s tolerated).
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

import sample_data
from client import entity_id, format_id, id_from_self_link

pytestmark = pytest.mark.filtered_delete


# ===========================================================================
# Isolated-subtree scaffolding (single-file ownership)
# ===========================================================================
@dataclass
class SubTree:
    """One self-contained subtree created by a test: Thing -> Location +
    Datastream(+ Sensor + ObservedProperty).  Ids resolved at runtime."""
    tag: str
    thing_id: object
    location_id: object
    ds_id: object
    sensor_id: object
    op_id: object
    foi_ids: list = field(default_factory=list)  # explicitly-created FoIs to clean up


def _build_subtree(client, tag: str) -> SubTree:
    """Deep-insert a fresh Thing with one Location and one Datastream (inline
    Sensor + ObservedProperty), then read every id back by navigating the tree
    (id-type-agnostic).  No Observations yet -- each test seeds its own."""
    payload = {
        "name": f"{tag} Thing",
        "description": "filtered-delete subtree",
        "properties": {"tag": tag},
        "Locations": [sample_data.minimal_location(tag)],
        "Datastreams": [
            {
                "name": f"{tag} Datastream",
                "description": "filtered-delete datastream",
                "unitOfMeasurement": sample_data.unit_lumen(),
                "observationType": sample_data.OM_MEASUREMENT,
                "Sensor": sample_data.minimal_sensor(tag),
                "ObservedProperty": sample_data.minimal_observed_property(tag),
            }
        ],
    }
    resp = client.create("Things", payload)
    assert resp.status_code == 201, (
        f"subtree deep-insert failed: {resp.status_code} {resp.text[:400]}"
    )
    thing_url = client.location_of(resp)
    thing = client.nav(thing_url)
    thing_id = entity_id(thing)

    locations = client.nav(f"{thing_url}/Locations")["value"]
    assert locations, "subtree Thing has no Location"
    location_id = entity_id(locations[0])

    ds_docs = client.nav(
        f"{thing_url}/Datastreams",
        params={"$expand": "Sensor,ObservedProperty"},
    )["value"]
    assert len(ds_docs) == 1, f"expected 1 subtree Datastream, got {len(ds_docs)}"
    ds = ds_docs[0]

    return SubTree(
        tag=tag,
        thing_id=thing_id,
        location_id=location_id,
        ds_id=entity_id(ds),
        sensor_id=entity_id(ds["Sensor"]),
        op_id=entity_id(ds["ObservedProperty"]),
    )


def _cleanup(client, st: SubTree) -> None:
    """Best-effort teardown.  Deleting the Thing cascades its Datastream and any
    remaining Observations; Location/Sensor/ObservedProperty and explicitly
    created FeaturesOfInterest are independent -> delete them too.  404s are fine."""
    def _safe(path):
        try:
            client.delete(path)
        except Exception:
            pass

    _safe(f"Things({format_id(st.thing_id)})")
    _safe(f"Locations({format_id(st.location_id)})")
    _safe(f"Sensors({format_id(st.sensor_id)})")
    _safe(f"ObservedProperties({format_id(st.op_id)})")
    for fid in st.foi_ids:
        _safe(f"FeaturesOfInterest({format_id(fid)})")


@pytest.fixture
def subtree(client, unique_name):
    """Factory: call subtree() to get a fresh isolated SubTree.  Every subtree
    created during the test is cleaned up on teardown."""
    created: list[SubTree] = []

    def _make() -> SubTree:
        st = _build_subtree(client, unique_name("fdel"))
        created.append(st)
        return st

    yield _make

    for st in created:
        _cleanup(client, st)


# ===========================================================================
# helpers -- creation, scoped queries, geometry bbox
# ===========================================================================
def _post_obs(client, ds_id, result, time, foi_id=None):
    """POST one Observation onto ds_id (optionally linked to an existing FoI).
    Returns its @iot.id."""
    payload = {
        "phenomenonTime": time,
        "result": result,
        "Datastream": {"@iot.id": ds_id},
    }
    if foi_id is not None:
        payload["FeatureOfInterest"] = {"@iot.id": foi_id}
    resp = client.post("Observations", json=payload)
    assert resp.status_code == 201, (
        f"POST Observation -> {resp.status_code}: {resp.text[:300]}"
    )
    return id_from_self_link(client.location_of(resp))


def _post_foi(client, st: SubTree, lon, lat):
    """POST a FeatureOfInterest at Point(lon, lat); track it for cleanup.
    Returns its @iot.id (used to give an Observation a controlled geometry)."""
    payload = {
        "name": f"{st.tag} FoI {lon},{lat}",
        "description": "filtered-delete foi",
        "encodingType": sample_data.GEOJSON,
        "feature": {"type": "Point", "coordinates": [lon, lat]},
    }
    resp = client.post("FeaturesOfInterest", json=payload)
    assert resp.status_code == 201, (
        f"POST FeatureOfInterest -> {resp.status_code}: {resp.text[:300]}"
    )
    fid = id_from_self_link(client.location_of(resp))
    st.foi_ids.append(fid)
    return fid


def _scoped(ds_id, predicate=None) -> str:
    """Build a $filter ALWAYS scoped to ds_id (cross-entity + `and`), so a delete
    can only ever touch this test's own Observations."""
    base = f"Datastream/@iot.id eq {format_id(ds_id)}"
    return base if predicate is None else f"{base} and ({predicate})"


def _filtered_delete(client, ds_id, predicate):
    """DELETE /Observations?$filter=<scoped predicate>."""
    return client.delete("Observations", params={"$filter": _scoped(ds_id, predicate)})


def _obs_results(client, ds_id):
    """Sorted result values currently on ds_id's Observations (scoped via the
    navigation path)."""
    body = client.get(
        f"Datastreams({format_id(ds_id)})/Observations",
        params={"$top": 100000, "$select": "result"},
    ).json()
    return sorted(float(v["result"]) for v in body["value"])


def _obs_ids(client, ds_id, predicate=None):
    """Set of Observation @iot.ids matching a scoped predicate on ds_id, via
    GET /Observations?$filter=...&$select=@iot.id.  This is the GET 'oracle'."""
    body = client.get(
        "Observations",
        params={
            "$filter": _scoped(ds_id, predicate),
            "$select": "@iot.id",
            "$top": 100000,
        },
    ).json()
    return {o["@iot.id"] for o in body["value"]}


def _iter_coords(node):
    """Yield every leaf (x, y) coordinate pair in a GeoJSON coordinates tree
    (handles Point, Polygon, MultiPolygon, ... uniformly)."""
    if (
        isinstance(node, list)
        and len(node) >= 2
        and isinstance(node[0], (int, float))
        and not isinstance(node[0], bool)
        and isinstance(node[1], (int, float))
        and not isinstance(node[1], bool)
    ):
        yield (float(node[0]), float(node[1]))
    elif isinstance(node, list):
        for child in node:
            yield from _iter_coords(child)


def _bbox(geometry):
    """(minx, miny, maxx, maxy) of a GeoJSON geometry's coordinates."""
    pts = list(_iter_coords(geometry["coordinates"]))
    assert pts, f"geometry has no coordinates: {geometry}"
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    return (min(xs), min(ys), max(xs), max(ys))


# ===========================================================================
# 1. Base filtered delete -- 200 + {"deleted": N}; only non-matched remain
# ===========================================================================
def test_base_filtered_delete(client, subtree):
    """FROST FilteredDelete extension -- DELETE /Observations?$filter scoped to
    the test's own Datastream deletes EXACTLY the matching Observations: status
    200 with body {"deleted": N} where N == number created with result < X; a
    follow-up scoped GET confirms ONLY the non-matched (result >= X) remain."""
    st = subtree()
    for i, res in enumerate([1, 2, 3, 4, 5, 6], start=1):
        _post_obs(client, st.ds_id, res, f"2021-01-{i:02d}T00:00:00Z")

    # result < 4 -> {1, 2, 3} : three Observations must be deleted.
    resp = _filtered_delete(client, st.ds_id, "result lt 4")
    assert resp.status_code == 200, f"-> {resp.status_code}: {resp.text[:300]}"
    assert resp.json() == {"deleted": 3}, resp.text[:300]

    # Only the non-matched (result >= 4) remain on our datastream.
    assert _obs_results(client, st.ds_id) == [4.0, 5.0, 6.0]


# ===========================================================================
# 2. Deleted set == GET set (non-trivial `or` predicate, still scoped)
# ===========================================================================
def test_deleted_set_equals_get_set(client, subtree):
    """FROST FilteredDelete extension -- the deleted set is EXACTLY the GET set.
    Capture the oracle id set via GET .../$filter=<scoped predicate>&$select=
    @iot.id BEFORE deleting; DELETE the same predicate; assert precisely those
    ids are gone and no others on our datastream (deleted set === GET set)."""
    st = subtree()
    for i, res in enumerate([1, 2, 3, 4, 5, 6, 7, 8], start=1):
        _post_obs(client, st.ds_id, res, f"2022-02-{i:02d}T00:00:00Z")

    predicate = "result lt 3 or result gt 6"  # -> {1, 2, 7, 8}
    oracle = _obs_ids(client, st.ds_id, predicate)
    all_before = _obs_ids(client, st.ds_id)
    assert len(oracle) == 4
    assert len(all_before) == 8
    assert oracle < all_before

    resp = _filtered_delete(client, st.ds_id, predicate)
    assert resp.status_code == 200, f"-> {resp.status_code}: {resp.text[:300]}"
    assert resp.json() == {"deleted": len(oracle)}, resp.text[:300]

    remaining = _obs_ids(client, st.ds_id)
    # exactly the oracle gone, nothing else touched.
    assert remaining == all_before - oracle
    assert remaining.isdisjoint(oracle)
    assert _obs_results(client, st.ds_id) == [3.0, 4.0, 5.0, 6.0]


# ===========================================================================
# 3. MAINTENANCE (key test) -- phenomenonTime AND observedArea recomputed
# ===========================================================================
def test_maintenance_recompute_phenomenontime_and_observedarea(client, subtree):
    """FROST FilteredDelete extension -- after a filtered delete the touched
    Datastream's phenomenonTime AND observedArea are RECOMPUTED from the
    REMAINING Observations.  Seed the to-be-deleted ones (result < 10) with BOTH
    the earliest phenomenonTime (year 2000) AND outlier FeatureOfInterest points
    (far from the cluster); the kept ones (result >= 10) with a later, tight
    time/geometry cluster (year 2020).  Record phenomenonTime + observedArea
    BEFORE, DELETE the matched (scoped), then assert AFTER that the
    phenomenonTime start moved FORWARD and the observedArea bbox SHRANK -- i.e.
    neither still spans the deleted Observations."""
    st = subtree()

    # (result, phenomenonTime, FoI lon, FoI lat)
    outliers = [
        (1, "2000-01-01T00:00:00Z", -80.0, -60.0),
        (2, "2000-01-02T00:00:00Z", 80.0, 70.0),
    ]
    cluster = [
        (10, "2020-06-01T00:00:00Z", 10.0, 45.0),
        (11, "2020-06-02T00:00:00Z", 10.4, 45.0),
        (12, "2020-06-03T00:00:00Z", 10.2, 45.4),
    ]
    for res, t, lon, lat in outliers + cluster:
        fid = _post_foi(client, st, lon, lat)
        _post_obs(client, st.ds_id, res, t, foi_id=fid)

    def _ds_state():
        doc = client.get(
            f"Datastreams({format_id(st.ds_id)})",
            params={"$select": "phenomenonTime,observedArea"},
        ).json()
        return doc

    before = _ds_state()
    before_start = before["phenomenonTime"].split("/")[0]
    before_bbox = _bbox(before["observedArea"])

    # Delete the outliers (earliest time + far geometry) only.
    resp = _filtered_delete(client, st.ds_id, "result lt 10")
    assert resp.status_code == 200, f"-> {resp.status_code}: {resp.text[:300]}"
    assert resp.json() == {"deleted": 2}, resp.text[:300]
    assert _obs_results(client, st.ds_id) == [10.0, 11.0, 12.0]

    after = _ds_state()
    after_start = after["phenomenonTime"].split("/")[0]
    after_bbox = _bbox(after["observedArea"])

    # --- phenomenonTime start moved FORWARD (no longer spans the 2000 obs) ---
    assert before_start[:4] == "2000", f"before start should be 2000: {before_start}"
    assert after_start[:4] == "2020", f"after start should be 2020: {after_start}"
    assert after_start[:10] > before_start[:10], (before_start, after_start)

    # --- observedArea bbox SHRANK in BOTH dimensions, recomputed from cluster ---
    bminx, bminy, bmaxx, bmaxy = before_bbox
    aminx, aminy, amaxx, amaxy = after_bbox
    assert (amaxx - aminx) < (bmaxx - bminx), (before_bbox, after_bbox)
    assert (amaxy - aminy) < (bmaxy - bminy), (before_bbox, after_bbox)
    # after bbox lies strictly inside before bbox.
    assert bminx <= aminx and amaxx <= bmaxx, (before_bbox, after_bbox)
    assert bminy <= aminy and amaxy <= bmaxy, (before_bbox, after_bbox)
    # BEFORE clearly spanned the far outliers ...
    assert bminx <= -79.0 and bmaxx >= 79.0, before_bbox
    assert bminy <= -59.0 and bmaxy >= 69.0, before_bbox
    # ... AFTER is the tight cluster only and no longer reaches them.
    assert 9.0 <= aminx and amaxx <= 11.0, after_bbox
    assert 44.0 <= aminy and amaxy <= 46.0, after_bbox


# ===========================================================================
# 4. No-filter safeguard -- 400 + exact message, NOTHING deleted
# ===========================================================================
def test_no_filter_safeguard(client, subtree):
    """FROST FilteredDelete extension -- DELETE /Observations with NO $filter is
    rejected with 400 and body message exactly '$filter is required for
    collection delete' (istSOS4 never does an unbounded collection delete); a
    scoped GET confirms our datastream's Observations are untouched."""
    st = subtree()
    for i, res in enumerate([1, 2, 3], start=1):
        _post_obs(client, st.ds_id, res, f"2023-03-{i:02d}T00:00:00Z")

    resp = client.delete("Observations")
    assert resp.status_code == 400, f"-> {resp.status_code}: {resp.text[:300]}"
    assert resp.json()["message"] == "$filter is required for collection delete", (
        resp.text[:300]
    )

    # Nothing was deleted.
    assert _obs_results(client, st.ds_id) == [1.0, 2.0, 3.0]


# ===========================================================================
# 5. Malformed $filter -- 400 (never 500), NOTHING deleted
# ===========================================================================
def test_malformed_filter_returns_400(client, subtree):
    """FROST FilteredDelete extension -- a malformed $filter (unknown operator
    'zz') is rejected with a clean 400, NOT a 500; a scoped GET confirms our
    datastream's Observations are untouched."""
    st = subtree()
    for i, res in enumerate([1, 2, 3], start=1):
        _post_obs(client, st.ds_id, res, f"2024-04-{i:02d}T00:00:00Z")

    # Scoped AND malformed: even if it somehow parsed, it could only ever touch
    # our own datastream -- but it must be rejected at parse time with 400.
    resp = client.delete(
        "Observations",
        params={"$filter": f"Datastream/@iot.id eq {format_id(st.ds_id)} and result zz 5"},
    )
    assert resp.status_code == 400, (
        f"malformed $filter must be 400 (not {resp.status_code}): {resp.text[:300]}"
    )
    assert resp.status_code != 500

    # Nothing was deleted.
    assert _obs_results(client, st.ds_id) == [1.0, 2.0, 3.0]


# ===========================================================================
# 6. Single-entity DELETE /Observations(<id>) intact alongside the collection route
# ===========================================================================
def test_single_entity_delete_still_works(client, subtree):
    """FROST FilteredDelete extension -- the per-id DELETE /Observations(<id>)
    route is unaffected by the collection route: deleting one Observation by id
    succeeds (200/204) and a follow-up proves it is gone (GET -> 404, and a
    re-DELETE of the same id -> 404)."""
    st = subtree()
    oid = _post_obs(client, st.ds_id, 42, "2025-05-01T00:00:00Z")

    resp = client.delete(f"Observations({format_id(oid)})")
    assert resp.status_code in (200, 204), f"-> {resp.status_code}: {resp.text[:300]}"

    # Confirm it is gone.
    got = client.get(f"Observations({format_id(oid)})")
    assert got.status_code == 404, f"deleted obs should 404, got {got.status_code}"

    # The per-id route reports a now-missing id as 404 (route still live).
    again = client.delete(f"Observations({format_id(oid)})")
    assert again.status_code == 404, f"re-delete should 404, got {again.status_code}"


# ===========================================================================
# 7. (optional) No-match delete -- 200 {"deleted": 0}, nothing removed
# ===========================================================================
def test_no_match_delete_returns_zero(client, subtree):
    """FROST FilteredDelete extension -- a scoped filter that matches nothing is
    still success: 200 with body {"deleted": 0}; a scoped GET confirms all of our
    datastream's Observations are still present."""
    st = subtree()
    for i, res in enumerate([1, 2, 3], start=1):
        _post_obs(client, st.ds_id, res, f"2026-06-{i:02d}T00:00:00Z")

    resp = _filtered_delete(client, st.ds_id, "result gt 100000")
    assert resp.status_code == 200, f"-> {resp.status_code}: {resp.text[:300]}"
    assert resp.json() == {"deleted": 0}, resp.text[:300]

    assert _obs_results(client, st.ds_id) == [1.0, 2.0, 3.0]
