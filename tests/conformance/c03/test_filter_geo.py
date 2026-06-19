"""
test_c03_filter_geo.py -- c03 Filtering Extension: $filter GEOSPATIAL and
SPATIAL-RELATIONSHIP functions (OGC 18-088 Table 23,
req/request-data/built-in-query-functions).

built-in-query-functions (Req 31) IS now declared in istSOS4's
serverSettings.conformance; every geo / ST_* function below returns 200 live and
is asserted with the geometrically correct boolean for the seed geometry
(including geo.length on a LITERAL LINESTRING, which is now conformant):
  Location & auto FeatureOfInterest  = Point(-117.05, 51.05)
  SEED_CONTAINING_POLYGON_WKT        contains that point   (within/intersects TRUE)
  SEED_DISJOINT_POLYGON_WKT          does not              (disjoint TRUE)
  FAR_POINT_WKT (0,0)                far away              (distance large)

Each test is scoped by id (id eq <seed location/foi id>) so the boolean predicate
is the only variable: a true predicate -> the seed entity, false -> empty set.
"""

from __future__ import annotations

import pytest

import sample_data
from client import format_id

pytestmark = pytest.mark.c03

CONT = f"geography'{sample_data.SEED_CONTAINING_POLYGON_WKT}'"
DISJ = f"geography'{sample_data.SEED_DISJOINT_POLYGON_WKT}'"
FAR = f"geography'{sample_data.FAR_POINT_WKT}'"
PX, PY = sample_data.LOCATION_COORDINATES
SEED_PT = f"geography'POINT({PX} {PY})'"


def fetch(client, path, params=None) -> dict:
    r = client.get(path, params=params)
    assert r.status_code == 200, (
        f"GET {path} params={params} -> {r.status_code}: {r.text[:400]}"
    )
    return r.json()


def values(doc) -> list:
    return doc.get("value", [])


def ids_of(doc) -> list:
    return [e["@iot.id"] for e in values(doc)]


def loc_predicate(client, seed, pred) -> dict:
    """Seed Location iff the geo predicate is true (scoped by location id)."""
    return fetch(client, "Locations",
                 {"$filter": f"id eq {format_id(seed.location_id)} and {pred}"})


def foi_predicate(client, seed, pred) -> dict:
    assert seed.foi_ids, "seed produced no FeatureOfInterest"
    fid = seed.foi_ids[0]
    return fetch(client, "FeaturesOfInterest",
                 {"$filter": f"id eq {format_id(fid)} and {pred}"})


# ===========================================================================
# Geospatial functions  (geo.distance, geo.length, geo.intersects)
# ===========================================================================
def test_geo_intersects_positive(client, seed):
    """req/request-data/built-in-query-functions -- geo.intersects(Point,Polygon): the
    seed point lies inside the containing polygon."""
    assert ids_of(loc_predicate(client, seed, f"geo.intersects(location,{CONT})")) == [seed.location_id]


def test_geo_intersects_negative(client, seed):
    """req/request-data/built-in-query-functions -- geo.intersects with a disjoint polygon -> empty."""
    assert values(loc_predicate(client, seed, f"geo.intersects(location,{DISJ})")) == []


def test_geo_distance_far(client, seed):
    """req/request-data/built-in-query-functions -- geo.distance(Point,Point): the seed
    point is far from (0,0)."""
    assert ids_of(loc_predicate(client, seed, f"geo.distance(location,{FAR}) gt 1")) == [seed.location_id]


def test_geo_distance_self_zero(client, seed):
    """req/request-data/built-in-query-functions -- geo.distance to the identical point is 0."""
    assert ids_of(loc_predicate(client, seed, f"geo.distance(location,{SEED_PT}) lt 1")) == [seed.location_id]


def test_geo_length_point_is_zero(client, seed):
    """req/request-data/built-in-query-functions -- geo.length of a Point property is 0."""
    assert ids_of(loc_predicate(client, seed, "geo.length(location) lt 1")) == [seed.location_id]


def test_geo_length_literal_linestring(client, seed):
    """req/request-data/built-in-query-functions (now DECLARED) -- geo.length on a literal
    geography per the 18-088 Table 23 example (geography'LINESTRING(...)'). The line
    LINESTRING(0 0, 0 1) has length > 0, so the (always-true) predicate selects the
    id-scoped seed location."""
    doc = loc_predicate(client, seed, "geo.length(geography'LINESTRING(0 0, 0 1)') gt 0")
    assert ids_of(doc) == [seed.location_id]


# ===========================================================================
# Spatial-relationship functions on Location/location
# ===========================================================================
def test_st_within_positive(client, seed):
    """req/request-data/built-in-query-functions -- st_within(point, containing polygon)."""
    assert ids_of(loc_predicate(client, seed, f"st_within(location,{CONT})")) == [seed.location_id]


def test_st_within_negative(client, seed):
    """req/request-data/built-in-query-functions -- st_within(point, disjoint polygon) -> empty."""
    assert values(loc_predicate(client, seed, f"st_within(location,{DISJ})")) == []


def test_st_intersects(client, seed):
    """req/request-data/built-in-query-functions -- st_intersects(point, containing polygon)."""
    assert ids_of(loc_predicate(client, seed, f"st_intersects(location,{CONT})")) == [seed.location_id]


def test_st_disjoint_positive(client, seed):
    """req/request-data/built-in-query-functions -- st_disjoint(point, far polygon)."""
    assert ids_of(loc_predicate(client, seed, f"st_disjoint(location,{DISJ})")) == [seed.location_id]


def test_st_disjoint_negative(client, seed):
    """req/request-data/built-in-query-functions -- st_disjoint with the containing polygon -> empty."""
    assert values(loc_predicate(client, seed, f"st_disjoint(location,{CONT})")) == []


def test_st_equals_positive(client, seed):
    """req/request-data/built-in-query-functions -- st_equals(point, identical point)."""
    assert ids_of(loc_predicate(client, seed, f"st_equals(location,{SEED_PT})")) == [seed.location_id]


def test_st_equals_negative(client, seed):
    """req/request-data/built-in-query-functions -- st_equals with a different point -> empty."""
    assert values(loc_predicate(client, seed, f"st_equals(location,{FAR})")) == []


def test_st_contains(client, seed):
    """req/request-data/built-in-query-functions -- st_contains(prop, geography) per Table 23
    example order; a point contains the identical point."""
    assert ids_of(loc_predicate(client, seed, f"st_contains(location,{SEED_PT})")) == [seed.location_id]


def test_st_touches_false(client, seed):
    """req/request-data/built-in-query-functions -- st_touches: an interior point does not
    touch the polygon boundary -> correctly false (empty)."""
    assert values(loc_predicate(client, seed, f"st_touches(location,{CONT})")) == []


def test_st_overlaps_false(client, seed):
    """req/request-data/built-in-query-functions -- st_overlaps: a point cannot overlap a
    polygon (different dimensions) -> correctly false (empty)."""
    assert values(loc_predicate(client, seed, f"st_overlaps(location,{CONT})")) == []


def test_st_crosses_false(client, seed):
    """req/request-data/built-in-query-functions -- st_crosses: a point does not cross a
    polygon -> correctly false (empty)."""
    assert values(loc_predicate(client, seed, f"st_crosses(location,{CONT})")) == []


def test_st_relate_positive(client, seed):
    """req/request-data/built-in-query-functions -- st_relate with a DE-9IM intersection
    pattern; two identical points' interiors intersect -> matches."""
    assert ids_of(loc_predicate(client, seed, f"st_relate(location,{SEED_PT},'T********')")) == [seed.location_id]


def test_st_relate_negative(client, seed):
    """req/request-data/built-in-query-functions -- st_relate intersection pattern against a
    far point -> no match (empty)."""
    assert values(loc_predicate(client, seed, f"st_relate(location,{FAR},'T********')")) == []


# ===========================================================================
# Geo functions on FeatureOfInterest/feature
# ===========================================================================
def test_geo_intersects_feature_positive(client, seed):
    """req/request-data/built-in-query-functions -- geo.intersects on FeatureOfInterest/feature
    (the auto FoI shares the seed point geometry)."""
    fid = seed.foi_ids[0]
    assert ids_of(foi_predicate(client, seed, f"geo.intersects(feature,{CONT})")) == [fid]


def test_st_within_feature_negative(client, seed):
    """req/request-data/built-in-query-functions -- st_within on FoI/feature with a disjoint
    polygon -> empty."""
    assert values(foi_predicate(client, seed, f"st_within(feature,{DISJ})")) == []


def test_st_equals_feature_positive(client, seed):
    """req/request-data/built-in-query-functions -- st_equals on FoI/feature (identical point)."""
    fid = seed.foi_ids[0]
    assert ids_of(foi_predicate(client, seed, f"st_equals(feature,{SEED_PT})")) == [fid]
