"""
sample_data.py -- canonical STA payloads + the compliance seed tree.

The read-only `seed` fixture loads EXACTLY the OGC TeamEngine compliance init
dataset `entitiesDefault.json` (repo root) via one deep-insert request. We load
that file verbatim (no invented data; the intentional typos "Tempretaure" /
"Tempreture" are preserved) so the expected result-sets below are authoritative.

==============================================================================
SEED TREE (entitiesDefault.json) -- one self-contained subtree:

  Thing "thing name 1"  (properties.reference = "firstThing")
   +-- Location "location name 1"  Point(-117.05, 51.05)  application/vnd.geo+json
   +-- Datastream "datastream name 1"  unit Lumen (lm)   [DS1]
   |     +-- ObservedProperty "Luminous Flux"
   |     +-- Sensor "sensor name 1"  (application/pdf, metadata "Light flux sensor")
   |     +-- Observation result 3 @ 2015-03-03T00:00:00Z
   |     +-- Observation result 4 @ 2015-03-04T00:00:00Z
   +-- Datastream "datastream name 2"  unit Centigrade (C)   [DS2]
         +-- ObservedProperty "Tempretaure"   (sic)
         +-- Sensor "sensor name 2"  (application/pdf, metadata "Tempreture sensor")
         +-- Observation result 5 @ 2015-03-05T00:00:00Z
         +-- Observation result 6 @ 2015-03-06T00:00:00Z

A FeatureOfInterest is auto-generated from the Thing's Location for the
Observations (POSTed without an explicit FoI).

==============================================================================
DETERMINISTIC EXPECTATIONS (the backbone of the c03 result-set assertions).

The local database is NOT empty, so ALWAYS scope to the seed subtree:
  * per datastream:  GET /Datastreams(<DS1>)/Observations?$filter=...
  * across the Thing: GET /Observations?$filter=Datastream/Thing/@iot.id eq <thing> and ...
  * collection by name: combine with the seed names (unique within the subtree).
Read every id from the `seed` fixture at runtime; never hard-code ids.

Observations:
  DS1 -> results [3, 4]  times 2015-03-03, 2015-03-04
  DS2 -> results [5, 6]  times 2015-03-05, 2015-03-06
  Thing (all four)       results [3, 4, 5, 6]  times 2015-03-03..06

Comparison ($filter on result, scoped to the Thing's 4 observations):
  result eq 4              -> [4]
  result ne 4              -> [3, 5, 6]
  result gt 4              -> [5, 6]
  result ge 4              -> [4, 5, 6]
  result lt 5              -> [3, 4]
  result le 4              -> [3, 4]

Logical (scoped to the Thing):
  result gt 3 and result lt 6      -> [4, 5]
  result lt 4 or result gt 5       -> [3, 6]
  not (result gt 4)                -> [3, 4]

Arithmetic (scoped to the Thing):
  result add 1 eq 5                -> [4]
  result sub 1 eq 4                -> [5]
  result mul 2 eq 8                -> [4]
  result div 2 eq 2                -> [4]   (integer/numeric division: 4/2=2)
  result mod 2 eq 0                -> [4, 6]

Math (results are integers, so round/floor/ceiling are identity -- mirrors FROST):
  round(result) eq 4               -> [4]
  floor(result) eq 4               -> [4]
  ceiling(result) eq 4             -> [4]

Datetime (phenomenonTime; all four share year=2015, month=3, hour=0):
  year(phenomenonTime) eq 2015     -> [3, 4, 5, 6]
  month(phenomenonTime) eq 3       -> [3, 4, 5, 6]
  day(phenomenonTime) eq 3         -> [3]    (only 2015-03-03)
  day(phenomenonTime) ge 5         -> [5, 6]
  phenomenonTime gt 2015-03-04T12:00:00Z -> [5, 6]

String (on entity names; scope by navigation/relation):
  startswith(name,'datastream')  on /Datastreams scoped to Thing -> both DS
  endswith(name,'name 1')        -> "datastream name 1"
  length(name) eq 17             -> "datastream name 1"/"datastream name 2" (len 17)
  tolower(name) eq 'datastream name 1'
  NOTE: substringof is implemented-but-broken in istSOS4 (xfail); `contains`
  is an OData-4.01 alias not in 18-088 Table 23 (400 Unknown function, xfail).

Ordering / paging (scoped to the Thing's 4 observations):
  $orderby=result desc           -> [6, 5, 4, 3]
  $orderby=phenomenonTime asc    -> [3, 4, 5, 6]
  $top=2&$orderby=result asc     -> [3, 4] + @iot.nextLink
  $count=true                    -> @iot.count == 4 (Thing) / 2 (per datastream)

Geometry (Location & auto FeatureOfInterest at Point(-117.05, 51.05)):
  CONTAINING polygon -> intersects/within TRUE ; DISJOINT polygon -> FALSE.
  geo.distance to FAR_POINT(0,0) is large and positive.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

# --------------------------------------------------------------------------
# The canonical compliance dataset is loaded verbatim from entitiesDefault.json.
# --------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parents[2]
ENTITIES_DEFAULT_PATH = Path(
    os.environ.get("STA_SEED_FILE", _REPO_ROOT / "entitiesDefault.json")
)


def deep_insert_tree() -> dict:
    """The exact entitiesDefault.json deep-insert payload (a Thing with 1
    Location and 2 Datastreams, each with inline Sensor, ObservedProperty and 2
    Observations). Loaded verbatim -- intentional typos preserved."""
    with open(ENTITIES_DEFAULT_PATH, encoding="utf-8") as fh:
        return json.load(fh)


# --------------------------------------------------------------------------
# Documented expected values (mirror entitiesDefault.json). For assertions.
# --------------------------------------------------------------------------
THING_NAME = "thing name 1"
THING_DESCRIPTION = "thing 1"
LOCATION_NAME = "location name 1"
LOCATION_COORDINATES = [-117.05, 51.05]
LOCATION_ENCODING = "application/vnd.geo+json"

# Datastreams in payload order. DS1 = Luminous Flux, DS2 = Tempretaure (sic).
DS1_NAME = "datastream name 1"
DS1_UNIT_NAME = "Lumen"
DS1_OBSERVED_PROPERTY = "Luminous Flux"
DS1_SENSOR = "sensor name 1"
DS1_RESULTS = [3, 4]
DS1_TIMES = ["2015-03-03T00:00:00Z", "2015-03-04T00:00:00Z"]

DS2_NAME = "datastream name 2"
DS2_UNIT_NAME = "Centigrade"
DS2_OBSERVED_PROPERTY = "Tempretaure"  # intentional typo, do NOT "fix"
DS2_SENSOR = "sensor name 2"
DS2_RESULTS = [5, 6]
DS2_TIMES = ["2015-03-05T00:00:00Z", "2015-03-06T00:00:00Z"]

# Across the whole Thing.
ALL_RESULTS = DS1_RESULTS + DS2_RESULTS                # [3, 4, 5, 6]
ALL_TIMES = DS1_TIMES + DS2_TIMES

SEED_POINT = {"type": "Point", "coordinates": LOCATION_COORDINATES}
# Polygon containing Point(-117.05, 51.05):
SEED_CONTAINING_POLYGON_WKT = (
    "POLYGON((-118 50, -116 50, -116 52, -118 52, -118 50))"
)
# Polygon NOT containing the seed point:
SEED_DISJOINT_POLYGON_WKT = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"
FAR_POINT_WKT = "POINT(0 0)"

# encodingType constants reused by c02's own-entity payloads.
GEOJSON = "application/vnd.geo+json"
SENSOR_PDF = "application/pdf"
OM_MEASUREMENT = "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement"


def unit_lumen() -> dict:
    return {
        "name": "Lumen",
        "symbol": "lm",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/Lumen",
    }


# --------------------------------------------------------------------------
# Minimal single-entity payloads (used by c02 create/update tests). Each takes
# a unique `tag` (from the unique_name fixture) so created entities are
# collision-free and findable; c02 cleans up everything it creates.
# --------------------------------------------------------------------------
def minimal_thing(tag: str) -> dict:
    return {
        "name": f"{tag} Thing",
        "description": "conformance thing",
        "properties": {"tag": tag},
    }


def minimal_location(tag: str) -> dict:
    return {
        "name": f"{tag} Location",
        "description": "conformance location",
        "encodingType": GEOJSON,
        "location": dict(SEED_POINT),
    }


def minimal_sensor(tag: str, metadata="Light flux sensor") -> dict:
    # istSOS4 stores metadata as JSON(B); a bare string is accepted (the
    # encodingType application/pdf case). Pass a dict to exercise object metadata.
    return {
        "name": f"{tag} Sensor",
        "description": "conformance sensor",
        "encodingType": SENSOR_PDF,
        "metadata": metadata,
    }


def minimal_observed_property(tag: str) -> dict:
    return {
        "name": f"{tag} ObservedProperty",
        "definition": "https://example.org/def/temperature",
        "description": "conformance observed property",
    }


def minimal_datastream(tag: str, thing_id, sensor_id, observed_property_id) -> dict:
    return {
        "name": f"{tag} Datastream",
        "description": "conformance datastream",
        "unitOfMeasurement": unit_lumen(),
        "observationType": OM_MEASUREMENT,
        "Thing": {"@iot.id": thing_id},
        "Sensor": {"@iot.id": sensor_id},
        "ObservedProperty": {"@iot.id": observed_property_id},
    }


def minimal_observation(tag: str, datastream_id, result=3,
                        phenomenon_time="2015-03-03T00:00:00Z") -> dict:
    return {
        "phenomenonTime": phenomenon_time,
        "result": result,
        "Datastream": {"@iot.id": datastream_id},
    }


def minimal_feature_of_interest(tag: str) -> dict:
    return {
        "name": f"{tag} FeatureOfInterest",
        "description": "conformance feature of interest",
        "encodingType": GEOJSON,
        "feature": dict(SEED_POINT),
    }


def minimal_historical_location(tag: str, thing_id, time="2015-03-03T00:00:00Z") -> dict:
    return {
        "time": time,
        "Thing": {"@iot.id": thing_id},
    }
