# Copyright 2025 SUPSI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
STA to STAC 1.0 transformer.

Consumes a HarvestedCatalog from app.v1.connector.harvester and builds a
complete STAC 1.0 Catalog as plain Python dicts -- no pystac object graph.

Output shape (written to Redis by cache.py via flatten_stac_catalog):
    {
        "catalog": {
            ...catalog metadata...
            "collection_ids": ["thing-1", "thing-2", ...]   # tracking list
        },
        "collections": [
            {
                ...collection metadata...
                "item_ids": ["datastream-10", ...]           # tracking list
                "items":   [{...item dict...}, ...]          # full items
            },
            ...
        ]
    }    

Mapping decisions: docs/STA-STAC-Mapping-Reference.md

Public interface:
    build_stac_catalog(catalog) -> dict
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional, Union

import asyncpg
from app import HOSTNAME, SUBPATH, VERSION
from app.v1.connector.harvester import HarvestedCatalog, HarvestedThing

logger = logging.getLogger(__name__)


_STAC_VERSION = "1.0.0"
_MEDIA_JSON = "application/json"
_MEDIA_GEOJSON = "application/geo+json"
_MEDIA_CSV = "text/csv"

STAC_ROOT_HREF = f"{HOSTNAME}{SUBPATH}{VERSION}/connector/stac"


# Temporal helpers
def _parse_iso(value: str) -> Optional[datetime]:
    """
    Parse an ISO 8601 string to a timezone-aware datetime, or return None.

    Handles the common STA format with a trailing 'Z' (e.g. 2020-01-01T00:00:00Z).
    Strings that cannot be parsed are logged and returned as None.
    """
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        logger.warning("Could not parse ISO 8601 datetime: %r", value)
        return None


def _parse_phenomenon_time(
    phenomenon_time: Union["asyncpg.Range", str, None],
) -> tuple[Optional[datetime], Optional[datetime]]:
    """
    Parse a Datastream phenomenon_time value into (start, end) datetimes.

    Datastream.phenomenon_time is sourced from the istSOS4 Postgres column
    sensorthings."Datastream"."phenomenonTime", which is a tstzrange.
    asyncpg decodes tstzrange natively into an asyncpg.Range object;
    both bounds may already be timezone-aware datetimes (or None for an
    unbounded side). A plain str is also accepted as a fallback (covers
    manually-constructed dicts in tests, or a "start/.." / "start/end" form).

    Returns (None, None) when phenomenon_time is None, empty, or start
    itself is unparseable/missing.
    """
    if phenomenon_time is None:
        return None, None

    if isinstance(phenomenon_time, str):
        parts = phenomenon_time.split("/", 1)
        start_str = parts[0].strip()
        end_str = parts[1].strip() if len(parts) > 1 else ""
        start = _parse_iso(start_str)
        end = _parse_iso(end_str) if end_str and end_str != ".." else None
        return start, end

    # asyncpg.Range (or any object with .lower/.upper/.isempty)
    if getattr(phenomenon_time, "isempty", False):
        return None, None

    start = phenomenon_time.lower
    end = phenomenon_time.upper

    if start is not None and start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end is not None and end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    return start, end


# Spatial helpers
def _extract_all_coordinates(geometry: dict) -> list[list[float]]:
    """
    Recursively extract all leaf [lon, lat] coordinate pairs from a GeoJSON
    geometry dict. Handles Point, MultiPoint, LineString, Polygon,
    MultiPolygon, and GeometryCollection.
    """
    geom_type = geometry.get("type", "")
    coords = geometry.get("coordinates")

    if geom_type == "Point" and coords:
        return [coords[:2]]
    if geom_type in ("MultiPoint", "LineString") and coords:
        return [c[:2] for c in coords]
    if geom_type == "Polygon" and coords:
        return [c[:2] for c in coords[0]]
    if geom_type == "MultiPolygon" and coords:
        result: list[list[float]] = []
        for polygon in coords:
            result.extend(c[:2] for c in polygon[0])
        return result
    if geom_type == "GeometryCollection":
        result = []
        for geom in geometry.get("geometries", []):
            result.extend(_extract_all_coordinates(geom))
        return result
    return []


def _bbox_from_geometry(geometry: Optional[dict]) -> Optional[list[float]]:
    """
    Derive a [minx, miny, maxx, maxy] bbox from a GeoJSON geometry dict.
    Returns None when geometry is None or no coordinates are extractable.
    """
    if geometry is None:
        return None
    coords = _extract_all_coordinates(geometry)
    if not coords:
        return None
    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    return [min(lons), min(lats), max(lons), max(lats)]


def _union_bboxes(bboxes: list[list[float]]) -> list[float]:
    """Compute the union bounding box from a list of [minx, miny, maxx, maxy] bboxes."""
    return [
        min(b[0] for b in bboxes),
        min(b[1] for b in bboxes),
        max(b[2] for b in bboxes),
        max(b[3] for b in bboxes),
    ]


def _resolve_item_geometry(
    thing: HarvestedThing, ds: dict
) -> tuple[Optional[dict], Optional[list[float]]]:
    """
    Resolve the geometry and bbox for a STAC Item.

    Fallback chain:
      1. Datastream.observed_area (preferred -- per-variable spatial footprint)
      2. First Thing.locations[0].geometry (fallback when observed_area is None)
      3. None (emitted as geometry:null; WARNING logged with IDs)
    """
    observed_area = ds.get("observed_area")
    if observed_area is not None:
        return observed_area, _bbox_from_geometry(observed_area)

    if thing.locations:
        first_geom = thing.locations[0].get("geometry")
        if first_geom is not None:
            return first_geom, _bbox_from_geometry(first_geom)

    logger.warning(
        "Datastream %s in Thing %s has no observed_area and Thing has no "
        "Locations -- Item will be emitted with geometry:null",
        ds.get("id"), thing.id,
    )
    return None, None


# Keyword extraction
def _extract_collection_keywords(thing: HarvestedThing) -> list[str]:
    """
    Build the deduplicated keyword list for a Collection from a Thing and
    its Datastreams.

    Sources:
      - Thing.name (always first)
      - ObservedProperty.name from each Datastream -- split on ":" so a
        category:subcategory:phenomenon_id naming convention emits each part
      - Datastream.properties["keywords"] list

    Preserves insertion order while deduplicating.
    """
    seen: set[str] = set()
    keywords: list[str] = []

    def _add(kw: str) -> None:
        kw = kw.strip()
        if kw and kw not in seen:
            seen.add(kw)
            keywords.append(kw)

    _add(thing.name)
    for ds in thing.datastreams:
        op = ds.get("observed_property")
        if op and op.get("name"):
            for part in op["name"].split(":"):
                _add(part)
        for kw in (ds.get("properties") or {}).get("keywords", []):
            if isinstance(kw, str):
                _add(kw)

    return keywords


# Description composer
def _compose_item_description(ds: dict, thing: HarvestedThing) -> str:
    """
    Compose the Item description from a Datastream and its parent Thing.

    Datastream.description is primary. ObservedProperty and Sensor
    descriptions are appended as supplementary context. Falls back to
    Datastream.name when all description fields are empty.
    """
    parts: list[str] = []
    if ds.get("description"):
        parts.append(ds["description"])
    op = ds.get("observed_property")
    if op and op.get("description"):
        parts.append(op["description"])
    sensor = ds.get("sensor")
    if sensor and sensor.get("description"):
        parts.append(sensor["description"])
    return " | ".join(p for p in parts if p) or ds.get("name", "")


# STA href reconstruction
def _datastream_href(ds_id) -> str:
    """
    Build the absolute STA href for a Datastream entity.

    Uses HOSTNAME/SUBPATH/VERSION from app/__init__.py -- the same constants
    main.py uses for every STA entity link. The harvested Datastream dict
    has no self_link field (the harvester contract is deliberate about this).
    """
    return f"{HOSTNAME}{SUBPATH}{VERSION}/Datastreams({ds_id})"


def _thing_href(thing_id) -> str:
    return f"{HOSTNAME}{SUBPATH}{VERSION}/Things({thing_id})"


# Link builders
def _item_nav_links(item_id: str, collection_id: str) -> list[dict]:
    """
    Build the complete set of STAC navigation links for an Item.

    Required by STAC 1.0: self, root, parent, collection.
    The sta_datastream cross-reference is appended last as a custom rel.
    """
    collection_href = f"{STAC_ROOT_HREF}/collections/{collection_id}"
    item_href = f"{collection_href}/items/{item_id}"
    return [
        {"rel": "self",       "href": item_href,           "type": _MEDIA_GEOJSON},
        {"rel": "root",       "href": STAC_ROOT_HREF,      "type": _MEDIA_JSON},
        {"rel": "parent",     "href": collection_href,     "type": _MEDIA_JSON},
        {"rel": "collection", "href": collection_href,     "type": _MEDIA_JSON},
    ]


def _collection_nav_links(collection_id: str, item_ids: list[str]) -> list[dict]:
    """
    Build the complete set of STAC navigation links for a Collection.

    Required by STAC 1.0: self, root, parent, one rel=item per Item.
    The sta_thing cross-reference is appended by _build_collection_dict.
    """
    collection_href = f"{STAC_ROOT_HREF}/collections/{collection_id}"
    links = [
        {"rel": "self",   "href": collection_href, "type": _MEDIA_JSON},
        {"rel": "root",   "href": STAC_ROOT_HREF,  "type": _MEDIA_JSON},
        {"rel": "parent", "href": STAC_ROOT_HREF,  "type": _MEDIA_JSON},
    ]
    for iid in item_ids:
        links.append({
            "rel":  "item",
            "href": f"{collection_href}/items/{iid}",
            "type": _MEDIA_GEOJSON,
        })
    return links


def _catalog_nav_links(collection_ids: list[str]) -> list[dict]:
    """
    Build the complete set of STAC navigation links for the root Catalog.

    Required by STAC 1.0: self, root (self-referential), one rel=child per
    Collection.
    """
    links = [
        {"rel": "self", "href": STAC_ROOT_HREF, "type": _MEDIA_JSON},
        {"rel": "root", "href": STAC_ROOT_HREF, "type": _MEDIA_JSON},
    ]
    for cid in collection_ids:
        links.append({
            "rel":  "child",
            "href": f"{STAC_ROOT_HREF}/collections/{cid}",
            "type": _MEDIA_JSON,
        })
    return links


# Item builder
_RESERVED_DS_PROPS = frozenset({
    "observedArea", "phenomenonTime", "resultTime",
    "created", "updated", "platform", "resolution",
    "instruments", "keywords", "license", "providers",
})


def _build_item_dict(
    thing: HarvestedThing,
    ds: dict,
    collection_id: str,
) -> Optional[dict]:
    """
    Build a STAC Item as a plain dict for one Datastream.

    Returns None (with WARNING) when Datastream has no parseable
    phenomenon_time -- a STAC Item without datetime is invalid.
    Items with null geometry are emitted (geometry:null is valid GeoJSON).

    Datetime rule (STAC 1.0 spec):
      - Closed interval (start + end both known):  datetime = null,
        start_datetime and end_datetime carry the interval.
      - Open-ended / live stream (no end):         datetime = start,
        start_datetime = start, end_datetime = null.
      - No parseable phenomenon_time:              Item is skipped entirely.

    Links: all navigation links (self, root, parent, collection) plus the
    sta_datastream cross-reference are written here and cached verbatim.
    api.py serves Item dicts from cache without any link injection.
    """
    start, end = _parse_phenomenon_time(ds.get("phenomenon_time"))
    if start is None:
        logger.warning(
            "Skipping Datastream %s in Thing %s: no usable phenomenon_time -- "
            "a STAC Item without datetime is invalid",
            ds.get("id"), thing.id,
        )
        return None

    geometry, bbox = _resolve_item_geometry(thing, ds)
    item_id = f"datastream-{ds['id']}"

    # STAC 1.0: datetime must be null when start+end interval is present.
    if end is not None:
        item_datetime = None
        start_dt_str = start.isoformat()
        end_dt_str = end.isoformat()
    else:
        # Open-ended stream: datetime = start, end_datetime = null.
        item_datetime = start.strftime("%Y-%m-%dT%H:%M:%SZ")
        start_dt_str = start.isoformat()
        end_dt_str = None

    properties: dict = {
        "datetime":       item_datetime,
        "start_datetime": start_dt_str,
        "end_datetime":   end_dt_str,
        "title":          f"{thing.name} - {ds.get('name', '')}",
        "description":    _compose_item_description(ds, thing),
        "thing_id":       thing.id,
        "thing_name":     thing.name,
        "datastream_id":  ds.get("id"),
    }

    uom = ds.get("unit_of_measurement")
    if uom:
        properties["unit_of_measurement"] = uom

    obs_type = ds.get("observation_type")
    if obs_type:
        properties["observation_type"] = obs_type

    op = ds.get("observed_property")
    if op is not None:
        if op.get("name"):
            properties["observed_property"] = op["name"]
        if op.get("id") is not None:
            properties["observed_property_id"] = op["id"]
        if op.get("definition") is not None:
            properties["observed_property_definition"] = op["definition"]
    else:
        logger.warning(
            "Datastream %s in Thing %s has no ObservedProperty -- "
            "observed_property fields will be absent from Item properties",
            ds.get("id"), thing.id,
        )

    sensor = ds.get("sensor")
    if sensor is not None:
        if sensor.get("name"):
            properties["sensor_name"] = sensor["name"]
        if sensor.get("id") is not None:
            properties["sensor_id"] = sensor["id"]
        if sensor.get("metadata") is not None:
            properties["sensor_metadata"] = sensor["metadata"]
    else:
        logger.warning(
            "Datastream %s in Thing %s has no Sensor -- "
            "sensor fields will be absent from Item properties",
            ds.get("id"), thing.id,
        )

    ds_props = ds.get("properties") or {}
    for key in ("created", "updated", "platform", "resolution"):
        value = ds_props.get(key)
        if value:
            properties[key] = str(value)
    instruments = ds_props.get("instruments")
    if instruments:
        properties["instruments"] = (
            instruments if isinstance(instruments, list) else [str(instruments)]
        )
    for k, v in ds_props.items():
        if k not in _RESERVED_DS_PROPS and k not in properties:
            properties[k] = v

    base_href = _datastream_href(ds.get("id"))
    ds_name = ds.get("name", "")

    assets = {
        "observations_json": {
            "href":        f"{base_href}/Observations",
            "type":        _MEDIA_JSON,
            "title":       f"{ds_name} -- JSON observations feed",
            "roles":       ["data"],
            "description": f"Live OGC SensorThings Observations feed for Datastream: {ds_name}",
        },
        "observations_csv": {
            "href":        f"{base_href}/Observations?$resultFormat=CSV",
            "type":        _MEDIA_CSV,
            "title":       f"{ds_name} -- CSV export",
            "roles":       ["data"],
            "description": f"CSV bulk export of Observations for Datastream: {ds_name}",
        },
        "datastream": {
            "href":        base_href,
            "type":        _MEDIA_JSON,
            "title":       f"STA Datastream: {ds_name}",
            "roles":       ["metadata"],
            "description": f"OGC SensorThings Datastream entity for: {ds_name}",
        },
    }

    # Navigation links built here -- served from cache as-is by api.py.
    # sta_datastream appended after nav links as a custom cross-reference rel.
    links = _item_nav_links(item_id, collection_id) + [
        {
            "rel":   "sta_datastream",
            "href":  base_href,
            "type":  _MEDIA_JSON,
            "title": f"STA Datastream: {ds_name}",
        }
    ]

    return {
        "type":            "Feature",
        "stac_version":    _STAC_VERSION,
        "stac_extensions": [],
        "id":              item_id,
        "geometry":        geometry,
        "bbox":            bbox,
        "properties":      properties,
        "links":           links,
        "assets":          assets,
        "collection":      collection_id,
    }


# Collection builder
def _build_collection_dict(
    thing: HarvestedThing,
    items: list[dict],
) -> dict:
    """
    Build a STAC Collection as a plain dict for one Thing.

    Extent is computed bottom-up from the pre-built Item dicts.
    The "item_ids" tracking list and "items" list are written here for
    cache.py (flatten_stac_catalog reads both). api.py strips "item_ids"
    and "items" before serving -- the full navigation links are already
    in the cached "links" array.

    Temporal extent rule:
      - collection_end is null if ANY item stream is still open (no end_datetime).
        This correctly signals "ongoing" to STAC clients.
      - collection_end is the maximum end_datetime only when ALL items are closed.
    """
    collection_id = f"thing-{thing.id}"

    # Spatial extent
    bboxes: list[list[float]] = [
        item["bbox"] for item in items if item.get("bbox") is not None
    ]
    if not bboxes:
        for loc in thing.locations:
            geom = loc.get("geometry")
            if geom:
                bbox = _bbox_from_geometry(geom)
                if bbox:
                    bboxes.append(bbox)
    if bboxes:
        spatial_bbox = _union_bboxes(bboxes)
    else:
        logger.warning(
            "Thing %s (%s) has no spatial metadata from Items or Locations "
            "-- Collection will use world bbox fallback",
            thing.id, thing.name,
        )
        spatial_bbox = [-180.0, -90.0, 180.0, 90.0]

    # Temporal extent
    # Pull start/end strings directly from already-built item properties --
    # they are always ISO strings at this point (never asyncpg.Range).
    starts: list[datetime] = []
    ends: list[Optional[datetime]] = []
    for item in items:
        s = item["properties"].get("start_datetime")
        e = item["properties"].get("end_datetime")
        if s:
            dt = _parse_iso(s)
            if dt:
                starts.append(dt)
        ends.append(_parse_iso(e) if e else None)

    collection_start = min(starts).isoformat() if starts else None

    # null end = at least one stream is still open (ongoing).
    any_open = any(e is None for e in ends)
    collection_end = None if any_open else (
        max(ends).isoformat() if ends else None  # type: ignore[arg-type]
    )

    # Summaries
    keywords = _extract_collection_keywords(thing)
    op_defs: list[str] = []
    unit_symbols: list[str] = []
    for ds in thing.datastreams:
        op = ds.get("observed_property")
        if op and op.get("definition") is not None:
            op_defs.append(str(op["definition"]))
        uom = ds.get("unit_of_measurement")
        if uom and uom.get("symbol"):
            unit_symbols.append(uom["symbol"])

    thing_props = thing.properties or {}

    item_ids = [item["id"] for item in items]

    # Navigation links built here -- served from cache as-is by api.py.
    # sta_thing appended after nav links as a custom cross-reference rel.
    links = _collection_nav_links(collection_id, item_ids) + [
        {
            "rel":   "sta_thing",
            "href":  _thing_href(thing.id),
            "type":  _MEDIA_JSON,
            "title": f"STA Thing: {thing.name}",
        }
    ]

    coll: dict = {
        "type":         "Collection",
        "stac_version": _STAC_VERSION,
        "id":           collection_id,
        "title":        thing.name or None,
        "description":  (
            thing.description
            or f"STAC Collection for SensorThings Thing: {thing.name}"
        ),
        "keywords": keywords,
        "extent": {
            "spatial":  {"bbox": [spatial_bbox]},
            "temporal": {"interval": [[collection_start, collection_end]]},
        },
        "links":            links,
        "license":          "other",
        "thing_id":         thing.id,
        "thing_properties": thing.properties,
        "summaries": {
            "observed_property_definitions": list(dict.fromkeys(op_defs)),
            "unit_symbols":                  list(dict.fromkeys(unit_symbols)),
        },
    }

    if thing_props.get("license"):
        coll["license"] = thing_props["license"]
    if thing_props.get("providers"):
        coll["providers"] = thing_props["providers"]

    # Tracking lists consumed by cache.py / api.py -- not part of STAC spec.
    coll["item_ids"] = item_ids
    coll["items"] = items

    return coll


# Public interface
def build_stac_catalog(catalog: HarvestedCatalog) -> dict:
    """
    Build a complete STAC 1.0 Catalog from a HarvestedCatalog and return it
    as a plain dict ready for cache.py to flatten into Redis.

    Output shape:
        {
            "catalog": { ...metadata..., "collection_ids": [...], "links": [...] },
            "collections": [
                { ...metadata..., "item_ids": [...], "items": [...], "links": [...] },
                ...
            ]
        }

    All STAC navigation links are built here at transform time and stored in
    the cache. api.py serves cached objects as-is -- it does not inject or
    reconstruct any links. Only the FeatureCollection and /collections
    envelope-level links in api.py are assembled at serve time (those are
    wrappers, not cached entities).

    Called exactly once per harvest cycle by scheduler.py. Does not touch
    Postgres, Redis, or disk.
    """
    collections: list[dict] = []
    skipped_items = 0

    for thing in catalog.things:
        items: list[dict] = []
        for ds in thing.datastreams:
            item = _build_item_dict(thing, ds, f"thing-{thing.id}")
            if item is not None:
                items.append(item)
            else:
                skipped_items += 1

        if not items and thing.datastreams:
            logger.warning(
                "Thing %s (%s): all %d Datastreams were skipped -- "
                "Collection will have no Items",
                thing.id, thing.name, len(thing.datastreams),
            )

        collections.append(_build_collection_dict(thing, items))

    collection_ids = [c["id"] for c in collections]

    root_catalog = {
        "type":         "Catalog",
        "stac_version": _STAC_VERSION,
        "id":           "istsos-connector-catalog",
        "description":  (
            f"istSOS4 deployment: {catalog.thing_count} Things, "
            f"harvested at {catalog.harvested_at}."
        ),
        # Navigation links built here, cached verbatim, served as-is.
        "links": _catalog_nav_links(collection_ids),
        # Tracking list -- not part of STAC spec, consumed by api.py to
        # enumerate children for the /stac/collections listing route.
        "collection_ids": collection_ids,
    }

    logger.info(
        "STAC transform complete: %d Collections, %d Items, %d skipped",
        len(collections),
        sum(len(c["item_ids"]) for c in collections),
        skipped_items,
    )

    return {
        "catalog":     root_catalog,
        "collections": collections,
    }