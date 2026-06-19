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

Consumes a HarvestedCatalog from app.v1.connector.harvester and builds a complete
STAC 1.0 Catalog, with one Collection per Thing and one Item per Datastream.
All objects are constructed using pystac and serialized to plain dicts.

This is a pure function over a HarvestedCatalog without reading from Postgres,
or performing read or write Redis/disk, and is not called per-request from
api.py and rather, scheduler.py calls build_stac_catalog() once per harvest cycle
and writes the resulting dict to the cache. api.py only ever reads the
cached dict back via cache.py.

Mapping decisions (at docs/STA-STAC-Mapping-Reference.md):

Public interface:
    build_stac_catalog(catalog, config) -> dict  (full root Catalog, with
        Collections and Items embedded as children -- this is the single
        dict written to the cache file by scheduler.py)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional, Union

import asyncpg
import pystac
from app import HOSTNAME, SUBPATH, VERSION

from app.v1.connector.config import Settings
from app.v1.connector.harvester import HarvestedCatalog, HarvestedThing

logger = logging.getLogger(__name__)


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
    phenomenon_time: Union[asyncpg.Range, str, None],
) -> tuple[Optional[datetime], Optional[datetime]]:
    """
    Parse a Datastream phenomenon_time value into (start, end) datetimes.

    Datastream.phenomenon_time is sourced from the istSOS4 Postgres
    column sensorthings."Datastream"."phenomenonTime", which is a
    tstzrange (see database/istsos_schema.sql). asyncpg decodes tstzrange
    natively into an asyncpg.Range object and not a "start/end" string,
    so this parses .lower/.upper/.isempty directly rather than splitting
    on "/". Both bounds may already be timezone-aware datetimes (or
    None, for an unbounded side of the range) straight from asyncpg; no
    string parsing is needed for the typical case.

    A plain str is still accepted as a fallback (covers a manually
    constructed dict, e.g. in tests, or a future caller that already
    stringified the range) and parsed in the old "start/end" /
    "start/.." form.

    Returns (start, None) when end is absent, open-ended, or the upper
    bound is exclusive-and-equal-to-empty (per Range.isempty).
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

    # asyncpg.Range (or any object with the same .lower/.upper/.isempty shape)
    if getattr(phenomenon_time, "isempty", False):
        return None, None

    start = phenomenon_time.lower
    end = phenomenon_time.upper

    if start is not None and start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end is not None and end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    return start, end


def _compute_item_datetime(ds: dict) -> Optional[datetime]:
    """
    Determine the primary datetime value for a STAC Item from a Datastream dict.

    Per the STAC spec, datetime is the primary timestamp. Set to
    phenomenon_time end for closed streams, or start for live (open-ended)
    streams. Returns None if phenomenon_time is absent or unparseable --
    the caller skips the Datastream in that case.
    """
    phenomenon_time = ds.get("phenomenon_time")
    start, end = _parse_phenomenon_time(phenomenon_time)
    if start is None:
        return None
    return end if end is not None else start


# Spatial helpers
def _extract_all_coordinates(geometry: dict) -> list[list[float]]:
    """
    Recursively extract all leaf [lon, lat] coordinate pairs from a GeoJSON
    geometry dict. Works for Point, MultiPoint, LineString, Polygon,
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
    Resolve the geometry and bbox for a STAC Item from a Datastream dict.

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
    - Thing.name (always included)
    - ObservedProperty.name from each Datastream -- split on ":" so a
      category:subcategory:phenomenon_id naming convention emits each part
    - Datastream.properties["keywords"] list

    Preserves insertion order while deduplicating. Thing.name is first.
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

    Datastream.description is primary. When absent or empty, ObservedProperty
    and Sensor descriptions are appended as supplementary context. Falls back
    to the Datastream name when all description fields are empty.
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


# Item properties population
def _populate_item_properties(
    item: pystac.Item,
    thing: HarvestedThing,
    ds: dict,
) -> None:
    """
    Write all recommended and optional properties into item.properties.

    Per the harvester contract: thing.name and ds["name"] are never None
    (default ""); ds["observed_property"] and ds["sensor"] may be None.
    """
    props = item.properties  # direct reference to the mutable dict

    props["title"] = f"{thing.name} - {ds.get('name', '')}"
    props["description"] = _compose_item_description(ds, thing)

    # Temporal interval (in addition to the mandatory item.datetime)
    phenomenon_time = ds.get("phenomenon_time")
    start, end = _parse_phenomenon_time(phenomenon_time)
    if start:
        props["start_datetime"] = start.isoformat()
        props["end_datetime"] = end.isoformat() if end is not None else None

    # Thing reverse-lookup fields (denormalized for STAC search)
    props["thing_id"] = thing.id
    props["thing_name"] = thing.name
    props["datastream_id"] = ds.get("id")

    uom = ds.get("unit_of_measurement")
    if uom is not None:
        props["unit_of_measurement"] = uom

    obs_type = ds.get("observation_type")
    if obs_type:
        props["observation_type"] = obs_type

    op = ds.get("observed_property")
    if op is not None:
        if op.get("name"):
            props["observed_property"] = op["name"]
        if op.get("id") is not None:
            props["observed_property_id"] = op["id"]
        if op.get("definition") is not None:
            props["observed_property_definition"] = op["definition"]
    else:
        logger.warning(
            "Datastream %s in Thing %s has no ObservedProperty -- "
            "observed_property fields will be absent from Item properties",
            ds.get("id"), thing.id,
        )

    sensor = ds.get("sensor")
    if sensor is not None:
        if sensor.get("name"):
            props["sensor_name"] = sensor["name"]
        if sensor.get("id") is not None:
            props["sensor_id"] = sensor["id"]
        if sensor.get("metadata") is not None:
            props["sensor_metadata"] = sensor["metadata"]
    else:
        logger.warning(
            "Datastream %s in Thing %s has no Sensor -- "
            "sensor fields will be absent from Item properties",
            ds.get("id"), thing.id,
        )

    # Optional gap fields from ds["properties"]
    ds_props = ds.get("properties") or {}

    for key in ("created", "updated", "platform", "resolution"):
        value = ds_props.get(key)
        if value:
            props[key] = str(value)

    instruments = ds_props.get("instruments")
    if instruments:
        props["instruments"] = instruments if isinstance(instruments, list) else [str(instruments)]

    # Pass through any remaining non-reserved properties keys.
    _RESERVED_KEYS = frozenset({
        "observedArea", "phenomenonTime", "resultTime",
        "created", "updated", "platform", "resolution", "instruments",
        "keywords", "license", "providers",
    })
    for k, v in ds_props.items():
        if k not in _RESERVED_KEYS and k not in props:
            props[k] = v


# Asset construction
def _datastream_href(ds_id, config: Settings) -> str:
    """
    Build the absolute STA href for a Datastream entity.

    Follows the exact convention istSOS4's main.py uses for every other
    STA entity link (see __handle_root in api/app/main.py):

        f"{HOSTNAME}{SUBPATH}{VERSION}/Datastreams({id})"

    HOSTNAME, SUBPATH, and VERSION come from app/__init__.py, the same
    place main.py imports them from -- they are not connector Settings.
    There is no self_link stored on the harvested Datastream dict -- the
    harvester contract is deliberate about this (STA HTTP selfLinks only
    existed in the old HTTP-harvesting architecture). This is the one
    place that reconstructs it.
    """
    return f"{HOSTNAME}{SUBPATH}{VERSION}/Datastreams({ds_id})"


def _attach_assets(item: pystac.Item, ds: dict, config: Settings) -> None:
    """
    Attach the standard set of Assets to item for one Datastream.

    Always emits:
      observations_json  -- REST/JSON observations feed    roles: ["data"]
      observations_csv   -- CSV bulk export                roles: ["data"]
      datastream          -- STA Datastream metadata link   roles: ["metadata"]

    The datastream asset provides a round-trip link from a STAC Item back
    to its originating STA entity, enabling consumers who discover the
    dataset via STAC to access the full STA metadata.
    """
    ds_name = ds.get("name", "")
    base_href = _datastream_href(ds.get("id"), config)

    item.add_asset(
        "observations_json",
        pystac.Asset(
            href=f"{base_href}/Observations",
            media_type=pystac.MediaType.JSON,
            title=f"{ds_name} -- JSON observations feed",
            roles=["data"],
            extra_fields={
                "description": f"Live OGC SensorThings Observations feed for Datastream: {ds_name}"
            },
        ),
    )

    item.add_asset(
        "observations_csv",
        pystac.Asset(
            href=f"{base_href}/Observations?$resultFormat=CSV",
            media_type="text/csv",
            title=f"{ds_name} -- CSV export",
            roles=["data"],
            extra_fields={
                "description": f"CSV bulk export of Observations for Datastream: {ds_name}"
            },
        ),
    )

    item.add_asset(
        "datastream",
        pystac.Asset(
            href=base_href,
            media_type=pystac.MediaType.JSON,
            title=f"STA Datastream: {ds_name}",
            roles=["metadata"],
            extra_fields={
                "description": f"OGC SensorThings Datastream entity for: {ds_name}"
            },
        ),
    )


# Item builder
def _build_item(
    thing: HarvestedThing,
    ds: dict,
    collection_id: str,
    config: Settings,
) -> Optional[pystac.Item]:
    """
    Build a pystac.Item for one Datastream dict.

    Returns None (with WARNING) when Datastream has no parseable
    phenomenon_time -- a STAC Item without datetime is invalid and cannot
    be indexed by any STAC API. This is the only entity-level skip.

    Items with null geometry are emitted -- geometry:null is valid GeoJSON.
    """
    item_datetime = _compute_item_datetime(ds)
    if item_datetime is None:
        logger.warning(
            "Skipping Datastream %s in Thing %s: no usable phenomenon_time -- "
            "a STAC Item without datetime is invalid",
            ds.get("id"), thing.id,
        )
        return None

    geometry, bbox = _resolve_item_geometry(thing, ds)

    item = pystac.Item(
        id=f"datastream-{ds['id']}",
        geometry=geometry,
        bbox=bbox,
        datetime=item_datetime,
        properties={},
    )

    _populate_item_properties(item, thing, ds)
    _attach_assets(item, ds, config)

    # Round-trip link back to the STA Datastream entity
    ds_href = _datastream_href(ds.get("id"), config)
    item.add_link(
        pystac.Link(
            rel="sta_datastream",
            target=ds_href,
            media_type=pystac.MediaType.JSON,
            title=f"STA Datastream: {ds.get('name', '')}",
        )
    )

    item.collection_id = collection_id
    return item


# Collection extent computation
def _compute_collection_extent(
    thing: HarvestedThing,
    items: list[pystac.Item],
) -> pystac.Extent:
    """
    Compute the spatial and temporal extent for a Collection from its Items.

    Spatial:
      Union bbox of all Item bboxes. Falls back to union of Thing.locations
      geometries when no Items have a bbox. Falls back to world bbox when
      neither source provides coordinates (WARNING logged).

    Temporal:
      min(start_datetime) / max(end_datetime) across all Items.
      end is None when any Item is open-ended (live stream).
    """
    bboxes: list[list[float]] = [item.bbox for item in items if item.bbox is not None]

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

    spatial_extent = pystac.SpatialExtent(bboxes=[spatial_bbox])

    starts: list[datetime] = []
    ends: list[Optional[datetime]] = []

    for item in items:
        start_str = item.properties.get("start_datetime")
        end_str = item.properties.get("end_datetime")

        if start_str:
            dt = _parse_iso(start_str)
            if dt:
                starts.append(dt)

        if end_str is not None:
            ends.append(_parse_iso(end_str))
        else:
            ends.append(None)  # open-ended stream

    collection_start = min(starts) if starts else None

    if ends and all(e is not None for e in ends):
        collection_end: Optional[datetime] = max(e for e in ends if e is not None)
    else:
        collection_end = None

    temporal_extent = pystac.TemporalExtent(intervals=[[collection_start, collection_end]])

    return pystac.Extent(spatial=spatial_extent, temporal=temporal_extent)


# Collection builder
def _build_collection(
    thing: HarvestedThing,
    items: list[pystac.Item],
    config: Settings,
) -> pystac.Collection:
    """
    Build a pystac.Collection for one Thing with its pre-built Items.

    The Collection extent is computed bottom-up from the Items -- STAC
    Collection extents are envelopes over member Items, not independently
    specified fields.
    """
    collection_id = f"thing-{thing.id}"
    extent = _compute_collection_extent(thing, items)
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

    extra_fields: dict = {
        "thing_id": thing.id,
        "thing_properties": thing.properties,
        "summaries": {
            "observed_property_definitions": list(dict.fromkeys(op_defs)),
            "unit_symbols": list(dict.fromkeys(unit_symbols)),
        },
    }

    thing_props = thing.properties or {}
    if thing_props.get("license"):
        extra_fields["license"] = thing_props["license"]
    if thing_props.get("providers"):
        extra_fields["providers"] = thing_props["providers"]

    collection = pystac.Collection(
        id=collection_id,
        description=thing.description or f"STAC Collection for SensorThings Thing: {thing.name}",
        extent=extent,
        title=thing.name or None,
        extra_fields=extra_fields,
    )

    if keywords:
        collection.extra_fields["keywords"] = keywords

    # Round-trip link back to the STA Thing entity
    thing_href = f"{HOSTNAME}{SUBPATH}{VERSION}/Things({thing.id})"
    collection.add_link(
        pystac.Link(
            rel="sta_thing",
            target=thing_href,
            media_type=pystac.MediaType.JSON,
            title=f"STA Thing: {thing.name}",
        )
    )

    for item in items:
        collection.add_item(item)

    return collection


# Public interface
def build_stac_catalog(catalog: HarvestedCatalog, config: Settings) -> dict:
    """
    Build a complete STAC 1.0 Catalog from a HarvestedCatalog and return it
    as a plain dict, with every Collection and Item embedded as children.

    Constructs:
      - One pystac.Catalog (root)
      - One pystac.Collection per Thing
      - One pystac.Item per Datastream (skipping those without a usable
        phenomenon_time)
      - 3 pystac.Assets per Item (observations_json, observations_csv,
        datastream)

    Calls pystac.normalize_hrefs() once, before serialization, to populate
    all self/root/parent/child link hrefs across the whole tree in one pass.

    This is called exactly once per harvest cycle, by scheduler.py. It does
    not touch the cache, Postgres, or Redis. api.py never calls this
    function directly -- it only reads the dict this function returns,
    already written to the cache by scheduler.py.

    Returns:
        dict -- pystac.Catalog.to_dict() output, fully JSON-serializable,
        with child Collections and their Items embedded.
    """
    stac_root_href = f"{HOSTNAME}{SUBPATH}{VERSION}/connector/stac"

    root_catalog = pystac.Catalog(
        id="istsos-connector-catalog",
        description=(
            f"istSOS4 deployment: {catalog.thing_count} Things, "
            f"harvested at {catalog.harvested_at}."
        ),
        catalog_type=pystac.CatalogType.ABSOLUTE_PUBLISHED,
    )

    skipped_items = 0
    for thing in catalog.things:
        items: list[pystac.Item] = []
        for ds in thing.datastreams:
            item = _build_item(thing, ds, f"thing-{thing.id}", config)
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

        collection = _build_collection(thing, items, config)
        root_catalog.add_child(collection)

    root_catalog.set_self_href(f"{stac_root_href}")
    root_catalog.set_root(root_catalog)

    for collection in root_catalog.get_children():
        collection_href = f"{stac_root_href}/collections/{collection.id}"
        collection.set_self_href(collection_href)

        for item in collection.get_items():
            item.set_self_href(f"{collection_href}/items/{item.id}")

    logger.info(
        "STAC transform complete: %d Collections, %d Items skipped",
        catalog.thing_count, skipped_items,
    )

    root_dict = root_catalog.to_dict(include_self_link=True)
    root_dict["collections"] = []
    for child in root_catalog.get_children():
        collection_dict = child.to_dict(include_self_link=True)
        collection_dict["items"] = [
            item.to_dict(include_self_link=True) for item in child.get_items()
        ]
        root_dict["collections"].append(collection_dict)

    return root_dict
