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

