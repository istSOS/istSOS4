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
Shared helpers for the connector package.

Two unrelated groups of helpers live here, kept in one small module rather
than one apiece because neither is big enough to justify its own file:

1. Cache-key flattening for the STAC catalog (flatten_stac_catalog,
   CATALOG_KEY) -- converts a deeply nested STAC catalog tree into a flat
   dictionary of keys and values, so the API can fetch individual
   Catalogs, Collections, or Items instantly without loading or parsing
   the entire tree. Cache key scheme:
       stac:catalog
       stac:collection:{collection_id}
       stac:item:{collection_id}:{item_id}
   Items are namespaced under their parent collection ID for fast lookups
   without a secondary index or slow search scans.

2. STA-to-{STAC,DCAT} shared transform helpers (temporal/spatial parsing,
   href builders) -- these were previously defined twice, once in
   stac_transformer.py and once in dcat_transformer.py, byte-for-byte
   identical in every case except _union_bboxes (dcat_transformer.py's
   version additionally guarded the empty-list case; that guard is kept
   here since it's strictly safer and every existing call site already
   checks non-emptiness first anyway, so this changes no behavior).
   Consolidated here so a fix to, say, phenomenon_time parsing only needs
   to happen once and can't quietly drift between the two standards.
"""

from __future__ import annotations

import functools
import logging
from datetime import datetime, timezone
from typing import Any, Optional, Union

from fastapi import status
from fastapi.responses import JSONResponse

from app import HOSTNAME, SUBPATH, VERSION

logger = logging.getLogger(__name__)

CATALOG_KEY = "stac:catalog"


def _collection_key(collection_id: str) -> str:
    return f"stac:collection:{collection_id}"


def _item_key(collection_id: str, item_id: str) -> str:
    return f"stac:item:{collection_id}:{item_id}"


def flatten_stac_catalog(root_dict: dict) -> dict[str, Any]:
    """
    Flattens a nested STAC catalog dictionary into separate cache entries.

    Expects the build_stac_catalog() output shape from stac_transformer.py:
        {"catalog": {...catalog metadata, "links": [...]},
         "collections": [{...collection metadata, "items": [...], "links": [...]}]}

    Extracts nested items and collections from the root tree and maps them 
    to unique cache keys. It safely tracks relationships by embedding tracking 
    lists (`item_ids` and `collection_ids`) directly inside the parent objects.
    Navigation links on the catalog/collection/item dicts are already built
    by stac_transformer.py and are passed through untouched here.

    Safe to use: safely copies data to prevent mutating the original input tree.
    Malformed inputs (e.g., missing keys or IDs) will raise a standard KeyError.
    """

    flat: dict[str, Any] = {}

    catalog_entry = dict(root_dict["catalog"])
    collection_ids: list[str] = []

    for collection_dict in root_dict.get("collections", []):
        collection_id = collection_dict["id"]
        collection_ids.append(collection_id)

        collection_entry = {k: v for k, v in collection_dict.items() if k != "items"}
        item_ids: list[str] = []

        for item_dict in collection_dict.get("items", []):
            item_id = item_dict["id"]
            item_ids.append(item_id)
            flat[_item_key(collection_id, item_id)] = item_dict

        collection_entry["item_ids"] = item_ids
        flat[_collection_key(collection_id)] = collection_entry

    catalog_entry["collection_ids"] = collection_ids
    flat[CATALOG_KEY] = catalog_entry

    return flat


# Shared STA-to-{STAC,DCAT} transform helpers
#
# Moved here verbatim from stac_transformer.py / dcat_transformer.py, which
# each defined every one of these independently. See module docstring for
# the one intentional behavior change (_union_bboxes now guards the empty
# list, which no existing call site could actually trigger).

# Temporal helpers
def parse_iso(value: Optional[str]) -> Optional[datetime]:
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


def parse_phenomenon_time(
    phenomenon_time: Union[Any, str, None],
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
        start = parse_iso(start_str)
        end = parse_iso(end_str) if end_str and end_str != ".." else None
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
def extract_all_coordinates(geometry: dict) -> list[list[float]]:
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
            result.extend(extract_all_coordinates(geom))
        return result
    return []


def bbox_from_geometry(geometry: Optional[dict]) -> Optional[list[float]]:
    """
    Derive a [minx, miny, maxx, maxy] bbox from a GeoJSON geometry dict.
    Returns None when geometry is None or no coordinates are extractable.
    """
    if geometry is None:
        return None
    coords = extract_all_coordinates(geometry)
    if not coords:
        return None
    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    return [min(lons), min(lats), max(lons), max(lats)]


def union_bboxes(bboxes: list[list[float]]) -> Optional[list[float]]:
    """Compute the union bounding box from a list of [minx, miny, maxx, maxy]
    bboxes. Returns None for an empty input (every current call site already
    checks non-emptiness first, this is just a safe default for future ones)."""
    if not bboxes:
        return None
    return [
        min(b[0] for b in bboxes),
        min(b[1] for b in bboxes),
        max(b[2] for b in bboxes),
        max(b[3] for b in bboxes),
    ]


# HTTP error envelope helpers
#
# api.py had this exact shape -- JSONResponse with a
# {"code", "type": "error", "message"} body -- built ad hoc in ~15 places:
# a try/except Exception around every route body returning a 400 this way,
# plus _cache_unavailable (503)/_not_found (404) in api.py and _hidden
# (404) in auth_gate.py already hand-rolling the same envelope. Worse,
# _turtle_unavailable/_turtle_not_found in api.py were byte-for-byte
# identical to _cache_unavailable/_not_found under different names.
# Centralized here so there is exactly one place that defines what an
# error response looks like.

def error_response(status_code: int, detail: str) -> JSONResponse:
    """Standard connector error envelope: {"code", "type": "error", "message"}."""
    return JSONResponse(
        status_code=status_code,
        content={"code": status_code, "type": "error", "message": detail},
    )


def catch_errors(func):
    """
    Route decorator: run the handler, and if it raises, return the
    standard error_response(400, str(e)) instead of letting FastAPI turn
    it into a 500 -- replaces the identical try/except Exception block
    every connector route used to repeat individually.

    Composes with _require_enabled the same way every route already
    stacked its try/except beneath that decorator: put @catch_errors
    below @_require_enabled (i.e. closer to `async def`), so a
    disabled-standard 404 short-circuits before catch_errors is even
    entered.
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            return error_response(status.HTTP_400_BAD_REQUEST, str(e))
    return wrapper


# STA href reconstruction
def datastream_href(ds_id) -> str:
    """
    Build the absolute STA href for a Datastream entity.

    Uses HOSTNAME/SUBPATH/VERSION from app/__init__.py -- the same constants
    main.py uses for every STA entity link. The harvested Datastream dict
    has no self_link field (the harvester contract is deliberate about this).
    """
    return f"{HOSTNAME}{SUBPATH}{VERSION}/Datastreams({ds_id})"


def thing_href(thing_id) -> str:
    return f"{HOSTNAME}{SUBPATH}{VERSION}/Things({thing_id})"
