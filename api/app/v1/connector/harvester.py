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
Harvesting layer for the istSOS Metadata Connector.

Reads metadata directly from the istSOS4 Postgres database via a single
asyncpg JOIN query, normalises the flat result rows into typed Python
dataclasses, and returns a HarvestedCatalog consumed by the STAC and
DCAT-AP transformers.

Design decisions (see docs/Harvesting-Layer-Reference.md for full rationale):

- One asyncpg fetch() call returns everything both transformers need:
  Thing, Location, Datastream, ObservedProperty, and Sensor columns in
  a single flat result set, joined and ordered by (thing_id, ds_id).

- This is a pure read. harvest() never touches Redis. The caller
  (scheduled_harvest_job() in scheduler.py) owns the cache write, the
  advisory lock, and both transformer calls.

- Error handling: the query either succeeds or raises HarvesterQueryError.
  There is no per-request retry here -- if the cycle fails,
  scheduled_harvest_job() catches it, skips the cycle, and the previous
  valid cache stays in Redis untouched.

Public interface:
    harvest(pool) -> HarvestedCatalog
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import asyncpg

from connector.exceptions import HarvesterQueryError

logger = logging.getLogger(__name__)


# Internal data models (contract between harvester and transformers)
# See Harvesting-Layer-Reference.md for full schema documentation.
@dataclass
class HarvestedThing:
    """
    Normalised representation of a single Thing with its Locations and
    Datastreams.

    id and name are guaranteed non-None (skip with warning if id is
    absent, default to "" with warning if name is absent). All other
    fields may be None. locations and datastreams are always lists,
    never None, may be empty.
    """

    id: int
    name: str
    description: Optional[str]
    properties: Optional[dict]
    locations: list[dict]
    datastreams: list[dict]


@dataclass
class HarvestedCatalog:
    """
    Complete harvested snapshot of an istSOS4 deployment's Postgres state.

    things is always a list (never None, may be empty).
    harvested_at is an ISO 8601 UTC string, set when the query completes.
    thing_count always equals len(things).
    """

    things: list[HarvestedThing]
    harvested_at: str
    thing_count: int = field(init=False)

    def __post_init__(self) -> None:
        self.thing_count = len(self.things)


# The harvest query
_HARVEST_QUERY = """
SELECT
    t.id                            AS thing_id,
    t.name                          AS thing_name,
    t.description                   AS thing_description,
    t.properties                    AS thing_properties,
    l.id                             AS loc_id,
    l.name                           AS loc_name,
    l.description                   AS loc_description,
    l."encodingType"                AS loc_encoding_type,
    ST_AsGeoJSON(l.location)::json  AS location_geometry,
    l.properties                    AS loc_properties,
    d.id                            AS ds_id,
    d.name                          AS ds_name,
    d.description                   AS ds_description,
    d."unitOfMeasurement"           AS uom,
    d."observationType"             AS observation_type,
    d."observedArea"                AS observed_area,
    d."phenomenonTime"              AS phenomenon_time,
    d."resultTime"                  AS result_time,
    d.properties                    AS ds_properties,
    op.id                           AS op_id,
    op.name                         AS op_name,
    op.description                  AS op_description,
    op.definition                   AS op_definition,
    op.properties                   AS op_properties,
    s.id                            AS sensor_id,
    s.name                          AS sensor_name,
    s.description                   AS sensor_description,
    s."encodingType"                AS sensor_encoding_type,
    s.metadata                      AS sensor_metadata,
    s.properties                    AS sensor_properties
FROM sensorthings."Thing" t
LEFT JOIN sensorthings."Thing_Location" tl  ON tl.thing_id = t.id
LEFT JOIN sensorthings."Location" l         ON l.id = tl.location_id
LEFT JOIN sensorthings."Datastream" d       ON d.thing_id = t.id
LEFT JOIN sensorthings."ObservedProperty" op ON op.id = d."observedproperty_id"
LEFT JOIN sensorthings."Sensor" s           ON s.id = d.sensor_id
ORDER BY t.id, d.id;
"""


# Row-level normalisation helpers
# Each function maps a slice of one flat asyncpg.Record into the internal
# dict schema. All take the raw record plus enough context (thing_id,
# ds_id) to log a useful warning.
def _parse_unit_of_measurement(raw: Optional[dict]) -> Optional[dict]:
    """Normalise the uom JSON column into a flat dict. None stays None."""
    if not raw:
        return None
    return {
        "name": raw.get("name"),
        "symbol": raw.get("symbol"),
        "definition": raw.get("definition"),
    }


def _parse_observed_property(row: asyncpg.Record, thing_id: int, ds_id: Any) -> Optional[dict]:
    """
    Normalise the op_* columns on a row into an ObservedProperty dict.

    Returns None if op_id is NULL -- the Datastream simply has no linked
    ObservedProperty.
    """
    op_id = row["op_id"]
    if op_id is None:
        return None

    name = row["op_name"] or ""
    if not name:
        logger.warning(
            "ObservedProperty %s in Datastream %s (Thing %s) missing name, using empty string",
            op_id, ds_id, thing_id,
        )

    return {
        "id": op_id,
        "name": name,
        "description": row["op_description"],
        "definition": row["op_definition"],
        "properties": row["op_properties"] or None,
    }


def _parse_sensor(row: asyncpg.Record, thing_id: int, ds_id: Any) -> Optional[dict]:
    """
    Normalise the sensor_* columns on a row into a Sensor dict.

    Returns None if sensor_id is NULL -- the Datastream simply has no
    linked Sensor.
    """
    sensor_id = row["sensor_id"]
    if sensor_id is None:
        return None

    name = row["sensor_name"] or ""
    if not name:
        logger.warning(
            "Sensor %s in Datastream %s (Thing %s) missing name, using empty string",
            sensor_id, ds_id, thing_id,
        )

    return {
        "id": sensor_id,
        "name": name,
        "description": row["sensor_description"],
        "properties": row["sensor_properties"] or None,
        "encoding_type": row["sensor_encoding_type"] or "",
        "metadata": row["sensor_metadata"],
    }


def _parse_datastream(row: asyncpg.Record, thing_id: int) -> Optional[dict]:
    """
    Normalise the ds_* columns on a row into a Datastream dict.

    Returns None if ds_id is NULL -- this row is a Thing-Location cross
    row for a Thing with no Datastreams, not an actual Datastream.
    """
    ds_id = row["ds_id"]
    if ds_id is None:
        return None

    name = row["ds_name"] or ""
    if not name:
        logger.warning(
            "Datastream %s in Thing %s missing name, using empty string",
            ds_id, thing_id,
        )

    return {
        "id": ds_id,
        "name": name,
        "description": row["ds_description"],
        "properties": row["ds_properties"] or None,
        "phenomenon_time": row["phenomenon_time"],
        "result_time": row["result_time"],
        "observed_area": row["observed_area"],
        "observation_type": row["observation_type"],
        "unit_of_measurement": _parse_unit_of_measurement(row["uom"]),
        "observed_property": _parse_observed_property(row, thing_id, ds_id),
        "sensor": _parse_sensor(row, thing_id, ds_id),
    }


def _parse_location(row: asyncpg.Record, thing_id: int) -> Optional[dict]:
    """
    Normalise the loc_* columns on a row into a Location dict.

    The geometry column (location_geometry, already a parsed GeoJSON
    dict via ST_AsGeoJSON) is exposed under the key "geometry" to avoid
    confusion with the Location entity itself.

    Returns None if loc_id is NULL -- this row is a Thing-Datastream
    cross row for a Thing with no Locations, not an actual Location.
    """
    loc_id = row["loc_id"]
    if loc_id is None:
        return None

    name = row["loc_name"] or ""
    if not name:
        logger.warning(
            "Location %s in Thing %s missing name, using empty string",
            loc_id, thing_id,
        )

    return {
        "id": loc_id,
        "name": name,
        "description": row["loc_description"],
        "properties": row["loc_properties"] or None,
        "encoding_type": row["loc_encoding_type"] or "application/geo+json",
        "geometry": row["location_geometry"],
    }


# Row grouping (at "Row grouping" in the reference doc)
def _build_catalog(rows: list[asyncpg.Record]) -> HarvestedCatalog:
    """
    Group flat (Thing x Location x Datastream) rows into a HarvestedCatalog.

    The query is a cross product within each Thing: a Thing with 2
    Locations and 3 Datastreams produces 6 rows, all sharing identical
    Thing columns. This groups by thing_id first, then deduplicates
    Locations and Datastreams within each Thing by their own id so the
    cross product collapses back into the right list sizes -- without
    this, a Thing with 2 Locations and 3 Datastreams would otherwise end
    up with 6 entries in each list instead of 2 and 3.

    Things with no Datastreams (ds_id NULL on every row for that Thing)
    end up with an empty datastreams list. Same rule for Locations.
    """
    things_by_id: dict[int, HarvestedThing] = {}
    seen_location_ids: dict[int, set] = {}
    seen_datastream_ids: dict[int, set] = {}

    for row in rows:
        thing_id = row["thing_id"]
        if thing_id is None:
            logger.warning("Skipping row with NULL thing_id: %s", dict(row))
            continue

        thing = things_by_id.get(thing_id)
        if thing is None:
            name = row["thing_name"] or ""
            if not name:
                logger.warning("Thing %s missing name, using empty string", thing_id)
            thing = HarvestedThing(
                id=thing_id,
                name=name,
                description=row["thing_description"],
                properties=row["thing_properties"] or None,
                locations=[],
                datastreams=[],
            )
            things_by_id[thing_id] = thing
            seen_location_ids[thing_id] = set()
            seen_datastream_ids[thing_id] = set()

        location = _parse_location(row, thing_id)
        if location is not None and location["id"] not in seen_location_ids[thing_id]:
            seen_location_ids[thing_id].add(location["id"])
            thing.locations.append(location)

        datastream = _parse_datastream(row, thing_id)
        if datastream is not None and datastream["id"] not in seen_datastream_ids[thing_id]:
            seen_datastream_ids[thing_id].add(datastream["id"])
            thing.datastreams.append(datastream)

    things = list(things_by_id.values())

    if not things:
        logger.warning("Harvest query returned no Things")

    return HarvestedCatalog(
        things=things,
        harvested_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


# Public interface
async def harvest(pool: asyncpg.Pool) -> HarvestedCatalog:
    """
    Run the harvest JOIN query against pool and return a HarvestedCatalog.

    This is the only entry point for the harvesting layer. It does not
    interact with Redis, an advisory lock, or the transformers -- that
    orchestration belongs to scheduled_harvest_job() in scheduler.py.
    Pure read.

    Raises:
        HarvesterQueryError: the query itself failed (connection lost,
            bad SQL, permissions, pool exhausted, etc).
    """
    logger.info("Starting harvest")
    start = time.monotonic()

    try:
        rows = await pool.fetch(_HARVEST_QUERY)
    except Exception as exc:
        raise HarvesterQueryError(f"Harvest query failed: {exc}") from exc

    catalog = _build_catalog(rows)

    elapsed = time.monotonic() - start
    total_datastreams = sum(len(t.datastreams) for t in catalog.things)

    logger.info(
        "Harvest complete: %d Things, %d total Datastreams, elapsed=%.3fs",
        catalog.thing_count, total_datastreams, elapsed,
    )

    return catalog