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
