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

