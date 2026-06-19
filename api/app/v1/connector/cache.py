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
Cache reads for the connector API layer.

Backing store: a JSON file on disk, written once per harvest cycle by
scheduler.py. This is a deliberate stand-in for Redis -- the interface
shape (async, returns dict | None) matches what Harvesting-Layer-Reference.md
specifies for the eventual Redis-backed cache.py, so swapping the backend
later does not require touching api.py or scheduler.py's call sites.

These functions never trigger a harvest. If the cache file does not exist
yet (first boot, before the first scheduled cycle completes), they return
None -- the API layer is responsible for turning that into a 503.

Public interface:
    get_stac(config) -> dict | None
    get_dcat(config) -> dict | None
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from app.v1.connector.config import Settings

logger = logging.getLogger(__name__)


def _read_json(path: Path) -> Optional[dict]:
    """
    Read and parse a JSON file from disk. Returns None if the file does not
    exist or cannot be parsed -- both are treated as "cache not ready yet"
    rather than as errors, since the API layer's only valid response in
    that case is a 503, not a crash.
    """
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        logger.exception("Failed to read cache file: %s", path)
        return None


async def get_stac(config: Settings) -> Optional[dict]:
    """
    Return the cached STAC catalog dict, or None if the cache file has not
    been written yet by a harvest cycle.

    Reads from disk on every call -- no in-memory caching at this layer.
    """
    return _read_json(Path(config.STAC_CACHE_PATH))


async def get_dcat(config: Settings) -> Optional[dict]:
    """
    Return the cached DCAT-AP catalog dict, or None if the cache file has
    not been written yet by a harvest cycle.

    Reads from disk on every call -- no in-memory caching at this layer.
    """
    return _read_json(Path(config.DCAT_CACHE_PATH))


def write_stac(config: Settings, catalog: dict) -> None:
    """
    Write the STAC catalog dict to the cache file. Called by scheduler.py
    once per harvest cycle, never by api.py.

    Writes to a temp file then renames over the target, so a reader never
    observes a partially written file mid-write.
    """
    path = Path(config.STAC_CACHE_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(catalog, f, default=str)
    tmp_path.replace(path)


def write_dcat(config: Settings, catalog: dict) -> None:
    """
    Write the DCAT-AP catalog dict to the cache file. Called by
    scheduler.py once per harvest cycle, never by api.py.

    Writes to a temp file then renames over the target, so a reader never
    observes a partially written file mid-write.
    """
    path = Path(config.DCAT_CACHE_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(catalog, f, default=str)
    tmp_path.replace(path)
