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
Redis cache layer for the STAC connector API.

Uses the synchronous Redis client from `app.db.redis_db` to match the sta2rest 
pattern. While the reader functions use `async def` to match API conventions, 
their bodies make direct, fast blocking calls (sub-millisecond) to avoid 
introducing a separate async client pattern.

Data is stored flat, allowing the API to fetch individual Catalogs, Collections, 
or Items instantly by key without loading the entire tree. If a key is missing 
(e.g., before the first harvest or after a deletion), these functions return 
None, leaving the API layer to handle 404 or 503 errors.
"""


from __future__ import annotations

import json
import logging
from typing import Optional

from app.db.redis_db import redis
from app.v1.connector.utils import flatten_stac_catalog

logger = logging.getLogger(__name__)

_STAC_KEY_PREFIX = "stac:*"


def _collection_key(collection_id: str) -> str:
    return f"stac:collection:{collection_id}"


def _item_key(collection_id: str, item_id: str) -> str:
    return f"stac:item:{collection_id}:{item_id}"


async def get_catalog() -> Optional[dict]:
    """
    Return the cached root Catalog dict (collection_ids list, no embedded
    Collections), or None if no harvest cycle has written it yet.
    """
    raw = redis.get("stac:catalog")
    if raw is None:
        return None
    return json.loads(raw)


async def get_collection(collection_id: str) -> Optional[dict]:
    """
    Return one cached Collection dict (item_ids list, no embedded Items),
    or None if this collection_id has never been written -- either no
    harvest cycle has completed yet, or the Thing it was built from no
    longer exists in the current catalog.
    """
    raw = redis.get(_collection_key(collection_id))
    if raw is None:
        return None
    return json.loads(raw)


async def get_item(collection_id: str, item_id: str) -> Optional[dict]:
    """
    Return one cached Item dict, or None if this (collection_id, item_id)
    pair has never been written.

    collection_id is required, not inferred, since the cache key is
    namespaced by collection_id.
    """
    raw = redis.get(_item_key(collection_id, item_id))
    if raw is None:
        return None
    return json.loads(raw)


def write_stac_catalog(root_dict: dict) -> None:
    """
    Flattens and writes the STAC catalog to Redis, clearing old keys first.

    Following the sta2rest pattern, this uses the synchronous redis client 
    imported from app.db.redis_db. It runs safely as a background task. 

    To prevent orphaned data, it purges old keys using `SCAN` before saving 
    the new set. Readers hitting the cache mid-write may see a temporary miss.
    """

    cursor = 0
    stale_keys: list[str] = []
    while True:
        cursor, keys = redis.scan(cursor=cursor, match=_STAC_KEY_PREFIX)
        stale_keys.extend(keys)
        if cursor == 0:
            break
    if stale_keys:
        redis.delete(*stale_keys)

    flat = flatten_stac_catalog(root_dict)
    for key, value in flat.items():
        redis.set(key, json.dumps(value, default=str))

    logger.info(
        "STAC cache written to Redis: %d keys (1 catalog, %d collections, %d items)",
        len(flat),
        len(root_dict.get("collections", [])),
        sum(len(c.get("items", [])) for c in root_dict.get("collections", [])),
    )
