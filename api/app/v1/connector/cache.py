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
from typing import Optional, Dict, Any
import datetime

from app.db.redis_db import redis
from app.v1.connector.utils import flatten_stac_catalog

logger = logging.getLogger(__name__)

_STAC_KEY_PREFIX = "stac:*"
STAC_AVAILABILITY = False 
LAST_FETCH: Optional[str] = None


def get_stac_metadata() -> Dict[str, Any]:
    """Helper to fetch all metadata from Redis safely with defaults."""
    raw_avail = redis.get("stac:meta:availability")
    raw_fetch = redis.get("stac:meta:last_fetch")
    
    return {
        "stac_availability": json.loads(raw_avail) if raw_avail else False,
        "last_fetch": raw_fetch.decode("utf-8") if raw_fetch else None,
    }


def _collection_key(collection_id: str) -> str:
    return f"stac:collection:{collection_id}"


def _item_key(collection_id: str, item_id: str) -> str:
    return f"stac:item:{collection_id}:{item_id}"


def _network_catalog_key(network_id) -> str:
    return f"stac:network:{network_id}"


def _network_collection_key(network_id, collection_id: str) -> str:
    return f"stac:network:{network_id}:collection:{collection_id}"


def _network_item_key(network_id, collection_id: str, item_id: str) -> str:
    return f"stac:network:{network_id}:item:{collection_id}:{item_id}"


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


async def get_network_catalog(network_id) -> Optional[dict]:
    """Cached subcatalog dict for one Network, or None if it hasn't been written
    (NETWORK disabled, or this network_id doesn't exist)."""
    raw = redis.get(_network_catalog_key(network_id))
    return json.loads(raw) if raw is not None else None


async def get_network_collection(network_id, collection_id: str) -> Optional[dict]:
    raw = redis.get(_network_collection_key(network_id, collection_id))
    return json.loads(raw) if raw is not None else None


async def get_network_item(network_id, collection_id: str, item_id: str) -> Optional[dict]:
    raw = redis.get(_network_item_key(network_id, collection_id, item_id))
    return json.loads(raw) if raw is not None else None


def write_stac_catalog(root_dict: dict) -> None:
    """
    Flattens and writes the STAC catalog to Redis, clearing old keys first.

    root_dict is the direct output of stac_transformer.build_stac_catalog():
    {"catalog": {...}, "collections": [...]} -- flatten_stac_catalog (utils.py)
    unwraps "catalog" and walks each collection's "items" list to produce the
    flat stac:catalog / stac:collection:{id} / stac:item:{cid}:{id} keys.

    Following the sta2rest pattern, this uses the synchronous redis client 
    imported from app.db.redis_db. It runs safely as a background task. 

    To prevent orphaned data, it purges old keys using `SCAN` before saving 
    the new set. Readers hitting the cache mid-write may see a temporary miss.
    """
    global STAC_AVAILABILITY, LAST_FETCH

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

    redis.set("stac:meta:availability", json.dumps(True))
    redis.set("stac:meta:last_fetch", datetime.datetime.now(datetime.timezone.utc).isoformat())

    logger.info(
        "STAC cache written to Redis: %d keys (1 catalog, %d collections, %d items)",
        len(flat),
        len(root_dict.get("collections", [])),
        sum(len(c.get("items", [])) for c in root_dict.get("collections", [])),
    )


def write_stac_catalog_with_networks(root_dict: dict) -> None:
    """
    Flattens and writes the NETWORK=1 hierarchy to Redis, same purge-then-write
    as write_stac_catalog (single stac:* scan covers both namespaces).

    root_dict is build_stac_catalog_with_networks()'s output:
        {"catalog": {...}, "collections": [...orphan...], "networks": [{"network_id", "catalog", "collections"}]}

    Key layout:
        stac:catalog                          -> root (orphan scope + network_ids)
        stac:collection:{cid}                 -> orphan Collection    (same keys write_stac_catalog uses)
        stac:item:{cid}:{iid}                 -> orphan Item          (same keys write_stac_catalog uses)
        stac:network:{nid}                    -> Network subcatalog
        stac:network:{nid}:collection:{cid}   -> Network-scoped Collection
        stac:network:{nid}:item:{cid}:{iid}   -> Network-scoped Item
    """
    global STAC_AVAILABILITY, LAST_FETCH

    cursor = 0
    stale_keys: list[str] = []
    while True:
        cursor, keys = redis.scan(cursor=cursor, match=_STAC_KEY_PREFIX)
        stale_keys.extend(keys)
        if cursor == 0:
            break
    if stale_keys:
        redis.delete(*stale_keys)

    flat: Dict[str, Any] = {"stac:catalog": root_dict["catalog"]}

    for coll in root_dict["collections"]:
        cid = coll["id"]
        for item in coll.get("items", []):
            flat[f"stac:item:{cid}:{item['id']}"] = item
        flat[f"stac:collection:{cid}"] = {k: v for k, v in coll.items() if k != "items"}

    net_collections = 0
    net_items = 0
    for block in root_dict["networks"]:
        nid = block["network_id"]
        flat[_network_catalog_key(nid)] = block["catalog"]
        for coll in block["collections"]:
            cid = coll["id"]
            for item in coll.get("items", []):
                flat[_network_item_key(nid, cid, item["id"])] = item
                net_items += 1
            flat[_network_collection_key(nid, cid)] = {k: v for k, v in coll.items() if k != "items"}
            net_collections += 1

    for key, value in flat.items():
        redis.set(key, json.dumps(value, default=str))

    redis.set("stac:meta:availability", json.dumps(True))
    redis.set("stac:meta:last_fetch", datetime.datetime.now(datetime.timezone.utc).isoformat())

    logger.info(
        "STAC network cache written to Redis: %d keys (1 catalog, %d orphan collections, "
        "%d Networks, %d network collections, %d network items)",
        len(flat), len(root_dict.get("collections", [])), len(root_dict.get("networks", [])),
        net_collections, net_items,
    )
