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

from rdflib import Graph

from app.db.redis_db import redis
from app.v1.connector.utils import flatten_stac_catalog

logger = logging.getLogger(__name__)

_STAC_KEY_PREFIX = "stac:*"
STAC_AVAILABILITY = False 
LAST_FETCH: Optional[str] = None

# DCAT-AP graphs are cached whole per scope -- unlike STAC's flat
# per-Collection/per-Item keys, there is no cross-scope merge to
# invalidate, so each scope's serialized Turtle is written and purged
# under its own "dcat:*" prefix, entirely independent of "stac:*".
_DCAT_KEY_PREFIX = "dcat:*"


def get_stac_metadata() -> Dict[str, Any]:
    """Helper to fetch all metadata from Redis safely with defaults."""
    raw_avail = redis.get("stac:meta:availability")
    raw_fetch = redis.get("stac:meta:last_fetch")
    
    return {
        "stac_availability": json.loads(raw_avail) if raw_avail else False,
        "last_fetch": raw_fetch.decode("utf-8") if raw_fetch else None,
    }


def get_dcat_metadata() -> Dict[str, Any]:
    """Helper to fetch DCAT availability/last-fetch/network-id metadata from
    Redis safely with defaults. Independent of get_stac_metadata -- the two
    standards are harvested and cached on the same cycle but tracked as
    separate availability flags, since one transformer failing should not
    be reported as if both did."""
    raw_avail = redis.get("dcat:meta:availability")
    raw_fetch = redis.get("dcat:meta:last_fetch")
    raw_network_ids = redis.get("dcat:meta:network_ids")

    return {
        "dcat_availability": json.loads(raw_avail) if raw_avail else False,
        "last_fetch": raw_fetch.decode("utf-8") if raw_fetch else None,
        "network_ids": json.loads(raw_network_ids) if raw_network_ids else [],
    }


def _dcat_root_key() -> str:
    return "dcat:graph:root"


def _dcat_orphan_key() -> str:
    return "dcat:graph:orphan"


def _dcat_network_key(network_id) -> str:
    return f"dcat:graph:net-{network_id}"


# JSON-LD is cached alongside Turtle under the same scope, one key per
# format rather than one key holding both -- readers ask for exactly the
# bytes they want, and _purge_dcat_keys()'s "dcat:*" scan already covers
# both suffixes so no separate purge logic is needed.

def _dcat_root_jsonld_key() -> str:
    return "dcat:graph:root:jsonld"


def _dcat_orphan_jsonld_key() -> str:
    return "dcat:graph:orphan:jsonld"


def _dcat_network_jsonld_key(network_id) -> str:
    return f"dcat:graph:net-{network_id}:jsonld"


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


# DCAT-AP reads and writes
#
# Each scope (root / orphan / one graph per Network) is cached as one whole
# serialized Turtle document under its own key -- see dcat_transformer.py's
# module docstring for why DCAT graphs are never merged across scopes the
# way STAC's flat entity keys are. Readers get raw Turtle text back; api.py
# is responsible for setting the response content-type, this layer does not
# know or care that it's an HTTP response.

async def get_dcat_root() -> Optional[str]:
    """
    Return the cached root DCAT-AP Turtle document, or None if no harvest
    cycle has written it yet.

    Under NETWORK=0 this is the only DCAT graph and carries every Dataset
    and DatasetSeries. Under NETWORK=1 this is the structural-only root
    (Catalog + DataService + Agents + dct:hasPart links), see
    get_dcat_orphan / get_dcat_network for the scopes that carry data.
    """
    raw = redis.get(_dcat_root_key())
    if raw is None:
        return None
    return raw.decode("utf-8") if isinstance(raw, bytes) else raw


async def get_dcat_orphan() -> Optional[str]:
    """
    Return the cached orphan-scope DCAT-AP Turtle document (Datastreams with
    no assigned Network), or None if NETWORK=0 or no harvest cycle has
    written it yet.
    """
    raw = redis.get(_dcat_orphan_key())
    if raw is None:
        return None
    return raw.decode("utf-8") if isinstance(raw, bytes) else raw


async def get_dcat_network(network_id) -> Optional[str]:
    """
    Return the cached DCAT-AP Turtle document for one Network's sub-catalog,
    or None if this network_id doesn't exist, NETWORK=0, or no harvest cycle
    has completed yet.
    """
    raw = redis.get(_dcat_network_key(network_id))
    if raw is None:
        return None
    return raw.decode("utf-8") if isinstance(raw, bytes) else raw


# JSON-LD readers -- same scopes as the Turtle readers above, same
# None-if-missing contract, just the sibling ":jsonld" key.

async def get_dcat_root_jsonld() -> Optional[str]:
    """Return the cached root DCAT-AP JSON-LD document, or None if no
    harvest cycle has written it yet. Same scope rules as get_dcat_root."""
    raw = redis.get(_dcat_root_jsonld_key())
    if raw is None:
        return None
    return raw.decode("utf-8") if isinstance(raw, bytes) else raw


async def get_dcat_orphan_jsonld() -> Optional[str]:
    """Return the cached orphan-scope DCAT-AP JSON-LD document, or None if
    NETWORK=0 or no harvest cycle has written it yet. Same scope rules as
    get_dcat_orphan."""
    raw = redis.get(_dcat_orphan_jsonld_key())
    if raw is None:
        return None
    return raw.decode("utf-8") if isinstance(raw, bytes) else raw


async def get_dcat_network_jsonld(network_id) -> Optional[str]:
    """Return the cached DCAT-AP JSON-LD document for one Network's
    sub-catalog, or None if this network_id doesn't exist, NETWORK=0, or
    no harvest cycle has completed yet. Same scope rules as
    get_dcat_network."""
    raw = redis.get(_dcat_network_jsonld_key(network_id))
    if raw is None:
        return None
    return raw.decode("utf-8") if isinstance(raw, bytes) else raw


def _purge_dcat_keys() -> None:
    cursor = 0
    stale_keys: list[str] = []
    while True:
        cursor, keys = redis.scan(cursor=cursor, match=_DCAT_KEY_PREFIX)
        stale_keys.extend(keys)
        if cursor == 0:
            break
    if stale_keys:
        redis.delete(*stale_keys)


def write_dcat_catalog(result: Dict[str, Graph]) -> None:
    """
    Serialize and write the NETWORK=0 DCAT-AP graph to Redis.

    result is build_dcat_catalog()'s direct output: {"root": Graph}.
    Following the sta2rest / write_stac_catalog pattern, this uses the
    synchronous redis client and purges stale "dcat:*" keys before writing,
    so readers hitting the cache mid-write may see a temporary miss rather
    than a mixed old/new graph.
    """
    _purge_dcat_keys()

    root_graph = result["root"]
    redis.set(_dcat_root_key(), root_graph.serialize(format="turtle"))
    redis.set(_dcat_root_jsonld_key(), root_graph.serialize(format="json-ld"))

    redis.set("dcat:meta:availability", json.dumps(True))
    redis.set("dcat:meta:last_fetch", datetime.datetime.now(datetime.timezone.utc).isoformat())
    redis.set("dcat:meta:network_ids", json.dumps([]))

    logger.info(
        "DCAT cache written to Redis: 1 root graph (%d triples)",
        len(result["root"]),
    )


def write_dcat_catalog_with_networks(result: Dict[str, Any]) -> None:
    """
    Serialize and write the NETWORK=1 DCAT-AP graphs to Redis.

    result is build_dcat_catalog_with_networks()'s output:
        {"root": Graph, "orphan": Graph, "networks": {network_id: Graph, ...}}

    Key layout:
        dcat:graph:root        -> structural-only root (Catalog + DataService)
        dcat:graph:orphan      -> orphan scope (full Dataset/DatasetSeries content)
        dcat:graph:net-{id}    -> one per Network (full Dataset/DatasetSeries content)
    """
    _purge_dcat_keys()

    redis.set(_dcat_root_key(), result["root"].serialize(format="turtle"))
    redis.set(_dcat_root_jsonld_key(), result["root"].serialize(format="json-ld"))
    redis.set(_dcat_orphan_key(), result["orphan"].serialize(format="turtle"))
    redis.set(_dcat_orphan_jsonld_key(), result["orphan"].serialize(format="json-ld"))

    network_ids = []
    for network_id, graph in result["networks"].items():
        redis.set(_dcat_network_key(network_id), graph.serialize(format="turtle"))
        redis.set(_dcat_network_jsonld_key(network_id), graph.serialize(format="json-ld"))
        network_ids.append(network_id)

    redis.set("dcat:meta:availability", json.dumps(True))
    redis.set("dcat:meta:last_fetch", datetime.datetime.now(datetime.timezone.utc).isoformat())
    redis.set("dcat:meta:network_ids", json.dumps(network_ids))

    logger.info(
        "DCAT network cache written to Redis: 1 root graph (%d triples), "
        "1 orphan graph (%d triples), %d Network graphs",
        len(result["root"]), len(result["orphan"]), len(network_ids),
    )