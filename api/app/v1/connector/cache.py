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

Generations
-----------
Each standard ("stac", "dcat") is published as a whole generation of keys:

    {CONNECTOR_KEY_PREFIX}:{standard}:{generation}:{logical key}
    {CONNECTOR_KEY_PREFIX}:{standard}:current   -> id of the served generation
    {CONNECTOR_KEY_PREFIX}:{standard}:previous  -> id of the one before it

A writer serializes everything first, writes the new generation under fresh
keys, and only then flips ``current`` (one MULTI/EXEC). Readers resolve
``current`` and read from that generation. So a reader never sees a gap or a
half-written catalog, and if serialization or the write fails the pointer is
never moved and the previous catalog keeps being served. The previous
generation is kept for one more cycle, so a reader that resolved the old
pointer just before the flip still finds its keys; anything older is deleted.
The cost is that Redis holds up to two generations of each standard.

Every key is namespaced by CONNECTOR_KEY_PREFIX (derived from the deployment's
HOSTNAME + SUBPATH + VERSION by default), so several deployments can share one
Redis without overwriting each other.
"""


from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from rdflib import Graph

from app.db.redis_db import redis
from app.v1.connector.config import CATALOG_CLOSED_NETWORKS, CONNECTOR_KEY_PREFIX
from app.v1.connector.serialization import serialize_turtle
from app.v1.connector.utils import flatten_stac_catalog

logger = logging.getLogger(__name__)

_STAC = "stac"
_DCAT = "dcat"


def _pointer_key(standard: str, which: str) -> str:
    return f"{CONNECTOR_KEY_PREFIX}:{standard}:{which}"


def _gen_key(standard: str, generation: str, key: str) -> str:
    return f"{CONNECTOR_KEY_PREFIX}:{standard}:{generation}:{key}"


def _current_generation(standard: str) -> Optional[str]:
    raw = redis.get(_pointer_key(standard, "current"))
    if raw is None:
        return None
    return raw.decode("utf-8") if isinstance(raw, bytes) else raw


def _get(standard: str, key: str):
    """Read one logical key from the generation currently served, or None."""
    generation = _current_generation(standard)
    if generation is None:
        return None
    return redis.get(_gen_key(standard, generation, key))


def _publish(standard: str, values: Dict[str, Any]) -> int:
    """
    Write ``values`` (logical key -> str/bytes) as a new generation, flip the
    ``current`` pointer to it atomically, then drop every generation older than
    the one it replaced. Returns the number of keys written.
    """
    # time-ordered and unique: the clock alone is too coarse on some platforms
    generation = f"{time.time_ns():x}-{uuid.uuid4().hex[:8]}"
    pipe = redis.pipeline(transaction=False)
    for key, value in values.items():
        pipe.set(_gen_key(standard, generation, key), value)
    pipe.execute()

    previous = _current_generation(standard)
    flip = redis.pipeline(transaction=True)
    flip.set(_pointer_key(standard, "current"), generation)
    if previous is not None:
        flip.set(_pointer_key(standard, "previous"), previous)
    flip.execute()

    keep = {generation, previous}
    prefix = f"{CONNECTOR_KEY_PREFIX}:{standard}:"
    stale: list = []
    for raw_key in redis.scan_iter(match=prefix + "*", count=1000):
        key = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else raw_key
        gen = key[len(prefix):].split(":", 1)[0]
        if gen not in keep and gen not in ("current", "previous"):
            stale.append(raw_key)
    for i in range(0, len(stale), 1000):
        redis.delete(*stale[i:i + 1000])
    return len(values)


def _network_ids_key() -> str:
    return f"{CONNECTOR_KEY_PREFIX}:meta:network_ids"


def get_stac_metadata() -> Dict[str, Any]:
    """Helper to fetch all metadata from Redis safely with defaults."""
    raw_avail = _get(_STAC, "stac:meta:availability")
    raw_harvested_at = _get(_STAC, "stac:meta:last_harvested_at")

    return {
        "stac_availability": json.loads(raw_avail) if raw_avail else False,
        "last_harvested_at": raw_harvested_at.decode("utf-8") if raw_harvested_at else None,
    }


def get_dcat_metadata() -> Dict[str, Any]:
    """Helper to fetch DCAT availability/last-harvested metadata from Redis
    safely with defaults. Independent of get_stac_metadata -- the two
    standards are harvested and cached on the same cycle but tracked as
    separate availability flags, since one transformer failing should not
    be reported as if both did.

    Network ids are not part of this -- see get_harvest_network_ids(),
    since which Networks exist is a harvest-level fact shared by both
    standards, not a DCAT-owned one.
    """
    raw_avail = _get(_DCAT, "dcat:meta:availability")
    raw_harvested_at = _get(_DCAT, "dcat:meta:last_harvested_at")

    return {
        "dcat_availability": json.loads(raw_avail) if raw_avail else False,
        "last_harvested_at": raw_harvested_at.decode("utf-8") if raw_harvested_at else None,
    }


def get_harvest_network_ids() -> list:
    """
    Return the ids of every Network the last harvest cycle saw (NETWORK=1
    only), with closed networks (CATALOG_CLOSED_NETWORKS) filtered out so
    the ``/connector`` summary endpoint never leaks their existence to
    unauthenticated callers.

    This is a harvest-level fact, not owned by either standard -- the same
    Networks back both the STAC subcatalogs and the DCAT-AP Network
    graphs, so it's written once per cycle by scheduler.py right after
    harvest_with_networks(), independent of which of STAC_TRANSFORMER/
    DCAT_TRANSFORMER are actually on. Returns [] if NETWORK=0 or no
    harvest cycle has completed yet.
    """
    raw_network_ids = redis.get(_network_ids_key())
    all_ids = json.loads(raw_network_ids) if raw_network_ids else []
    return [nid for nid in all_ids if nid not in CATALOG_CLOSED_NETWORKS]


def write_harvest_network_ids(network_ids: list) -> None:
    """
    Write the raw (unfiltered) set of Network ids seen by the current
    harvest cycle. Called once per cycle from scheduler.py under NETWORK=1,
    regardless of which of STAC_TRANSFORMER/DCAT_TRANSFORMER are on --
    this is what get_harvest_network_ids() reads back, filtering closed
    networks at read time.
    """
    redis.set(_network_ids_key(), json.dumps(network_ids))


def _dcat_root_key() -> str:
    return "dcat:graph:root"


# Same root scope as _dcat_root_key(), but with closed Networks included in
# hasPart/dcat:catalog -- only ever written by write_dcat_catalog_with_networks
# (NETWORK=1). Served to authenticated callers only, same reveal rule STAC's
# stac_root applies via closed_network_ids -- see api.py's dcat_root.
def _dcat_root_all_key() -> str:
    return "dcat:graph:root:all"


def _dcat_network_key(network_id) -> str:
    return f"dcat:graph:net-{network_id}"


# JSON-LD is cached alongside Turtle under the same scope, one key per
# format rather than one key holding both -- readers ask for exactly the
# bytes they want, and each generation holds both, so the two formats
# are always served from the same cycle.

def _dcat_root_jsonld_key() -> str:
    return "dcat:graph:root:jsonld"


def _dcat_root_all_jsonld_key() -> str:
    return "dcat:graph:root:all:jsonld"


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
    raw = _get(_STAC, "stac:catalog")
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
    raw = _get(_STAC, _collection_key(collection_id))
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
    raw = _get(_STAC, _item_key(collection_id, item_id))
    if raw is None:
        return None
    return json.loads(raw)


async def get_network_catalog(network_id) -> Optional[dict]:
    """Cached subcatalog dict for one Network, or None if it hasn't been written
    (NETWORK disabled, or this network_id doesn't exist)."""
    raw = _get(_STAC, _network_catalog_key(network_id))
    return json.loads(raw) if raw is not None else None


async def get_network_collection(network_id, collection_id: str) -> Optional[dict]:
    raw = _get(_STAC, _network_collection_key(network_id, collection_id))
    return json.loads(raw) if raw is not None else None


async def get_network_item(network_id, collection_id: str, item_id: str) -> Optional[dict]:
    raw = _get(_STAC, _network_item_key(network_id, collection_id, item_id))
    return json.loads(raw) if raw is not None else None


def write_stac_catalog(root_dict: dict) -> None:
    """
    Flatten the STAC catalog and publish it as a new generation.

    root_dict is the direct output of stac_transformer.build_stac_catalog():
    {"catalog": {...}, "collections": [...]} -- flatten_stac_catalog (utils.py)
    unwraps "catalog" and walks each collection's "items" list to produce the
    flat stac:catalog / stac:collection:{id} / stac:item:{cid}:{id} keys.

    Everything is encoded before anything is written, and the served
    generation only changes once the whole new set is in Redis (see the
    module docstring), so readers never see a partial catalog.
    """
    flat = flatten_stac_catalog(root_dict)
    values = {key: json.dumps(value, default=str) for key, value in flat.items()}
    values["stac:meta:availability"] = json.dumps(True)
    values["stac:meta:last_harvested_at"] = datetime.now(timezone.utc).isoformat()
    _publish(_STAC, values)

    logger.info(
        "STAC cache written to Redis: %d keys (1 catalog, %d collections, %d items)",
        len(flat),
        len(root_dict.get("collections", [])),
        sum(len(c.get("items", [])) for c in root_dict.get("collections", [])),
    )


def write_stac_catalog_with_networks(root_dict: dict) -> None:
    """
    Flatten the NETWORK=1 hierarchy and publish it as a new generation, same
    encode-then-swap order as write_stac_catalog.

    root_dict is build_stac_catalog_with_networks()'s output:
        {"catalog": {...}, "networks": [{"network_id", "catalog", "collections"}]}

    Logical key layout (inside a generation):
        stac:catalog                          -> root (network_ids only, no directly-attached collections)
        stac:network:{nid}                    -> Network subcatalog
        stac:network:{nid}:collection:{cid}   -> Network-scoped Collection
        stac:network:{nid}:item:{cid}:{iid}   -> Network-scoped Item
    """
    flat: Dict[str, Any] = {"stac:catalog": root_dict["catalog"]}

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

    values = {key: json.dumps(value, default=str) for key, value in flat.items()}
    values["stac:meta:availability"] = json.dumps(True)
    values["stac:meta:last_harvested_at"] = datetime.now(timezone.utc).isoformat()
    _publish(_STAC, values)

    logger.info(
        "STAC network cache written to Redis: %d keys (1 catalog, "
        "%d Networks, %d network collections, %d network items)",
        len(flat), len(root_dict.get("networks", [])),
        net_collections, net_items,
    )


# DCAT-AP reads and writes
#
# Each scope (root / one graph per Network) is cached as one whole
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
    get_dcat_network for the scopes that carry data.
    """
    raw = _get(_DCAT, _dcat_root_key())
    if raw is None:
        return None
    return raw.decode("utf-8") if isinstance(raw, bytes) else raw


async def get_dcat_root_all() -> Optional[str]:
    """
    Return the cached root DCAT-AP Turtle document with closed Networks
    included in hasPart/dcat:catalog, or None if NETWORK=0 (this scope is
    only ever written by write_dcat_catalog_with_networks) or no harvest
    cycle has written it yet.

    Intended only for authenticated callers -- see api.py's dcat_root,
    which mirrors stac_transformer.py's closed_network_ids reveal rule.
    """
    raw = _get(_DCAT, _dcat_root_all_key())
    if raw is None:
        return None
    return raw.decode("utf-8") if isinstance(raw, bytes) else raw


async def get_dcat_network(network_id) -> Optional[str]:
    """
    Return the cached DCAT-AP Turtle document for one Network's sub-catalog,
    or None if this network_id doesn't exist, NETWORK=0, or no harvest cycle
    has completed yet.
    """
    raw = _get(_DCAT, _dcat_network_key(network_id))
    if raw is None:
        return None
    return raw.decode("utf-8") if isinstance(raw, bytes) else raw


# JSON-LD readers -- same scopes as the Turtle readers above, same
# None-if-missing contract, just the sibling ":jsonld" key.

async def get_dcat_root_jsonld() -> Optional[str]:
    """Return the cached root DCAT-AP JSON-LD document, or None if no
    harvest cycle has written it yet. Same scope rules as get_dcat_root."""
    raw = _get(_DCAT, _dcat_root_jsonld_key())
    if raw is None:
        return None
    return raw.decode("utf-8") if isinstance(raw, bytes) else raw


async def get_dcat_root_all_jsonld() -> Optional[str]:
    """JSON-LD sibling of get_dcat_root_all. Same None-if-missing contract,
    same NETWORK=1-only / authenticated-callers-only scope."""
    raw = _get(_DCAT, _dcat_root_all_jsonld_key())
    if raw is None:
        return None
    return raw.decode("utf-8") if isinstance(raw, bytes) else raw


async def get_dcat_network_jsonld(network_id) -> Optional[str]:
    """Return the cached DCAT-AP JSON-LD document for one Network's
    sub-catalog, or None if this network_id doesn't exist, NETWORK=0, or
    no harvest cycle has completed yet. Same scope rules as
    get_dcat_network."""
    raw = _get(_DCAT, _dcat_network_jsonld_key(network_id))
    if raw is None:
        return None
    return raw.decode("utf-8") if isinstance(raw, bytes) else raw


def _serialize_scope(graph: Graph) -> tuple[bytes, bytes]:
    """Turtle and JSON-LD bytes for one scope. Turtle goes through
    serialization.serialize_turtle, which avoids rdflib's quadratic
    prefix lookup on large graphs (see that module)."""
    return serialize_turtle(graph), graph.serialize(format="json-ld", encoding="utf-8")


def write_dcat_catalog(result: Dict[str, Graph]) -> None:
    """
    Serialize the NETWORK=0 DCAT-AP graph and publish it as a new generation.

    result is build_dcat_catalog()'s direct output: {"root": Graph}.
    Serialization is CPU-bound and happens entirely before any Redis write;
    if it raises, nothing is written and the previous catalog stays served.
    Serialize time and Redis-write time are timed and logged separately.
    """
    serialize_start = time.monotonic()
    root_ttl, root_jsonld = _serialize_scope(result["root"])
    serialize_elapsed = time.monotonic() - serialize_start

    write_start = time.monotonic()
    _publish(_DCAT, {
        _dcat_root_key(): root_ttl,
        _dcat_root_jsonld_key(): root_jsonld,
        "dcat:meta:availability": json.dumps(True),
        "dcat:meta:last_harvested_at": datetime.now(timezone.utc).isoformat(),
    })
    write_elapsed = time.monotonic() - write_start

    logger.info(
        "DCAT cache written to Redis: 1 root graph (%d triples) -- "
        "serialize=%.3fs, redis_write=%.3fs",
        len(result["root"]), serialize_elapsed, write_elapsed,
    )


def write_dcat_catalog_with_networks(result: Dict[str, Any]) -> None:
    """
    Serialize the NETWORK=1 DCAT-AP graphs and publish them as one generation.

    result is build_dcat_catalog_with_networks()'s output:
        {"root": Graph, "root_all": Graph, "networks": {network_id: Graph, ...}}

    Logical key layout (inside a generation):
        dcat:graph:root        -> structural-only root (Catalog + DataService),
                                   closed Networks omitted from hasPart/dcat:catalog
        dcat:graph:root:all    -> same root scope, closed Networks included --
                                   served only to authenticated callers, see
                                   api.py's dcat_root/dcat_root_ttl
        dcat:graph:net-{id}    -> one per Network (full Dataset/DatasetSeries content)

    Same order as write_dcat_catalog: serialize every scope, then write, then
    swap; serialize and Redis-write times are logged separately.
    """
    serialize_start = time.monotonic()
    root = _serialize_scope(result["root"])
    root_all = _serialize_scope(result["root_all"])
    networks = {nid: _serialize_scope(g) for nid, g in result["networks"].items()}
    serialize_elapsed = time.monotonic() - serialize_start

    values: Dict[str, Any] = {
        _dcat_root_key(): root[0],
        _dcat_root_jsonld_key(): root[1],
        _dcat_root_all_key(): root_all[0],
        _dcat_root_all_jsonld_key(): root_all[1],
    }
    for network_id, (ttl, jsonld) in networks.items():
        values[_dcat_network_key(network_id)] = ttl
        values[_dcat_network_jsonld_key(network_id)] = jsonld
    values["dcat:meta:availability"] = json.dumps(True)
    values["dcat:meta:last_harvested_at"] = datetime.now(timezone.utc).isoformat()

    write_start = time.monotonic()
    _publish(_DCAT, values)
    write_elapsed = time.monotonic() - write_start

    logger.info(
        "DCAT network cache written to Redis: 1 root graph (%d triples), "
        "1 root-all graph (%d triples), %d Network graphs -- "
        "serialize=%.3fs, redis_write=%.3fs",
        len(result["root"]), len(result["root_all"]), len(networks),
        serialize_elapsed, write_elapsed,
    )
