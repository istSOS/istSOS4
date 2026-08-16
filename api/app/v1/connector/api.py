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
Connector API router.

Mounted at {SUBPATH}{VERSION}/connector by api/app/v1/api.py. Pure reader
for catalog contents: every route reads the already-transformed catalog
from cache.py (Redis, written once per harvest cycle by scheduler.py) and
serves it as-is -- no route here runs the harvester or calls a transformer
directly. Gated routes validate bearer tokens via auth_gate.gate().

STAC (stac_transformer.py) is stored flat in Redis -- stac:catalog,
stac:collection:{id}, stac:item:{coll_id}:{id} -- each already carrying its
own navigation links. "collection_ids"/"item_ids" are internal tracking
lists, stripped from every response. The /stac/collections and
/stac/collections/{id}/items envelopes are synthetic wrappers assembled
here at request time, so only their own top-level "links" are built here.

DCAT (dcat_transformer.py) is cached one whole document per scope --
dcat:graph:root|orphan|net-{id} -- each in both Turtle and JSON-LD (a
sibling ":jsonld" key), so every /dcat/* route is a plain Redis read
served verbatim, no reconstruction.
"""

import functools
from typing import Optional

from app import HOSTNAME, SUBPATH, VERSION
from app.v1.connector.auth_gate import gate
from app.oauth import get_current_user_optional
from app.v1.connector.cache import (
    get_catalog,
    get_collection,
    get_item,
    get_stac_metadata,
    get_network_catalog,
    get_network_collection,
    get_network_item,
    get_dcat_metadata,
    get_dcat_root,
    get_dcat_root_jsonld,
    get_dcat_root_all,
    get_dcat_root_all_jsonld,
    get_dcat_orphan,
    get_dcat_orphan_jsonld,
    get_dcat_network,
    get_dcat_network_jsonld,
)

from app.v1.connector.config import get_settings, STAC_TRANSFORMER, DCAT_TRANSFORMER
from app.v1.connector.utils import catch_errors, error_response

from fastapi import APIRouter, Depends, status, Request
from fastapi.responses import JSONResponse, Response

v1 = APIRouter()
settings = get_settings()


_STAC_ROOT_HREF = f"{HOSTNAME}{SUBPATH}{VERSION}/connector/stac"


def _cache_unavailable(detail: str) -> JSONResponse:
    """
    Standard 503 response for when the cache has not been written yet
    by a harvest cycle -- not an error, just "ask again after the next
    scheduled run."
    """
    return error_response(status.HTTP_503_SERVICE_UNAVAILABLE, detail)


def _not_found(detail: str) -> JSONResponse:
    return error_response(status.HTTP_404_NOT_FOUND, detail)


def _disabled(env_var: str) -> JSONResponse:
    """404 response for a standard whose master switch is off."""
    return _not_found(
        f"This connector standard is disabled. Set {env_var}=1 to enable it."
    )


def _require_enabled(flag: bool, env_var: str):
    """Route decorator gating an endpoint on a master-switch flag."""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            if not flag:
                return _disabled(env_var)
            return await func(*args, **kwargs)
        return wrapper
    return decorator


async def _collection_envelope(coll: Optional[dict], collection_id: str):
    if coll is None:
        return _not_found(f"Collection '{collection_id}' not found.")
    return {k: v for k, v in coll.items() if k != "item_ids"}


async def _items_envelope(coll: Optional[dict], collection_id: str, collection_href: str, item_fetch):
    if coll is None:
        return _not_found(f"Collection '{collection_id}' not found.")
    items = []
    for iid in coll.get("item_ids", []):
        item = await item_fetch(iid)
        if item is not None:
            items.append(item)
    return {
        "type": "FeatureCollection",
        "features": items,
        "links": [
            {"rel": "self", "href": f"{collection_href}/items", "type": "application/geo+json"},
            {"rel": "collection", "href": collection_href, "type": "application/json"},
            {"rel": "root", "href": _STAC_ROOT_HREF, "type": "application/json"},
        ],
    }


async def _item_envelope(coll: Optional[dict], collection_id: str, item_id: str, item_fetch):
    if coll is None:
        return _not_found(f"Collection '{collection_id}' not found.")
    item = await item_fetch(item_id)
    if item is None:
        return _not_found(f"Item '{item_id}' not found in collection '{collection_id}'.")
    return item


@v1.get("")
async def get_connector_root(request: Request):
    current_path = request.url.path.rstrip("/")
    base_url = f"{request.url.scheme}://{request.url.netloc}"
    
    stac_meta = get_stac_metadata()
    dcat_meta = get_dcat_metadata()

    return {
        "stac_enabled": STAC_TRANSFORMER,
        "stac_availability": stac_meta["stac_availability"],
        "stac_url": f"{base_url}{current_path}/stac",

        "dcat_enabled": DCAT_TRANSFORMER,
        "dcat_mandatory_fields_set": settings.has_mandatory_dcat_fields,
        "dcat_availability": dcat_meta["dcat_availability"],
        "dcat_url_jsonld": f"{base_url}{current_path}/dcat/root",
        "dcat_url_ttl": f"{base_url}{current_path}/dcat/root.ttl",
        "dcat_network_ids": dcat_meta["network_ids"],

        "harvester_interval_minutes": settings.HARVEST_INTERVAL_MINUTES,
        "last_fetch": stac_meta["last_fetch"] or dcat_meta["last_fetch"],
    }


@v1.api_route(
    "/stac",
    methods=["GET"],
    tags=["STAC"],
    summary="STAC root Catalog",
    description=(
        "Entry point for any STAC client, including the eoAPI STAC browser. "
        "Returns the cached STAC 1.0 root Catalog, with child links for "
        "every Collection."
    ),
    status_code=status.HTTP_200_OK,
)
@_require_enabled(STAC_TRANSFORMER, "STAC_TRANSFORMER")
@catch_errors
async def stac_root(
    gate_result: Optional[JSONResponse] = Depends(gate),
    current_user: Optional[dict] = Depends(get_current_user_optional),
):
    if gate_result is not None:
        return gate_result
    catalog = await get_catalog()
    if catalog is None:
        return _cache_unavailable(
            "STAC catalog has not been generated yet. "
            "Try again after the next scheduled harvest cycle."
        )

    result = {k: v for k, v in catalog.items() if k not in ("collection_ids", "network_ids", "closed_network_ids")}

    # Closed networks are stripped from "links" at cache-write time (no
    # per-request auth context there); an authenticated caller still gets
    # the same closed-network gate bypass everywhere else (see
    # auth_gate.gate), so re-add their child links here for consistency --
    # otherwise an authenticated user could reach /stac/{id} directly but
    # never discover it existed from root.
    if current_user is not None:
        closed_links = [
            {"rel": "child", "href": f"{_STAC_ROOT_HREF}/{nid}", "type": "application/json"}
            for nid in catalog.get("closed_network_ids", [])
        ]
        result["links"] = [*result.get("links", []), *closed_links]

    return result


@v1.api_route(
    "/stac/collections",
    methods=["GET"],
    tags=["STAC"],
    summary="All STAC Collections",
    description=(
        "Returns the standard STAC Collections response envelope -- one "
        "Collection per Thing in the harvested catalog."
    ),
    status_code=status.HTTP_200_OK,
)
@_require_enabled(STAC_TRANSFORMER, "STAC_TRANSFORMER")
@catch_errors
async def stac_collections(gate_result: Optional[JSONResponse] = Depends(gate)):
    if gate_result is not None:
        return gate_result
    catalog = await get_catalog()
    if catalog is None:
        return _cache_unavailable(
            "STAC catalog has not been generated yet. "
            "Try again after the next scheduled harvest cycle."
        )

    collection_ids = catalog.get("collection_ids", [])

    collections = []
    for cid in collection_ids:
        coll = await get_collection(cid)
        if coll is None:
            # Transient mid-write miss: skip rather than 503 the whole
            # response. The next harvest cycle will make it consistent.
            continue

        collections.append(
            {k: v for k, v in coll.items() if k != "item_ids"}
        )

    return {
        "collections": collections,
        "links": [
            {
                "rel": "self",
                "href": f"{_STAC_ROOT_HREF}/collections",
                "type": "application/json",
            }
        ],
    }


@v1.api_route(
    "/stac/collections/{collection_id}",
    methods=["GET"],
    tags=["STAC"],
    summary="Single STAC Collection",
    description="Returns one STAC Collection identified by collection_id (format: thing-{id}).",
    status_code=status.HTTP_200_OK,
)
@_require_enabled(STAC_TRANSFORMER, "STAC_TRANSFORMER")
@catch_errors
async def stac_collection(collection_id: str, gate_result: Optional[JSONResponse] = Depends(gate)):
    if gate_result is not None:
        return gate_result
    return await _collection_envelope(await get_collection(collection_id), collection_id)


@v1.api_route(
    "/stac/collections/{collection_id}/items",
    methods=["GET"],
    tags=["STAC"],
    summary="All STAC Items in a Collection",
    description=(
        "Returns a GeoJSON FeatureCollection -- one Item per Datastream on "
        "the Thing backing this Collection."
    ),
    status_code=status.HTTP_200_OK,
)
@_require_enabled(STAC_TRANSFORMER, "STAC_TRANSFORMER")
@catch_errors
async def stac_items(collection_id: str, gate_result: Optional[JSONResponse] = Depends(gate)):
    if gate_result is not None:
        return gate_result
    coll = await get_collection(collection_id)
    collection_href = f"{_STAC_ROOT_HREF}/collections/{collection_id}"
    return await _items_envelope(coll, collection_id, collection_href, item_fetch=lambda iid: get_item(collection_id, iid))


@v1.api_route(
    "/stac/collections/{collection_id}/items/{item_id}",
    methods=["GET"],
    tags=["STAC"],
    summary="Single STAC Item",
    description="Returns one STAC Item identified by item_id (format: datastream-{id}).",
    status_code=status.HTTP_200_OK,
)
@_require_enabled(STAC_TRANSFORMER, "STAC_TRANSFORMER")
@catch_errors
async def stac_item(collection_id: str, item_id: str, gate_result: Optional[JSONResponse] = Depends(gate)):
    if gate_result is not None:
        return gate_result
    coll = await get_collection(collection_id)
    return await _item_envelope(coll, collection_id, item_id, item_fetch=lambda iid: get_item(collection_id, iid))


# Network paths
@v1.api_route(
    "/stac/{network_id}", methods=["GET"], tags=["STAC"],
    summary="STAC Network subcatalog",
    description=(
        "Cached STAC 1.0 subcatalog for one Network -- only populated when "
        "NETWORK is enabled. Lists every Thing with >=1 Datastream in this "
        "Network; present even if empty."
    ),
    status_code=status.HTTP_200_OK,
)
@_require_enabled(STAC_TRANSFORMER, "STAC_TRANSFORMER")
@catch_errors
async def stac_network_root(network_id: int, gate_result: Optional[JSONResponse] = Depends(gate)):
    if gate_result is not None:
        return gate_result
    net_catalog = await get_network_catalog(network_id)
    if net_catalog is None:
        return _not_found(f"Network '{network_id}' not found.")
    return {k: v for k, v in net_catalog.items() if k != "collection_ids"}


@v1.api_route("/stac/{network_id}/collections/{collection_id}", methods=["GET"], tags=["STAC"], status_code=status.HTTP_200_OK)
@_require_enabled(STAC_TRANSFORMER, "STAC_TRANSFORMER")
@catch_errors
async def stac_network_collection(network_id: int, collection_id: str, gate_result: Optional[JSONResponse] = Depends(gate)):
    if gate_result is not None:
        return gate_result
    coll = await get_network_collection(network_id, collection_id)
    return await _collection_envelope(coll, collection_id)


@v1.api_route("/stac/{network_id}/collections/{collection_id}/items", methods=["GET"], tags=["STAC"], status_code=status.HTTP_200_OK)
@_require_enabled(STAC_TRANSFORMER, "STAC_TRANSFORMER")
@catch_errors
async def stac_network_items(network_id: int, collection_id: str, gate_result: Optional[JSONResponse] = Depends(gate)):
    if gate_result is not None:
        return gate_result
    coll = await get_network_collection(network_id, collection_id)
    collection_href = f"{_STAC_ROOT_HREF}/{network_id}/collections/{collection_id}"
    return await _items_envelope(
        coll, collection_id, collection_href,
        item_fetch=lambda iid: get_network_item(network_id, collection_id, iid),
    )


@v1.api_route("/stac/{network_id}/collections/{collection_id}/items/{item_id}", methods=["GET"], tags=["STAC"], status_code=status.HTTP_200_OK)
@_require_enabled(STAC_TRANSFORMER, "STAC_TRANSFORMER")
@catch_errors
async def stac_network_item(network_id: int, collection_id: str, item_id: str, gate_result: Optional[JSONResponse] = Depends(gate)):
    if gate_result is not None:
        return gate_result
    coll = await get_network_collection(network_id, collection_id)
    return await _item_envelope(
        coll, collection_id, item_id,
        item_fetch=lambda iid: get_network_item(network_id, collection_id, iid),
    )


# DCAT-AP 3.0 routes
#
# Pure reader, same rule as every STAC route above: these serve cached
# text as-is. No route here touches Postgres, runs the harvester, or calls
# dcat_transformer.py directly.
#
# Each scope is exposed at two paths, format picked by the extension:
#   /dcat/root            JSON-LD (default -- the more broadly consumed format)
#   /dcat/root.ttl        Turtle
#   /dcat/orphan[.ttl]     same pattern
#   /dcat/{network_id}[.ttl]   same pattern
#
# The plain (no-suffix) and ".ttl" routes for a given scope are declared
# back-to-back below and share one cache-miss/error handling shape. Two
# ordering rules matter here, both because Starlette matches routes in
# registration order and the default path-param converter matches any
# segment without a "/" -- including one with a dot in it:
#   1. static "/dcat/root" and "/dcat/orphan" are registered before the
#      dynamic "/dcat/{network_id}" routes, so network_id never swallows them.
#   2. "/dcat/{network_id}.ttl" is registered before "/dcat/{network_id}",
#      since {network_id} alone would otherwise match "1.ttl" as its
#      capture and fail int coercion, never reaching the .ttl route.

_TURTLE_MEDIA_TYPE = "text/turtle"
_JSONLD_MEDIA_TYPE = "application/ld+json"


def _turtle_response(body: str) -> Response:
    return Response(content=body, media_type=_TURTLE_MEDIA_TYPE, status_code=status.HTTP_200_OK)


def _jsonld_response(body: str) -> Response:
    return Response(content=body, media_type=_JSONLD_MEDIA_TYPE, status_code=status.HTTP_200_OK)


# DCAT's cache-miss/not-found responses are the exact same envelope shape
# as STAC's above (503 "not written yet" / 404 "not found") -- they used
# to be separate byte-for-byte-identical functions (_turtle_unavailable,
# _turtle_not_found); aliased instead of duplicated now.
_turtle_unavailable = _cache_unavailable
_turtle_not_found = _not_found


@v1.api_route(
    "/dcat/root",
    methods=["GET"],
    tags=["DCAT"],
    summary="Root DCAT-AP 3.0 Catalog (JSON-LD)",
    description=(
        "Returns the cached root dcat:Catalog as JSON-LD. Under NETWORK=0 "
        "this carries every DatasetSeries and Dataset directly. Under "
        "NETWORK=1 this is structural only (Catalog + DataService + "
        "dct:hasPart links) -- fetch /dcat/orphan and /dcat/{network_id} "
        "for the scopes that carry Dataset content. Closed Networks are "
        "omitted from hasPart/dcat:catalog unless the caller is "
        "authenticated (same reveal rule as /stac's root Catalog). "
        "See /dcat/root.ttl for the same document as Turtle."
    ),
    status_code=status.HTTP_200_OK,
)
@_require_enabled(DCAT_TRANSFORMER, "DCAT_TRANSFORMER")
@catch_errors
async def dcat_root(
    gate_result: Optional[JSONResponse] = Depends(gate),
    current_user: Optional[dict] = Depends(get_current_user_optional),
):
    if gate_result is not None:
        return gate_result
    jsonld = None
    if current_user is not None:
        # NETWORK=1 writes a "root:all" scope for exactly this case; under
        # NETWORK=0 no such scope exists (no Networks to hide in the first
        # place), so this falls through to the plain root below -- same
        # content either way when nothing is closed.
        jsonld = await get_dcat_root_all_jsonld()
    if jsonld is None:
        jsonld = await get_dcat_root_jsonld()
    if jsonld is None:
        return _turtle_unavailable(
            "DCAT catalog has not been generated yet. "
            "Try again after the next scheduled harvest cycle."
        )
    return _jsonld_response(jsonld)


@v1.api_route(
    "/dcat/root.ttl",
    methods=["GET"],
    tags=["DCAT"],
    summary="Root DCAT-AP 3.0 Catalog (Turtle)",
    description="Same document as /dcat/root, serialized as Turtle instead of JSON-LD.",
    status_code=status.HTTP_200_OK,
)
@_require_enabled(DCAT_TRANSFORMER, "DCAT_TRANSFORMER")
@catch_errors
async def dcat_root_ttl(
    gate_result: Optional[JSONResponse] = Depends(gate),
    current_user: Optional[dict] = Depends(get_current_user_optional),
):
    if gate_result is not None:
        return gate_result
    turtle = None
    if current_user is not None:
        turtle = await get_dcat_root_all()
    if turtle is None:
        turtle = await get_dcat_root()
    if turtle is None:
        return _turtle_unavailable(
            "DCAT catalog has not been generated yet. "
            "Try again after the next scheduled harvest cycle."
        )
    return _turtle_response(turtle)


@v1.api_route(
    "/dcat/orphan",
    methods=["GET"],
    tags=["DCAT"],
    summary="Orphan-scope DCAT-AP 3.0 Catalog (JSON-LD)",
    description=(
        "Returns the cached orphan-scope dcat:Catalog as JSON-LD -- "
        "Datastreams with no assigned Network. Only populated when "
        "NETWORK=1; returns 404 under NETWORK=0. See /dcat/orphan.ttl "
        "for the same document as Turtle."
    ),
    status_code=status.HTTP_200_OK,
)
@_require_enabled(DCAT_TRANSFORMER, "DCAT_TRANSFORMER")
@catch_errors
async def dcat_orphan(gate_result: Optional[JSONResponse] = Depends(gate)):
    if gate_result is not None:
        return gate_result
    jsonld = await get_dcat_orphan_jsonld()
    if jsonld is None:
        return _turtle_not_found(
            "No orphan DCAT catalog is available. This deployment may "
            "be running with NETWORK=0, or no harvest cycle has "
            "completed yet."
        )
    return _jsonld_response(jsonld)


@v1.api_route(
    "/dcat/orphan.ttl",
    methods=["GET"],
    tags=["DCAT"],
    summary="Orphan-scope DCAT-AP 3.0 Catalog (Turtle)",
    description="Same document as /dcat/orphan, serialized as Turtle instead of JSON-LD.",
    status_code=status.HTTP_200_OK,
)
@_require_enabled(DCAT_TRANSFORMER, "DCAT_TRANSFORMER")
@catch_errors
async def dcat_orphan_ttl(gate_result: Optional[JSONResponse] = Depends(gate)):
    if gate_result is not None:
        return gate_result
    turtle = await get_dcat_orphan()
    if turtle is None:
        return _turtle_not_found(
            "No orphan DCAT catalog is available. This deployment may "
            "be running with NETWORK=0, or no harvest cycle has "
            "completed yet."
        )
    return _turtle_response(turtle)


@v1.api_route(
    "/dcat/{network_id}.ttl",
    methods=["GET"],
    tags=["DCAT"],
    summary="Network DCAT-AP 3.0 sub-catalog (Turtle)",
    description="Same document as /dcat/{network_id}, serialized as Turtle instead of JSON-LD.",
    status_code=status.HTTP_200_OK,
)
@_require_enabled(DCAT_TRANSFORMER, "DCAT_TRANSFORMER")
@catch_errors
async def dcat_network_ttl(network_id: int, gate_result: Optional[JSONResponse] = Depends(gate)):
    if gate_result is not None:
        return gate_result
    turtle = await get_dcat_network(network_id)
    if turtle is None:
        return _turtle_not_found(f"Network '{network_id}' not found.")
    return _turtle_response(turtle)


@v1.api_route(
    "/dcat/{network_id}",
    methods=["GET"],
    tags=["DCAT"],
    summary="Network DCAT-AP 3.0 sub-catalog (JSON-LD)",
    description=(
        "Returns the cached DCAT-AP JSON-LD document for one Network's "
        "sub-catalog. Only populated when NETWORK=1; returns 404 if this "
        "network_id doesn't exist or no harvest cycle has completed yet. "
        "See /dcat/{network_id}.ttl for the same document as Turtle."
    ),
    status_code=status.HTTP_200_OK,
)
@_require_enabled(DCAT_TRANSFORMER, "DCAT_TRANSFORMER")
@catch_errors
async def dcat_network(network_id: int, gate_result: Optional[JSONResponse] = Depends(gate)):
    if gate_result is not None:
        return gate_result
    jsonld = await get_dcat_network_jsonld(network_id)
    if jsonld is None:
        return _turtle_not_found(f"Network '{network_id}' not found.")
    return _jsonld_response(jsonld)
