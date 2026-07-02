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

Mounted at {SUBPATH}{VERSION}/connector by api/app/v1/api.py, the same way
main.py mounts the core STA router at {SUBPATH}{VERSION}.

Pure reader: every route here reads the already-transformed catalog from
cache.py (Redis, written once per harvest cycle by scheduler.py) and
serves it as a STAC-compliant response. No route in this file touches 
Postgres, runs the harvester, or calls stac_transformer.py directly.

Cache shape vs. STAC spec:
    cache.py stores three flat entity types in Redis:
        stac:catalog              -> catalog metadata + "collection_ids" list + "links"
        stac:collection:{id}      -> collection metadata + "item_ids" list + "links"
        stac:item:{coll_id}:{id}  -> full Item dict (already has "links")

    "collection_ids" and "item_ids" are internal tracking lists used to
    enumerate children without loading the full tree. They are NOT part of
    the STAC spec and are stripped from every outgoing response.

    All STAC navigation links (self, root, parent, child, item, collection)
    are built once by stac_transformer.py at harvest time and cached
    verbatim.

    The two exceptions are the /stac/collections listing and
    /stac/collections/{id}/items envelopes: these are synthetic wrappers
    api.py assembles at request time (a FeatureCollection and a Collections
    listing are not themselves cached entities), so their own top-level
    "links" arrays are built here. The Collection and Item objects nested
    inside those envelopes are still served with their cached links as-is.

For now only /connector/stac is live. /connector/dcat will follow the
same pattern once dcat_transformer.py lands.
"""

from app import HOSTNAME, SUBPATH, VERSION
from app.v1.connector.cache import get_catalog, get_collection, get_item, get_stac_metadata
from app.v1.connector.config import get_settings

from fastapi import APIRouter, status, Request
from fastapi.responses import JSONResponse

v1 = APIRouter()
cache = get_settings()

_STAC_ROOT_HREF = f"{HOSTNAME}{SUBPATH}{VERSION}/connector/stac"


def _cache_unavailable(detail: str) -> JSONResponse:
    """
    Standard 503 response for when the cache has not been written yet
    by a harvest cycle -- not an error, just "ask again after the next
    scheduled run."
    """
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={
            "code": 503,
            "type": "error",
            "message": detail,
        },
    )


def _not_found(detail: str) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={
            "code": 404,
            "type": "error",
            "message": detail,
        },
    )


@v1.get("")
async def get_connector_root(request: Request):
    current_path = request.url.path.rstrip("/")
    base_url = f"{request.url.scheme}://{request.url.netloc}"
    
    meta = get_stac_metadata()
    
    return {
        "stac_availability": meta["stac_availability"],
        "stac_url": f"{base_url}{current_path}/stac",
        
        "dcat_availability": False, 
        "dcat_url": {
            "json": f"{base_url}{current_path}/dcat",
            "ttl": f"{base_url}{current_path}/dcat.ttl"
        },
        
        "harvester_interval_minutes": cache.HARVEST_INTERVAL_MINUTES,
        "last_fetch": meta["last_fetch"]
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
async def stac_root():
    try:
        catalog = await get_catalog()
        if catalog is None:
            return _cache_unavailable(
                "STAC catalog has not been generated yet. "
                "Try again after the next scheduled harvest cycle."
            )

        return {
            k: v for k, v in catalog.items() if k != "collection_ids"
        }

    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )


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
async def stac_collections():
    try:
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
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )


@v1.api_route(
    "/stac/collections/{collection_id}",
    methods=["GET"],
    tags=["STAC"],
    summary="Single STAC Collection",
    description="Returns one STAC Collection identified by collection_id (format: thing-{id}).",
    status_code=status.HTTP_200_OK,
)
async def stac_collection(collection_id: str):
    try:
        coll = await get_collection(collection_id)
        if coll is None:
            return _not_found(f"Collection '{collection_id}' not found.")

        return {k: v for k, v in coll.items() if k != "item_ids"}

    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )


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
async def stac_items(collection_id: str):
    try:
        coll = await get_collection(collection_id)
        if coll is None:
            return _not_found(f"Collection '{collection_id}' not found.")

        item_ids = coll.get("item_ids", [])
        items = []
        for iid in item_ids:
            item = await get_item(collection_id, iid)
            if item is None:
                # Transient mid-write miss: skip.
                continue
            items.append(item)

        collection_href = f"{_STAC_ROOT_HREF}/collections/{collection_id}"
        return {
            "type": "FeatureCollection",
            "features": items,
            "links": [
                {
                    "rel": "self",
                    "href": f"{collection_href}/items",
                    "type": "application/geo+json",
                },
                {
                    "rel": "collection",
                    "href": collection_href,
                    "type": "application/json",
                },
                {
                    "rel": "root",
                    "href": _STAC_ROOT_HREF,
                    "type": "application/json",
                },
            ],
        }
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )


@v1.api_route(
    "/stac/collections/{collection_id}/items/{item_id}",
    methods=["GET"],
    tags=["STAC"],
    summary="Single STAC Item",
    description="Returns one STAC Item identified by item_id (format: datastream-{id}).",
    status_code=status.HTTP_200_OK,
)
async def stac_item(collection_id: str, item_id: str):
    try:
        coll = await get_collection(collection_id)
        if coll is None:
            return _not_found(f"Collection '{collection_id}' not found.")

        item = await get_item(collection_id, item_id)
        if item is None:
            return _not_found(
                f"Item '{item_id}' not found in collection '{collection_id}'."
            )

        return item

    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )
