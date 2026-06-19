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

Pure reader: every route here reads the already-transformed catalog dict
back from cache.py (currently a JSON file on disk, written once per
harvest cycle by scheduler.py) and slices it for the response. No route
in this file touches Postgres, runs the harvester, or calls
stac_transformer.py directly -- that work happens once per cycle in
scheduler.py, not per request.

For now only /connector/stac is live. /connector/dcat will follow the
same pattern once dcat_transformer.py lands.
"""

from app import HOSTNAME, SUBPATH, VERSION
from app.v1.connector.cache import get_stac
from app.v1.connector.config import get_settings

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse

v1 = APIRouter()

_STAC_ROOT_HREF = f"{HOSTNAME}{SUBPATH}{VERSION}/connector/stac"


def _cache_unavailable(detail: str) -> JSONResponse:
    """
    Standard 503 response for when the cache file has not been written yet
    by a harvest cycle which is not an error, just "ask again after the next
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
async def stac_root(config=Depends(get_settings)):
    try:
        catalog = await get_stac(config)
        if catalog is None:
            return _cache_unavailable(
                "STAC catalog has not been generated yet. "
                "Try again after the next scheduled harvest cycle."
            )

        root = {k: v for k, v in catalog.items() if k != "collections"}
        return root
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
async def stac_collections(config=Depends(get_settings)):
    try:
        catalog = await get_stac(config)
        if catalog is None:
            return _cache_unavailable(
                "STAC catalog has not been generated yet. "
                "Try again after the next scheduled harvest cycle."
            )

        collections = catalog.get("collections", [])
        return {
            "collections": [
                {k: v for k, v in c.items() if k != "items"} for c in collections
            ],
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
async def stac_collection(collection_id: str, config=Depends(get_settings)):
    try:
        catalog = await get_stac(config)
        if catalog is None:
            return _cache_unavailable(
                "STAC catalog has not been generated yet. "
                "Try again after the next scheduled harvest cycle."
            )

        collections = catalog.get("collections", [])
        match = next((c for c in collections if c["id"] == collection_id), None)
        if match is None:
            return _not_found(f"Collection '{collection_id}' not found.")

        return {k: v for k, v in match.items() if k != "items"}
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
async def stac_items(collection_id: str, config=Depends(get_settings)):
    try:
        catalog = await get_stac(config)
        if catalog is None:
            return _cache_unavailable(
                "STAC catalog has not been generated yet. "
                "Try again after the next scheduled harvest cycle."
            )

        collections = catalog.get("collections", [])
        match = next((c for c in collections if c["id"] == collection_id), None)
        if match is None:
            return _not_found(f"Collection '{collection_id}' not found.")

        items = match.get("items", [])
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
async def stac_item(collection_id: str, item_id: str, config=Depends(get_settings)):
    try:
        catalog = await get_stac(config)
        if catalog is None:
            return _cache_unavailable(
                "STAC catalog has not been generated yet. "
                "Try again after the next scheduled harvest cycle."
            )

        collections = catalog.get("collections", [])
        match = next((c for c in collections if c["id"] == collection_id), None)
        if match is None:
            return _not_found(f"Collection '{collection_id}' not found.")

        items = match.get("items", [])
        item_match = next((it for it in items if it["id"] == item_id), None)
        if item_match is None:
            return _not_found(
                f"Item '{item_id}' not found in collection '{collection_id}'."
            )

        return item_match
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )
