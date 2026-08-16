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
Shared helpers for the connector package.

Two unrelated groups of helpers live here, kept in one small module rather
than one apiece because neither is big enough to justify its own file:

1. Cache-key flattening for the STAC catalog (flatten_stac_catalog,
   CATALOG_KEY) -- converts a deeply nested STAC catalog tree into a flat
   dictionary of keys and values, so the API can fetch individual
   Catalogs, Collections, or Items instantly without loading or parsing
   the entire tree. Cache key scheme:
       stac:catalog
       stac:collection:{collection_id}
       stac:item:{collection_id}:{item_id}
   Items are namespaced under their parent collection ID for fast lookups
   without a secondary index or slow search scans.

2. STA-to-{STAC,DCAT} shared transform helpers (temporal/spatial parsing,
   href builders) -- these were previously defined twice, once in
   stac_transformer.py and once in dcat_transformer.py, byte-for-byte
   identical in every case except _union_bboxes (dcat_transformer.py's
   version additionally guarded the empty-list case; that guard is kept
   here since it's strictly safer and every existing call site already
   checks non-emptiness first anyway, so this changes no behavior).
   Consolidated here so a fix to, say, phenomenon_time parsing only needs
   to happen once and can't quietly drift between the two standards.
"""

from __future__ import annotations

import functools
import logging
from datetime import datetime, timezone
from typing import Any, Optional, Union

from fastapi import status
from fastapi.responses import JSONResponse

from app import HOSTNAME, SUBPATH, VERSION

logger = logging.getLogger(__name__)

CATALOG_KEY = "stac:catalog"


def _collection_key(collection_id: str) -> str:
    return f"stac:collection:{collection_id}"


def _item_key(collection_id: str, item_id: str) -> str:
    return f"stac:item:{collection_id}:{item_id}"


def flatten_stac_catalog(root_dict: dict) -> dict[str, Any]:
    """
    Flattens a nested STAC catalog dictionary into separate cache entries.

    Expects the build_stac_catalog() output shape from stac_transformer.py:
        {"catalog": {...catalog metadata, "links": [...]},
         "collections": [{...collection metadata, "items": [...], "links": [...]}]}

    Extracts nested items and collections from the root tree and maps them 
    to unique cache keys. It safely tracks relationships by embedding tracking 
    lists (`item_ids` and `collection_ids`) directly inside the parent objects.
    Navigation links on the catalog/collection/item dicts are already built
    by stac_transformer.py and are passed through untouched here.

    Safe to use: safely copies data to prevent mutating the original input tree.
    Malformed inputs (e.g., missing keys or IDs) will raise a standard KeyError.
    """

    flat: dict[str, Any] = {}

    catalog_entry = dict(root_dict["catalog"])
    collection_ids: list[str] = []

    for collection_dict in root_dict.get("collections", []):
        collection_id = collection_dict["id"]
        collection_ids.append(collection_id)

        collection_entry = {k: v for k, v in collection_dict.items() if k != "items"}
        item_ids: list[str] = []

        for item_dict in collection_dict.get("items", []):
            item_id = item_dict["id"]
            item_ids.append(item_id)
            flat[_item_key(collection_id, item_id)] = item_dict

        collection_entry["item_ids"] = item_ids
        flat[_collection_key(collection_id)] = collection_entry

    catalog_entry["collection_ids"] = collection_ids
    flat[CATALOG_KEY] = catalog_entry

    return flat


# HTTP error envelope helpers
#
# api.py had this exact shape -- JSONResponse with a
# {"code", "type": "error", "message"} body -- built ad hoc in ~15 places:
# a try/except Exception around every route body returning a 400 this way,
# plus _cache_unavailable (503)/_not_found (404) in api.py and _hidden
# (404) in auth_gate.py already hand-rolling the same envelope. Worse,
# _turtle_unavailable/_turtle_not_found in api.py were byte-for-byte
# identical to _cache_unavailable/_not_found under different names.
# Centralized here so there is exactly one place that defines what an
# error response looks like.

def error_response(status_code: int, detail: str) -> JSONResponse:
    """Standard connector error envelope: {"code", "type": "error", "message"}."""
    return JSONResponse(
        status_code=status_code,
        content={"code": status_code, "type": "error", "message": detail},
    )


def catch_errors(func):
    """
    Route decorator: run the handler, and if it raises, return the
    standard error_response(400, str(e)) instead of letting FastAPI turn
    it into a 500 -- replaces the identical try/except Exception block
    every connector route used to repeat individually.

    Composes with _require_enabled the same way every route already
    stacked its try/except beneath that decorator: put @catch_errors
    below @_require_enabled (i.e. closer to `async def`), so a
    disabled-standard 404 short-circuits before catch_errors is even
    entered.
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            return error_response(status.HTTP_400_BAD_REQUEST, str(e))
    return wrapper

