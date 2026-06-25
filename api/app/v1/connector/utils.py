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
Cache-key flattening utilities for the STAC catalog.

Converts a deeply nested STAC catalog tree into a flat dictionary of keys and 
values. This allows the API to fetch individual Catalogs, Collections, or Items 
instantly without needing to load or parse the entire catalog tree.

Cache key scheme:
    stac:catalog
    stac:collection:{collection_id}
    stac:item:{collection_id}:{item_id}

Items are intentionally namespaced under their parent collection ID to ensure 
fast lookups without requiring a secondary index or slow search scans.
"""

from __future__ import annotations

from typing import Any

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
