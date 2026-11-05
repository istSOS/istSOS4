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

"""PUT (full replacement) support for the STA core entity update endpoints.

conformance: req/create-update-delete/update-entity-put (18-088 §10.3).

PUT replaces the whole entity, unlike PATCH which merges:

* every MANDATORY property MUST be supplied, otherwise the request is a 400
  (the old value is not silently kept);
* every OPTIONAL property the client omits is reset to null/default (PUT
  replaces, it does not merge);
* the entity id and selfLink are immutable, so they are never part of the body;
* mandatory RELATIONS are intentionally left untouched when the body does not
  mention them, so a PUT cannot orphan a required link (referential integrity);
  a relation that IS supplied is re-linked through the very same update
  machinery used by PATCH.

Each PUT handler expands the body with ``build_put_payload`` and then calls
the entity's PATCH handler, so PUT and PATCH share one code path.
"""

import json

from app.utils.utils import validate_required_keys
from app.v1.endpoints.exceptions import BadRequest
from fastapi import Request


# Keys the client may echo from a GET but cannot change: the entity id, its
# selfLink and the navigation links. 18-088 §10.3.1: the service ignores them.
READ_ONLY_SUFFIXES = ("@iot.id", "@iot.selfLink", "@iot.navigationLink")


async def parse_body(request: Request) -> dict:
    """Parse a PATCH/PUT body, which must be a single JSON object (the entity),
    dropping the read-only top-level keys.

    An empty body is returned as ``{}`` so PUT's mandatory-property check turns
    it into a 400 (rather than a 422), and PATCH treats it as a no-op.
    """
    raw = await request.body()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise BadRequest(f"Invalid JSON body: {exc}")
    if not isinstance(data, dict):
        raise BadRequest("Body must be a JSON object representing the entity.")
    return {
        key: value
        for key, value in data.items()
        if not key.endswith(READ_ONLY_SUFFIXES)
    }


def build_put_payload(payload: dict, required_keys, optional_keys) -> dict:
    """Expand a PUT body into a full-replacement payload.

    Mandatory properties must be present (``validate_required_keys`` -> 400 if
    not). Optional properties the client omitted are explicitly set to ``None``
    so the existing update path resets them (PUT replaces; it does not merge).
    Relations are deliberately not added here, so an omitted mandatory relation
    keeps its current value instead of being orphaned.
    """
    validate_required_keys(payload, required_keys)
    for key in optional_keys:
        payload.setdefault(key, None)
    return payload


def request_body_openapi_example(example: dict) -> dict:
    """Build an ``openapi_extra`` dict documenting a JSON request-body example.

    The PATCH and PUT handlers parse the body manually (``parse_body``), so FastAPI generates no requestBody /
    example for them in the OpenAPI schema. This re-attaches the original Swagger
    example through ``openapi_extra`` without touching the parsing path.
    """
    return {
        "requestBody": {
            "content": {
                "application/json": {
                    "schema": {"type": "object"},
                    "example": example,
                }
            }
        }
    }
