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

The actual column writes reuse the existing ``update_*_entity`` functions and
the same ``validate_required_keys`` / ``validate_payload_keys`` validators as
the create path, rather than introducing a parallel validator.
"""

import json

from app.utils.utils import validate_payload_keys, validate_required_keys
from app.v1.endpoints.functions import set_role
import asyncpg
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import Request, status
from fastapi.responses import JSONResponse, Response

from .functions import check_id_exists, set_commit


async def parse_put_body(request: Request) -> dict:
    """Parse a PUT body, which must be a single JSON object (the entity).

    An empty body is returned as ``{}`` so the mandatory-property check below
    turns it into a 400 (rather than a 422), keeping error semantics aligned
    with the rest of the create/update paths.
    """
    raw = await request.body()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise Exception(f"Invalid JSON body: {exc}")
    if not isinstance(data, dict):
        raise Exception(
            "PUT body must be a JSON object representing the entity."
        )
    return data


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

    Both the PATCH handlers (body read via ``Depends(normalize_patch_body)`` so
    an RFC 6902 array is accepted) and the PUT handlers (body read from the raw
    ``Request``) parse the body manually, so FastAPI generates no requestBody /
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


async def handle_put_replace(
    *,
    pool,
    request: Request,
    entity_db_name: str,
    not_found_message: str,
    entity_id: int,
    commit_message,
    current_user,
    allowed_keys,
    required_keys,
    optional_keys,
    update_entity_fn,
    post_update=None,
):
    """Shared PUT (full replacement) flow for a single STA core entity.

    Mirrors the structure of the existing PATCH handlers (role, existence check,
    commit, entity update) but applies PUT semantics via ``build_put_payload``.
    ``post_update`` (when given) runs inside the same transaction after the
    column write, receiving ``(connection, entity_id, payload, updated)`` so an
    entity that maintains derived state on PATCH (e.g. Observation /
    FeatureOfInterest) keeps doing so on PUT.
    """
    try:
        if not entity_id:
            raise Exception(f"{entity_db_name} ID not provided")

        payload = await parse_put_body(request)

        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                if not await check_id_exists(
                    connection, entity_db_name, entity_id
                ):
                    if current_user is not None:
                        await connection.execute("RESET ROLE;")
                    return JSONResponse(
                        status_code=status.HTTP_404_NOT_FOUND,
                        content={
                            "code": 404,
                            "type": "error",
                            "message": not_found_message,
                        },
                    )

                payload = build_put_payload(
                    payload, required_keys, optional_keys
                )

                validate_payload_keys(payload, allowed_keys)

                commit_id = await set_commit(
                    connection,
                    commit_message,
                    current_user,
                )
                if commit_id is not None:
                    payload["commit_id"] = commit_id

                updated = await update_entity_fn(
                    connection, entity_id, payload
                )

                if post_update is not None:
                    await post_update(
                        connection, entity_id, payload, updated
                    )

                if current_user is not None:
                    await connection.execute("RESET ROLE;")

        return Response(status_code=status.HTTP_200_OK)
    except InsufficientPrivilegeError:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={
                "code": 401,
                "type": "error",
                "message": "Insufficient privileges.",
            },
        )
    except (asyncpg.PostgresConnectionError, asyncpg.TooManyConnectionsError):
        # conformance: req/request-data/status-code — DB unavailable is 503 (mirror read.py), not 400
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "code": 503,
                "type": "error",
                "message": "Database temporarily unavailable",
            },
        )
    except ValueError as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )
    except asyncpg.ForeignKeyViolationError:
        # conformance: bad @iot.id reference is a client error (400); controlled msg, no raw PG text
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "code": 400,
                "type": "error",
                "message": "Referenced entity does not exist.",
            },
        )
    except Exception:
        # conformance: req/request-data/status-code — internal errors are 500, not 400 (no stacktrace)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "code": 500,
                "type": "error",
                "message": "Internal server error",
            },
        )
