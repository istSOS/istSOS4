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

from app import AUTHORIZATION, POSTGRES_PORT_WRITE, VERSIONING
from app.db.asyncpg_db import get_pool, get_pool_w
from app.utils.utils import validate_payload_keys
from app.v1.endpoints.error_response import error_response
from app.v1.endpoints.functions import set_role
from fastapi import APIRouter, Depends, Header, Request, status
from fastapi.responses import Response

from .functions import check_id_exists, set_commit, update_thing_entity
from .json_patch import apply_json_patch_to_entity, normalize_patch_body
from .put import handle_put_replace, request_body_openapi_example
from app.v1.endpoints.exceptions import BadRequest

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)
message = Header(default=None, alias="commit-message", include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)

if VERSIONING or AUTHORIZATION:
    message = Header(alias="commit-message")

PAYLOAD_EXAMPLE = {
    "description": "thing 1",
    "name": "thing name 1",
    "properties": {"reference": "first"},
}

ALLOWED_KEYS = [
    "name",
    "description",
    "properties",
    "Locations",
    "Datastreams",
]

# conformance: req/create-update-delete/update-entity-put — mandatory Thing
# properties (also NOT NULL in the schema); "properties" is optional and is
# reset to null when a PUT omits it. Relations (Locations, Datastreams) are
# left untouched when absent so the existing links are not orphaned.
REQUIRED_PUT_KEYS = ["name", "description"]
OPTIONAL_PUT_KEYS = ["properties"]


@v1.api_route(
    "/Things({thing_id})",
    methods=["PATCH"],
    tags=["Things"],
    summary="Update a Thing",
    description="Update a Thing",
    status_code=status.HTTP_200_OK,
    openapi_extra=request_body_openapi_example(PAYLOAD_EXAMPLE),
)
async def update_thing(
    thing_id: int,
    payload=Depends(normalize_patch_body),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    if not thing_id:
        raise BadRequest("Thing ID not provided")

    async with pool.acquire() as connection:
        async with connection.transaction():
            if current_user is not None:
                await set_role(connection, current_user)

            if not await check_id_exists(connection, "Thing", thing_id):
                if current_user is not None:
                    await connection.execute("RESET ROLE;")
                return error_response(
                    status.HTTP_404_NOT_FOUND, "Thing not found."
                )

            # req/create-update-delete/update-entity-jsonpatch: resolve an
            # RFC 6902 array body into a merge dict against the current
            # entity; merge-patch dict bodies pass through unchanged.
            payload = await apply_json_patch_to_entity(
                connection, "Thing", thing_id, payload
            )

            if not payload:
                if current_user is not None:
                    await connection.execute("RESET ROLE;")
                return Response(status_code=status.HTTP_200_OK)

            validate_payload_keys(payload, ALLOWED_KEYS)

            commit_id = await set_commit(
                connection,
                commit_message,
                current_user,
            )
            if commit_id is not None:
                payload["commit_id"] = commit_id

            await update_thing_entity(connection, thing_id, payload)

            if current_user is not None:
                await connection.execute("RESET ROLE;")

    return Response(status_code=status.HTTP_200_OK)


@v1.api_route(
    "/Things({thing_id})",
    methods=["PUT"],
    tags=["Things"],
    summary="Replace a Thing",
    description="Replace a Thing (full update)",
    status_code=status.HTTP_200_OK,
    openapi_extra=request_body_openapi_example(PAYLOAD_EXAMPLE),
)
async def replace_thing(
    thing_id: int,
    request: Request,
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    # conformance: req/create-update-delete/update-entity-put (18-088 §10.3)
    return await handle_put_replace(
        pool=pool,
        request=request,
        entity_db_name="Thing",
        not_found_message="Thing not found.",
        entity_id=thing_id,
        commit_message=commit_message,
        current_user=current_user,
        allowed_keys=ALLOWED_KEYS,
        required_keys=REQUIRED_PUT_KEYS,
        optional_keys=OPTIONAL_PUT_KEYS,
        update_entity_fn=update_thing_entity,
    )
