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
from app.v1.endpoints.functions import set_role
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Depends, Header, Request, status
from fastapi.responses import JSONResponse, Response

from .functions import check_id_exists, set_commit, update_network_entity
from .json_patch import apply_json_patch_to_entity, normalize_patch_body
from .put import handle_put_replace, request_body_openapi_example

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)
message = Header(default=None, alias="commit-message", include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)

if VERSIONING or AUTHORIZATION:
    message = Header(alias="commit-message")

PAYLOAD_EXAMPLE = {
    "name": "network 1",
}

ALLOWED_KEYS = ["name", "Datastreams"]

# conformance: req/create-update-delete/update-entity-put (18-088 §10.3) —
# Network's only proprietary structural property is the mandatory "name"
# (NETWORK extension). It has no optional structural properties; the
# "Datastreams" relation is left untouched when a PUT omits it so existing
# links are not orphaned (same rule as the core entities).
REQUIRED_PUT_KEYS = ["name"]
OPTIONAL_PUT_KEYS = []


@v1.api_route(
    "/Networks({network_id})",
    methods=["PATCH"],
    tags=["Networks"],
    summary="Update a Network",
    description="Update a Network",
    status_code=status.HTTP_200_OK,
    openapi_extra=request_body_openapi_example(PAYLOAD_EXAMPLE),
)
async def update_network(
    network_id: int,
    payload=Depends(normalize_patch_body),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if not network_id:
            raise Exception("Network ID not provided")

        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                if not await check_id_exists(
                    connection, "Network", network_id
                ):

                    return JSONResponse(
                        status_code=status.HTTP_404_NOT_FOUND,
                        content={
                            "code": 404,
                            "type": "error",
                            "message": "Sensor not found.",
                        },
                    )

                # req/create-update-delete/update-entity-jsonpatch: resolve an
                # RFC 6902 array body into a merge dict; dict bodies pass through.
                payload = await apply_json_patch_to_entity(
                    connection, "Network", network_id, payload
                )

                if not payload:
                    return Response(status_code=status.HTTP_200_OK)

                validate_payload_keys(payload, ALLOWED_KEYS)

                commit_id = await set_commit(
                    connection,
                    commit_message,
                    current_user,
                )
                if commit_id is not None:
                    payload["commit_id"] = commit_id

                await update_network_entity(connection, network_id, payload)


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
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )


@v1.api_route(
    "/Networks({network_id})",
    methods=["PUT"],
    tags=["Networks"],
    summary="Replace a Network",
    description="Replace a Network (full update)",
    status_code=status.HTTP_200_OK,
    openapi_extra=request_body_openapi_example(PAYLOAD_EXAMPLE),
)
async def replace_network(
    network_id: int,
    request: Request,
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    # conformance: req/create-update-delete/update-entity-put (18-088 §10.3) —
    # full replacement of a Network. NETWORK extension: this route is only
    # registered when the NETWORK flag is set, because api.py includes
    # update_network.v1 exclusively inside its `if NETWORK:` block (mirrors the
    # existing PATCH/POST Network routes). Missing mandatory "name" -> 400; the
    # @iot.id and selfLink are immutable and never part of the body; any
    # client-supplied id in the body is rejected by validate_payload_keys.
    return await handle_put_replace(
        pool=pool,
        request=request,
        entity_db_name="Network",
        not_found_message="Network not found.",
        entity_id=network_id,
        commit_message=commit_message,
        current_user=current_user,
        allowed_keys=ALLOWED_KEYS,
        required_keys=REQUIRED_PUT_KEYS,
        optional_keys=OPTIONAL_PUT_KEYS,
        update_entity_fn=update_network_entity,
    )
