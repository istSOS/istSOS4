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
from app.v1.endpoints.functions import (
    get_datastreams_from_foi,
    set_role,
    update_datastream_observedArea,
)
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Body, Depends, Header, Request, status
from fastapi.responses import JSONResponse, Response

from .functions import (
    check_id_exists,
    set_commit,
    update_feature_of_interest_entity,
)
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
    "name": "A weather station.",
    "description": "A weather station.",
    "encodingType": "application/vnd.geo+json",
    "feature": {"type": "Point", "coordinates": [-114.05, 51.05]},
}

ALLOWED_KEYS = [
    "name",
    "description",
    "encodingType",
    "feature",
    "properties",
    "Observations",
]


@v1.api_route(
    "/FeaturesOfInterest({feature_of_interest_id})",
    methods=["PATCH"],
    tags=["FeaturesOfInterest"],
    summary="Update a Feature of Interest",
    description="Update a Feature of Interest",
    status_code=status.HTTP_200_OK,
    openapi_extra=request_body_openapi_example(PAYLOAD_EXAMPLE),
)
async def update_feature_of_interest(
    feature_of_interest_id: int,
    payload=Depends(normalize_patch_body),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if not feature_of_interest_id:
            raise Exception("Feature of Interest ID not provided")

        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                if not await check_id_exists(
                    connection, "FeaturesOfInterest", feature_of_interest_id
                ):
                    if current_user is not None:
                        await connection.execute("RESET ROLE;")

                    return JSONResponse(
                        status_code=status.HTTP_404_NOT_FOUND,
                        content={
                            "code": 404,
                            "type": "error",
                            "message": "Feature of Interest not found.",
                        },
                    )

                # req/create-update-delete/update-entity-jsonpatch: resolve an
                # RFC 6902 array body into a merge dict; dict bodies pass through.
                payload = await apply_json_patch_to_entity(
                    connection,
                    "FeaturesOfInterest",
                    feature_of_interest_id,
                    payload,
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

                await update_feature_of_interest_entity(
                    connection,
                    feature_of_interest_id,
                    payload,
                )

                datastream_records = await get_datastreams_from_foi(
                    connection, feature_of_interest_id
                )
                for record in datastream_records:
                    ds_id = record["datastream_id"]
                    await update_datastream_observedArea(
                        connection, ds_id, feature_of_interest_id
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
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )


# conformance: req/create-update-delete/update-entity-put — mandatory
# FeatureOfInterest properties (also NOT NULL in the schema); "properties" is
# optional and is reset to null when a PUT omits it. The Observations relation
# is left untouched when absent so existing links are not orphaned.
REQUIRED_PUT_KEYS = ["name", "description", "encodingType", "feature"]
OPTIONAL_PUT_KEYS = ["properties"]


async def _post_update_feature_of_interest(
    connection, feature_of_interest_id, payload, updated
):
    """Recompute observedArea of every Datastream referencing this FoI.

    Mirrors the PATCH handler so a PUT maintains the same derived state
    (req/create-update-delete/update-entity-put).
    """
    datastream_records = await get_datastreams_from_foi(
        connection, feature_of_interest_id
    )
    for record in datastream_records:
        await update_datastream_observedArea(
            connection, record["datastream_id"], feature_of_interest_id
        )


@v1.api_route(
    "/FeaturesOfInterest({feature_of_interest_id})",
    methods=["PUT"],
    tags=["FeaturesOfInterest"],
    summary="Replace a Feature of Interest",
    description="Replace a Feature of Interest (full update)",
    status_code=status.HTTP_200_OK,
    openapi_extra=request_body_openapi_example(PAYLOAD_EXAMPLE),
)
async def replace_feature_of_interest(
    feature_of_interest_id: int,
    request: Request,
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    # conformance: req/create-update-delete/update-entity-put (18-088 §10.3)
    return await handle_put_replace(
        pool=pool,
        request=request,
        entity_db_name="FeaturesOfInterest",
        not_found_message="Feature of Interest not found.",
        entity_id=feature_of_interest_id,
        commit_message=commit_message,
        current_user=current_user,
        allowed_keys=ALLOWED_KEYS,
        required_keys=REQUIRED_PUT_KEYS,
        optional_keys=OPTIONAL_PUT_KEYS,
        update_entity_fn=update_feature_of_interest_entity,
        post_update=_post_update_feature_of_interest,
    )
