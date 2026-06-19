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
import asyncpg
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Body, Depends, Header, Request, status
from fastapi.responses import JSONResponse, Response

from .functions import check_id_exists, set_commit, update_datastream_entity
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
    "unitOfMeasurement": {
        "name": "Lumen",
        "symbol": "lm",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/Lumen",
    },
    "description": "datastream 1",
    "name": "datastream name 1",
    "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
}

ALLOWED_KEYS = [
    "name",
    "description",
    "unitOfMeasurement",
    "observationType",
    "observedArea",
    "phenomenonTime",
    "resultTime",
    "properties",
    "Thing",
    "Sensor",
    "ObservedProperty",
    "Observations",
]

if AUTHORIZATION:
    ALLOWED_KEYS.append("Network")


@v1.api_route(
    "/Datastreams({datastream_id})",
    methods=["PATCH"],
    tags=["Datastreams"],
    summary="Update a Datastream",
    description="Update a Datastream",
    status_code=status.HTTP_200_OK,
    openapi_extra=request_body_openapi_example(PAYLOAD_EXAMPLE),
)
async def update_datastream(
    datastream_id: int,
    payload=Depends(normalize_patch_body),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if not datastream_id:
            raise Exception("Datastream ID not provided")

        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                if not await check_id_exists(
                    connection, "Datastream", datastream_id
                ):
                    if current_user is not None:
                        await connection.execute("RESET ROLE;")

                    return JSONResponse(
                        status_code=status.HTTP_404_NOT_FOUND,
                        content={
                            "code": 404,
                            "type": "error",
                            "message": "Datastream not found.",
                        },
                    )

                # req/create-update-delete/update-entity-jsonpatch: resolve an
                # RFC 6902 array body into a merge dict; dict bodies pass through.
                payload = await apply_json_patch_to_entity(
                    connection, "Datastream", datastream_id, payload
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

                await update_datastream_entity(
                    connection,
                    datastream_id,
                    payload,
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
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )


# conformance: req/create-update-delete/update-entity-put — mandatory Datastream
# properties (also NOT NULL in the schema). observedArea / phenomenonTime /
# resultTime / properties are optional and reset to null when a PUT omits them.
# The mandatory relations (Thing, Sensor, ObservedProperty[, Network]) and the
# Observations collection are left untouched when absent so the existing,
# required links are not orphaned.
REQUIRED_PUT_KEYS = ["name", "description", "unitOfMeasurement", "observationType"]
OPTIONAL_PUT_KEYS = ["observedArea", "phenomenonTime", "resultTime", "properties"]


@v1.api_route(
    "/Datastreams({datastream_id})",
    methods=["PUT"],
    tags=["Datastreams"],
    summary="Replace a Datastream",
    description="Replace a Datastream (full update)",
    status_code=status.HTTP_200_OK,
    openapi_extra=request_body_openapi_example(PAYLOAD_EXAMPLE),
)
async def replace_datastream(
    datastream_id: int,
    request: Request,
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    # conformance: req/create-update-delete/update-entity-put (18-088 §10.3)
    return await handle_put_replace(
        pool=pool,
        request=request,
        entity_db_name="Datastream",
        not_found_message="Datastream not found.",
        entity_id=datastream_id,
        commit_message=commit_message,
        current_user=current_user,
        allowed_keys=ALLOWED_KEYS,
        required_keys=REQUIRED_PUT_KEYS,
        optional_keys=OPTIONAL_PUT_KEYS,
        update_entity_fn=update_datastream_entity,
    )
