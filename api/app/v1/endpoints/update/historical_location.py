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
from app.v1.endpoints.error_response import error_response

from .functions import (
    check_id_exists,
    set_commit,
    update_historical_location_entity,
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

PAYLOAD_EXAMPLE = {"time": "2015-07-01T00:00:00.000Z"}

ALLOWED_KEYS = ["time", "Thing", "Locations"]


@v1.api_route(
    "/HistoricalLocations({historical_location_id})",
    methods=["PATCH"],
    tags=["HistoricalLocations"],
    summary="Update a Historical Location",
    description="Update a Historical Location",
    status_code=status.HTTP_200_OK,
    openapi_extra=request_body_openapi_example(PAYLOAD_EXAMPLE),
)
async def update_historical_location(
    historical_location_id: int,
    payload=Depends(normalize_patch_body),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if not historical_location_id:
            raise Exception("Historical Location ID not provided")

        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                if not await check_id_exists(
                    connection, "HistoricalLocation", historical_location_id
                ):
                    if current_user is not None:
                    return error_response(status.HTTP_404_NOT_FOUND, "Historical Location not found.")

                # req/create-update-delete/update-entity-jsonpatch: resolve an
                # RFC 6902 array body into a merge dict; dict bodies pass through.
                payload = await apply_json_patch_to_entity(
                    connection,
                    "HistoricalLocation",
                    historical_location_id,
                    payload,
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

                await update_historical_location_entity(
                    connection,
                    historical_location_id,
                    payload,
                )


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
        return error_response(status.HTTP_503_SERVICE_UNAVAILABLE, "Database temporarily unavailable")
    except ValueError as e:
        return error_response(status.HTTP_400_BAD_REQUEST, str(e))
    except asyncpg.ForeignKeyViolationError:
        # conformance: bad @iot.id reference is a client error (400); controlled msg, no raw PG text
        return error_response(status.HTTP_400_BAD_REQUEST, "Referenced entity does not exist.")
    except Exception:
        # conformance: req/request-data/status-code — internal errors are 500, not 400 (no stacktrace)
        return error_response(status.HTTP_500_INTERNAL_SERVER_ERROR, "Internal server error")


# conformance: req/create-update-delete/update-entity-put — "time" is the only
# mutable scalar property (NOT NULL) and is mandatory. The Thing and Locations
# relations are left untouched when absent so the existing, required Thing link
# is not orphaned.
REQUIRED_PUT_KEYS = ["time"]
OPTIONAL_PUT_KEYS = []


@v1.api_route(
    "/HistoricalLocations({historical_location_id})",
    methods=["PUT"],
    tags=["HistoricalLocations"],
    summary="Replace a Historical Location",
    description="Replace a Historical Location (full update)",
    status_code=status.HTTP_200_OK,
    openapi_extra=request_body_openapi_example(PAYLOAD_EXAMPLE),
)
async def replace_historical_location(
    historical_location_id: int,
    request: Request,
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    # conformance: req/create-update-delete/update-entity-put (18-088 §10.3)
    return await handle_put_replace(
        pool=pool,
        request=request,
        entity_db_name="HistoricalLocation",
        not_found_message="Historical Location not found.",
        entity_id=historical_location_id,
        commit_message=commit_message,
        current_user=current_user,
        allowed_keys=ALLOWED_KEYS,
        required_keys=REQUIRED_PUT_KEYS,
        optional_keys=OPTIONAL_PUT_KEYS,
        update_entity_fn=update_historical_location_entity,
    )
