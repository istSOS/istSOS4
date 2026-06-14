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

import asyncpg
from app import AUTHORIZATION, POSTGRES_PORT_WRITE, VERSIONING
from app.db.asyncpg_db import get_pool, get_pool_w
from app.utils.utils import (
    require_json_content_type,
    validate_payload_keys,
    validate_required_keys,
)
from app.v1.endpoints.error_response import error_response
from app.v1.endpoints.functions import set_role
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Body, Depends, Header, Request, status
from fastapi.responses import JSONResponse, Response

from .functions import insert_thing_entity, set_commit

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

REQUIRED_KEYS = ["name"]


@v1.api_route(
    "/Things",
    methods=["POST"],
    tags=["Things"],
    summary="Create a new Thing",
    description="Create a new Thing entity.",
    status_code=status.HTTP_201_CREATED,
)
async def create_thing(
    request: Request,
    payload: dict = Body(examples=[PAYLOAD_EXAMPLE]),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        require_json_content_type(request)

        validate_payload_keys(payload, ALLOWED_KEYS)
        validate_required_keys(payload, REQUIRED_KEYS)

        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                commit_id = await set_commit(
                    connection, commit_message, current_user
                )
                if commit_id is not None:
                    payload["commit_id"] = commit_id

                _, header = await insert_thing_entity(
                    connection, payload, commit_id
                )

        return Response(
            status_code=status.HTTP_201_CREATED,
            headers={"location": header},
        )
    except InsufficientPrivilegeError as e:
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={
                "code": 403,
                "type": "error",
                "message": "Insufficient privileges.",
            },
        )
    except (asyncpg.PostgresConnectionError, asyncpg.TooManyConnectionsError):
        # conformance: req/request-data/status-code — DB unavailable is 503 (mirror read.py), not 400
        return error_response(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Database temporarily unavailable",
        )
    except ValueError as e:
        return error_response(status.HTTP_400_BAD_REQUEST, str(e))
    except asyncpg.ForeignKeyViolationError:
        # conformance: bad @iot.id reference is a client error (400); controlled msg, no raw PG text
        return error_response(
            status.HTTP_400_BAD_REQUEST, "Referenced entity does not exist."
        )
    except (asyncpg.IntegrityConstraintViolationError, asyncpg.DataError):
        # conformance: req/create-update-delete/create-entity — a payload that
        # violates a NOT NULL / CHECK / data constraint (e.g. a deep-inserted
        # related entity missing a required column) is a client error (400), not
        # a 500. UniqueViolation (409) and ForeignKey (400) are handled above.
        return error_response(
            status.HTTP_400_BAD_REQUEST,
            "Invalid entity: a required value is missing or not allowed.",
        )
    except Exception:
        # conformance: req/request-data/status-code — internal errors are 500, not 400 (no stacktrace)
        return error_response(
            status.HTTP_500_INTERNAL_SERVER_ERROR, "Internal server error"
        )


@v1.api_route(
    "/Locations({location_id})/Things",
    methods=["POST"],
    tags=["Things"],
    summary="Create a new Thing for a Location",
    description="Create a new Thing entity for a Location entity.",
    status_code=status.HTTP_201_CREATED,
)
async def create_thing_for_location(
    request: Request,
    location_id: int,
    payload: dict = Body(examples=[PAYLOAD_EXAMPLE]),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        require_json_content_type(request)

        if not location_id:
            raise Exception("No location ID provided")

        payload["Locations"] = [{"@iot.id": location_id}]

        validate_payload_keys(payload, ALLOWED_KEYS)
        validate_required_keys(payload, REQUIRED_KEYS)

        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                commit_id = await set_commit(
                    connection, commit_message, current_user
                )
                if commit_id is not None:
                    payload["commit_id"] = commit_id

                _, header = await insert_thing_entity(
                    connection, payload, commit_id
                )

                if current_user is not None:
                    payload["user_id"] = current_user["id"]

        return Response(
            status_code=status.HTTP_201_CREATED,
            headers={"location": header},
        )
    except InsufficientPrivilegeError:
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={
                "code": 403,
                "type": "error",
                "message": "Insufficient privileges.",
            },
        )
    except (asyncpg.PostgresConnectionError, asyncpg.TooManyConnectionsError):
        # conformance: req/request-data/status-code — DB unavailable is 503 (mirror read.py), not 400
        return error_response(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Database temporarily unavailable",
        )
    except ValueError as e:
        return error_response(status.HTTP_400_BAD_REQUEST, str(e))
    except asyncpg.ForeignKeyViolationError:
        # conformance: bad @iot.id reference is a client error (400); controlled msg, no raw PG text
        return error_response(
            status.HTTP_400_BAD_REQUEST, "Referenced entity does not exist."
        )
    except (asyncpg.IntegrityConstraintViolationError, asyncpg.DataError):
        # conformance: req/create-update-delete/create-entity — a payload that
        # violates a NOT NULL / CHECK / data constraint (e.g. a deep-inserted
        # related entity missing a required column) is a client error (400), not
        # a 500. UniqueViolation (409) and ForeignKey (400) are handled above.
        return error_response(
            status.HTTP_400_BAD_REQUEST,
            "Invalid entity: a required value is missing or not allowed.",
        )
    except Exception:
        # conformance: req/request-data/status-code — internal errors are 500, not 400 (no stacktrace)
        return error_response(
            status.HTTP_500_INTERNAL_SERVER_ERROR, "Internal server error"
        )
