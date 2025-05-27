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
from fastapi import APIRouter, Body, Depends, Header, status
from fastapi.responses import JSONResponse, Response

from .functions import check_id_exists, set_commit, update_datastream_entity

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
    ALLOWED_KEYS.append("network")


@v1.api_route(
    "/Datastreams({datastream_id})",
    methods=["PATCH"],
    tags=["Datastreams"],
    summary="Update a Datastream",
    description="Update a Datastream",
    status_code=status.HTTP_200_OK,
)
async def update_datastream(
    datastream_id: int,
    payload: dict = Body(example=PAYLOAD_EXAMPLE),
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
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )
