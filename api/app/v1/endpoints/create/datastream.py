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
from fastapi import APIRouter, Body, Depends, Header, Request, status
from fastapi.responses import JSONResponse, Response

from .functions import insert_datastream_entity, set_commit

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
    "Thing": {"@iot.id": 1},
    "Sensor": {"@iot.id": 1},
    "ObservedProperty": {"@iot.id": 1},
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
    "/Datastreams",
    methods=["POST"],
    tags=["Datastreams"],
    summary="Create a new Datastream",
    description="Create a new Datastream entity.",
    status_code=status.HTTP_201_CREATED,
)
async def create_datastream(
    request: Request,
    payload: dict = Body(example=PAYLOAD_EXAMPLE),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if (
            not "content-type" in request.headers
            or request.headers["content-type"] != "application/json"
        ):
            raise Exception("Only content-type application/json is supported.")

        validate_payload_keys(payload, ALLOWED_KEYS)

        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                commit_id = await set_commit(
                    connection, commit_message, current_user
                )
                if commit_id is not None:
                    payload["commit_id"] = commit_id

                _, header = await insert_datastream_entity(
                    connection, payload, commit_id=commit_id
                )

                if current_user is not None:
                    await connection.execute("RESET ROLE;")
        return Response(
            status_code=status.HTTP_201_CREATED,
            headers={"location": header},
        )
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
            content={
                "code": 400,
                "type": "error",
                "message": str(e),
            },
        )


PAYLOAD_EXAMPLE_THING = {
    "unitOfMeasurement": {
        "name": "Lumen",
        "symbol": "lm",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/Lumen",
    },
    "description": "datastream 1",
    "name": "datastream name 1",
    "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
    "Sensor": {"@iot.id": 1},
    "ObservedProperty": {"@iot.id": 1},
}


@v1.api_route(
    "/Things({thing_id})/Datastreams",
    methods=["POST"],
    tags=["Datastreams"],
    summary="Create a new Datastream for a Thing",
    description="Create a new Datastream entity for a Thing entity.",
    status_code=status.HTTP_201_CREATED,
)
async def create_datastream_for_thing(
    request: Request,
    thing_id: int,
    payload: dict = Body(example=PAYLOAD_EXAMPLE_THING),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if (
            not "content-type" in request.headers
            or request.headers["content-type"] != "application/json"
        ):
            raise Exception("Only content-type application/json is supported.")

        if not thing_id:
            raise Exception("Thing ID is required.")

        payload["Thing"] = {"@iot.id": thing_id}

        validate_payload_keys(payload, ALLOWED_KEYS)

        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                commit_id = await set_commit(
                    connection, commit_message, current_user
                )
                if commit_id is not None:
                    payload["commit_id"] = commit_id

                _, header = await insert_datastream_entity(
                    connection, payload, thing_id=thing_id, commit_id=commit_id
                )

                if current_user is not None:
                    await connection.execute("RESET ROLE;")
        return Response(
            status_code=status.HTTP_201_CREATED,
            headers={"location": header},
        )
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
            content={
                "code": 400,
                "type": "error",
                "message": str(e),
            },
        )


PAYLOAD_EXAMPLE_SENSOR = {
    "unitOfMeasurement": {
        "name": "Lumen",
        "symbol": "lm",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/Lumen",
    },
    "description": "datastream 1",
    "name": "datastream name 1",
    "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
    "Thing": {"@iot.id": 1},
    "ObservedProperty": {"@iot.id": 1},
}


@v1.api_route(
    "/Sensors({sensor_id})/Datastreams",
    methods=["POST"],
    tags=["Datastreams"],
    summary="Create a new Datastream for a Sensor",
    description="Create a new Datastream entity for a Sensor entity.",
    status_code=status.HTTP_201_CREATED,
)
async def create_datastream_for_sensor(
    request: Request,
    sensor_id: int,
    payload: dict = Body(example=PAYLOAD_EXAMPLE_SENSOR),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if (
            not "content-type" in request.headers
            or request.headers["content-type"] != "application/json"
        ):
            raise Exception("Only content-type application/json is supported.")

        if not sensor_id:
            raise Exception("Sensor ID is required.")

        payload["Sensor"] = {"@iot.id": sensor_id}

        validate_payload_keys(payload, ALLOWED_KEYS)

        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                commit_id = await set_commit(
                    connection, commit_message, current_user
                )
                if commit_id is not None:
                    payload["commit_id"] = commit_id

                _, header = await insert_datastream_entity(
                    connection,
                    payload,
                    sensor_id=sensor_id,
                    commit_id=commit_id,
                )

                if current_user is not None:
                    await connection.execute("RESET ROLE;")
        return Response(
            status_code=status.HTTP_201_CREATED,
            headers={"location": header},
        )
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
            content={
                "code": 400,
                "type": "error",
                "message": str(e),
            },
        )


PAYLOAD_EXAMPLE_OBSERVED_PROPERTY = {
    "unitOfMeasurement": {
        "name": "Lumen",
        "symbol": "lm",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/Lumen",
    },
    "description": "datastream 1",
    "name": "datastream name 1",
    "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
    "Thing": {"@iot.id": 1},
    "Sensor": {"@iot.id": 1},
}


@v1.api_route(
    "/ObservedProperties({observed_property_id})/Datastreams",
    methods=["POST"],
    tags=["Datastreams"],
    summary="Create a new Datastream for an Observed Property",
    description="Create a new Datastream entity for an Observed Property entity.",
    status_code=status.HTTP_201_CREATED,
)
async def create_datastream_for_observed_property(
    request: Request,
    observed_property_id: int,
    payload: dict = Body(example=PAYLOAD_EXAMPLE_OBSERVED_PROPERTY),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if (
            not "content-type" in request.headers
            or request.headers["content-type"] != "application/json"
        ):
            raise Exception("Only content-type application/json is supported.")

        if not observed_property_id:
            raise Exception("Observed Property ID is required.")

        payload["ObservedProperty"] = {"@iot.id": observed_property_id}

        validate_payload_keys(payload, ALLOWED_KEYS)

        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                commit_id = await set_commit(
                    connection, commit_message, current_user
                )
                if commit_id is not None:
                    payload["commit_id"] = commit_id

                _, header = await insert_datastream_entity(
                    connection,
                    payload,
                    observed_property_id=observed_property_id,
                    commit_id=commit_id,
                )

                if current_user is not None:
                    await connection.execute("RESET ROLE;")
        return Response(
            status_code=status.HTTP_201_CREATED,
            headers={"location": header},
        )
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
            content={
                "code": 400,
                "type": "error",
                "message": str(e),
            },
        )
