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

import json

from app import HOSTNAME, POSTGRES_PORT_WRITE, SUBPATH, VERSION
from app.db.asyncpg_db import get_pool, get_pool_w
from app.oauth import get_current_user
from app.v1.endpoints.functions import insert_commit, set_role
from asyncpg.exceptions import InsufficientPrivilegeError, UniqueViolationError
from fastapi import APIRouter, Body, Depends, status
from fastapi.responses import JSONResponse, Response

v1 = APIRouter()

PAYLOAD_EXAMPLE = {
    "username": "cp1",
    "password": "qwertz",
    "uri": "https://orcid.org/0000-0004-3456-7890",
    "role": "viewer",  # viewer, editor, obs_manager, sensor, custom
}

ROLES = {
    "viewer": "user",
    "editor": "user",
    "obs_manager": "sensor",
    "sensor": "sensor",
}


@v1.api_route(
    "/Users",
    methods=["POST"],
    tags=["Users"],
    summary="Create a new User",
    description="Create a new User entity.",
    status_code=status.HTTP_201_CREATED,
)
async def create_user(
    payload: dict = Body(example=PAYLOAD_EXAMPLE),
    current_user=Depends(get_current_user),
    pgpool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if not isinstance(payload, dict):
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={
                    "code": 400,
                    "type": "error",
                    "message": "Invalid payload format. Expected a dictionary.",
                },
            )

        async with pgpool.acquire() as connection:
            async with connection.transaction():
                if (
                    "username" not in payload
                    or "password" not in payload
                    or "role" not in payload
                ):
                    return JSONResponse(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        content={
                            "code": 400,
                            "type": "error",
                            "message": "Missing required properties: 'username' or 'password' or 'role'.",
                        },
                    )

                if current_user is not None:
                    if current_user["role"] != "administrator":
                        raise InsufficientPrivilegeError

                    await set_role(connection, current_user)

                password = payload.pop("password", None)

                for key in list(payload.keys()):
                    if isinstance(payload[key], dict):
                        payload[key] = json.dumps(payload[key])

                keys = ", ".join(f'"{key}"' for key in payload.keys())
                values_placeholders = ", ".join(
                    (f"${i+1}") for i in range(len(payload))
                )
                query = f"""
                    INSERT INTO sensorthings."User" ({keys})
                    VALUES ({values_placeholders})
                    RETURNING id, username, uri;
                """
                user = await connection.fetchrow(query, *payload.values())

                if not payload.get("uri"):
                    query = """
                        UPDATE sensorthings."User"
                        SET uri = $1 || $2 || $3 ||  '/Users(' || sensorthings."User".id || ')'
                        WHERE sensorthings."User".id = $4;
                    """
                    await connection.execute(
                        query, HOSTNAME, SUBPATH, VERSION, user["id"]
                    )

                if payload["role"] == "sensor":
                    commit = {
                        "message": "Sensor data",
                        "author": user["uri"],
                        "encodingType": "text/plain",
                        "user_id": user["id"],
                    }
                    await insert_commit(connection, commit, "CREATE")

                if current_user is not None:
                    await connection.execute("RESET ROLE;")

                if payload["role"] in ROLES:
                    payload["role"] = ROLES[payload["role"]]

                query = """
                    CREATE USER "{username}"
                    WITH ENCRYPTED PASSWORD '{password}'
                    IN ROLE "{role}";
                """
                await connection.execute(
                    query.format(
                        username=user["username"],
                        password=password,
                        role=payload["role"],
                    )
                )

                query = """
                    GRANT "{user}" TO "{role}";
                """
                await connection.execute(
                    query.format(
                        user=payload["username"],
                        role=current_user["username"],
                    )
                )

        return Response(status_code=status.HTTP_201_CREATED)

    except UniqueViolationError:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"message": "User already exists."},
        )
    except InsufficientPrivilegeError:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"message": "Insufficient privileges."},
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"message": str(e)},
        )
