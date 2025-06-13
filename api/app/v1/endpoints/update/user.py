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

from app import POSTGRES_PORT_WRITE
from app.db.asyncpg_db import get_pool, get_pool_w
from app.oauth import get_current_user
from app.utils.utils import validate_payload_keys
from app.v1.endpoints.functions import set_role
from asyncpg.exceptions import InsufficientPrivilegeError, UndefinedObjectError
from fastapi import APIRouter, Body, Depends, Query, status
from fastapi.responses import JSONResponse, Response

v1 = APIRouter()

PAYLOAD_EXAMPLE = {
    "contact": {
        "email": "example@mail.com",
        "name": "example",
    },
    "uri": "https://orcid.org/0000-0004-3456-7890",
}

ALLOWED_KEYS = [
    "contact",
    "uri",
]


@v1.api_route(
    "/Users",
    methods=["PATCH"],
    tags=["Users"],
    summary="Update a User",
    description="Update a User",
    status_code=status.HTTP_200_OK,
)
async def update_user(
    user: str = Query(
        alias="user",
        description="The the user to update",
    ),
    payload: dict = Body(example=PAYLOAD_EXAMPLE),
    current_user=Depends(get_current_user),
    pgpool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if not user:
            raise Exception("User not provided")

        async with pgpool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    if current_user["role"] != "administrator":
                        raise InsufficientPrivilegeError

                    await set_role(connection, current_user)

                query = """
                    SELECT username FROM sensorthings."User"
                    WHERE username = $1;
                """
                result = await connection.fetch(query, user)

                if not result:
                    return JSONResponse(
                        status_code=status.HTTP_404_NOT_FOUND,
                        content={"message": "User not found."},
                    )

                if not payload:
                    if current_user is not None:
                        await connection.execute("RESET ROLE;")
                    return Response(status_code=status.HTTP_200_OK)

                validate_payload_keys(payload, ALLOWED_KEYS)

                payload = {
                    key: (
                        json.dumps(value) if isinstance(value, dict) else value
                    )
                    for key, value in payload.items()
                }

                set_clause = ", ".join(
                    [f'"{key}" = ${i + 2}' for i, key in enumerate(payload)]
                )

                query = f"""
                    UPDATE sensorthings."User"
                    SET {set_clause}
                    WHERE username = $1;
                """
                await connection.execute(query, user, *payload.values())

                if current_user is not None:
                    await connection.execute("RESET ROLE;")

        return Response(status_code=status.HTTP_200_OK)

    except UndefinedObjectError as e:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"message": "Policy not found"},
        )
    except InsufficientPrivilegeError as e:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"message": "Insufficient privileges"},
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"message": str(e)},
        )
