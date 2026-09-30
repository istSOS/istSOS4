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
from app.v1.endpoints.openapi_responses import merge
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Body, Depends, status
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
    "/Users/{user_id}",
    methods=["PATCH"],
    tags=["Users"],
    summary="Update a User",
    description=(
        "Update an existing user's contact info and/or uri, identified by "
        "id -- consistent with every other /Users endpoint. A user's role "
        "and Network scope are changed with `PATCH /Users/{user_id}/role`; "
        "sending `role` here returns 400."
    ),
    status_code=status.HTTP_200_OK,
    responses=merge(
        {
            200: {"description": "Updated (or, with an empty payload, a no-op). Response body is empty."},
            404: {
                "description": "No user exists with that id.",
                "content": {"application/json": {"example": {"message": "User not found."}}},
            },
            # Genuinely 401 here, not 403 -- an inconsistency worth knowing
            # about rather than silently normalising away: most other
            # endpoints in this API map InsufficientPrivilegeError to 403.
            401: {
                "description": (
                    "The caller is not an administrator. Mapped to 401 "
                    "here specifically, unlike most other endpoints in "
                    "this API, which use 403 for the same condition."
                ),
                "content": {"application/json": {"example": {"message": "Insufficient privileges"}}},
            },
            400: {
                "description": (
                    "`role` in the payload (use `PATCH /Users/{user_id}/role`), "
                    "an unrecognised payload key, or any other failure -- "
                    "the message is the raw exception text."
                ),
                "content": {"application/json": {"example": {"message": "Unrecognized key(s): foo"}}},
            },
        }
    ),
)
async def update_user(
    user_id: int,
    payload: dict = Body(examples=[PAYLOAD_EXAMPLE]),
    current_user=Depends(get_current_user),
    pgpool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        async with pgpool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    if current_user["role"] != "administrator":
                        raise InsufficientPrivilegeError

                    await set_role(connection, current_user)

                query = """
                    SELECT username, role FROM sensorthings."User"
                    WHERE id = $1;
                """
                result = await connection.fetchrow(query, user_id)

                if not result:
                    return JSONResponse(
                        status_code=status.HTTP_404_NOT_FOUND,
                        content={"message": "User not found."},
                    )

                if not payload:
                    return Response(status_code=status.HTTP_200_OK)

                # Roles are application-layer (sensorthings."User".role), not
                # per-user PostgreSQL roles, so a role change here used to hit
                # a REVOKE/GRANT on a role that no longer exists. The role
                # endpoint owns role and scope changes and their safeguards.
                if "role" in payload:
                    return JSONResponse(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        content={
                            "message": (
                                "Use PATCH /Users/{user_id}/role to change "
                                "a user's role."
                            )
                        },
                    )

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
                    WHERE id = $1;
                """
                await connection.execute(query, user_id, *payload.values())

        return Response(status_code=status.HTTP_200_OK)

    except InsufficientPrivilegeError:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"message": "Insufficient privileges"},
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"message": str(e)},
        )
