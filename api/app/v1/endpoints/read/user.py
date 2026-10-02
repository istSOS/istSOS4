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

import ujson
from app import AUTHORIZATION
from app.db.asyncpg_db import get_pool
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Depends, Header, status
from fastapi.responses import JSONResponse

from .read import set_role

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)


@v1.api_route(
    "/Users",
    methods=["GET"],
    tags=["Users"],
    summary="Get all users",
    description="Returns all the users provided by this api (subject to any parameters set)",
    status_code=status.HTTP_200_OK,
)
async def get_users(
    current_user=user,
    pool=Depends(get_pool),
):
    try:
        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    if current_user["role"] != "administrator":
                        raise InsufficientPrivilegeError

                    await set_role(connection, current_user)

                query = """
                    SELECT row_to_json(t) AS users
                    FROM (SELECT * FROM sensorthings."User") t;
                """
                users = await connection.fetch(query)

                users = [ujson.loads(record["users"]) for record in users]

                if current_user is not None:
                    await connection.execute("RESET ROLE;")

                return JSONResponse(
                    status_code=status.HTTP_200_OK,
                    content={"value": users},
                )

    except InsufficientPrivilegeError:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"message": "Insufficient privileges."},
        )
    except Exception:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"message": "Users not found."},
        )


# Columns any authenticated user may read about another user, e.g. to name the
# author of a commit. `contact` is personal data and stays administrator-only.
PUBLIC_USER_COLUMNS = "id, username, role, uri"
ADMIN_USER_COLUMNS = PUBLIC_USER_COLUMNS + ", contact"


@v1.api_route(
    "/Users({user_id})",
    methods=["GET"],
    tags=["Users"],
    summary="Get a user",
    description=(
        "Returns one user. Any authenticated user gets id, username, role and "
        "uri; administrators also get contact."
    ),
    status_code=status.HTTP_200_OK,
)
async def get_user(
    user_id: int,
    current_user=user,
    pool=Depends(get_pool),
):
    # No SET ROLE here: the `sensor` database role has SELECT on "User"
    # revoked, and this endpoint deliberately exposes a fixed set of columns
    # to every role — the same unscoped lookup get_current_user performs.
    is_admin = (
        current_user is not None
        and current_user["role"] == "administrator"
    )
    columns = ADMIN_USER_COLUMNS if is_admin else PUBLIC_USER_COLUMNS

    try:
        async with pool.acquire() as connection:
            record = await connection.fetchval(
                f"""
                    SELECT row_to_json(t)
                    FROM (
                        SELECT {columns}
                        FROM sensorthings."User"
                        WHERE id = $1
                    ) t;
                """,
                user_id,
            )
    except Exception:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"message": "Could not read user."},
        )

    if record is None:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"message": "User not found."},
        )

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=ujson.loads(record),
    )
