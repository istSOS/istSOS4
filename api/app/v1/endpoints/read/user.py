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
    # Note: takes no query parameters at all -- every row is always
    # returned. Administrator-only; a non-admin caller gets 401.
    description="Returns every user, full rows, unfiltered. Administrator-only.",
    status_code=status.HTTP_200_OK,
    responses={
        200: {
            "description": "The full User table.",
            "content": {
                "application/json": {
                    "example": {
                        "value": [
                            {
                                "id": 1,
                                "username": "admin",
                                "role": "administrator",
                                "status": "active",
                            }
                        ]
                    }
                }
            },
        },
        401: {
            "description": "The caller is not an administrator.",
            "content": {"application/json": {"example": {"message": "Insufficient privileges."}}},
        },
        404: {
            "description": "Catch-all for any unexpected query failure.",
            "content": {"application/json": {"example": {"message": "Users not found."}}},
        },
    },
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
