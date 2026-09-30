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
from app.rbac_roles import PENDING_ROLE
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse

from .read import set_role

v1 = APIRouter()

user = Depends(lambda: None)

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

                # ORDER BY id: without it Postgres returns rows in physical
                # heap order, which isn't insertion order -- an UPDATE
                # (approve/reject/role-change/deactivate) writes a new row
                # version elsewhere in the heap, so a user's position drifts
                # every time their row changes. id is the primary key, so
                # this is a stable, natural order at negligible cost for an
                # admin-sized table.
                query = """
                    SELECT row_to_json(t) AS users
                    FROM (SELECT * FROM sensorthings."User" ORDER BY id) t;
                """
                users = await connection.fetch(query)

                # Never expose the bcrypt hash. `SELECT *` is kept (rather
                # than an explicit column list) so the query still works
                # when AUTHORIZATION=0 and the migrations that add
                # `password`/`status`/... never ran; the secret column is
                # stripped here instead.
                #
                # `role` is reported as null for a not-yet-approved account.
                # In the DB the column holds the sentinel 'pending' -- an
                # internal, zero-privilege, non-RBAC value that every auth
                # gate keys on (get_current_user 403, /Login 403, the
                # `WHERE role = 'pending'` approve/reject guards, the RLS
                # "matches no policy" fail-close). Surfacing that literally,
                # next to status:'pending' / status:'rejected', misreads as
                # "their role is pending". `requested_role` already carries
                # what the applicant asked for. Active users are untouched.
                cleaned = []
                for record in users:
                    row = {
                        key: value
                        for key, value in ujson.loads(record["users"]).items()
                        if key != "password"
                    }
                    if row.get("role") == PENDING_ROLE:
                        row["role"] = None
                    cleaned.append(row)
                users = cleaned


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
