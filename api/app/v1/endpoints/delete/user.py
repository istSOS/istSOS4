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

"""DELETE /Users — Deactivate a user (never a hard delete).

Why deactivate instead of delete
---------------------------------
A real DELETE FROM sensorthings."User" fails unconditionally, for every
caller, including a true PostgreSQL superuser: AuditLog_actor_id_fkey is
ON DELETE SET NULL, and Postgres runs that enforcement trigger with the
*referenced table's owner* privileges (both User and AuditLog are owned
by "administrator"), not the caller's. administrator was deliberately
never granted UPDATE on AuditLog, since it's meant to be genuinely
append-only. Loosening that guarantee just to make DELETE work was
considered and rejected -- the audit trail staying provably untouchable,
even by an administrator, is the more important property to keep.

This endpoint sets sensorthings."User".status = 'deleted' instead. The
row, and every AuditLog entry that references it, is left alone.
DELETED_STATUS is enforced at the auth layer (see app/oauth.py:
authenticate_user, get_current_user) -- checked live on every request,
the same way role already is, so a deactivated user's still-valid JWT
stops working on its very next use with no token revocation step needed.

What this endpoint deliberately no longer does
------------------------------------------------
The previous hard-delete implementation also dropped per-user RLS
policies and ran DROP ROLE. Neither is needed now: a deactivated user is
rejected at the auth layer before any query runs, so a stale custom
policy naming them is harmless, and there is no per-user PostgreSQL role
to drop (the backend connects as one service account -- see
update/admin_approval.py's architecture note).
"""

from app import POSTGRES_PORT_WRITE
from app.db.asyncpg_db import get_pool, get_pool_w
from app.oauth import get_current_user
from app.rbac_roles import DELETED_STATUS
from app.v1.endpoints.functions import set_role
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse, Response

v1 = APIRouter()


@v1.api_route(
    "/Users/{user_id}",
    methods=["DELETE"],
    tags=["Users"],
    summary="Deactivate a User",
    description=(
        "Deactivates the given user, identified by id -- consistent with "
        "every other /Users endpoint. The account and every audit-log "
        "entry that references it are preserved, never physically "
        "deleted. A deactivated account can no longer authenticate, "
        "immediately: role and status are both checked live on every "
        "request, so an already-issued token stops working on its next "
        "use. The username stays permanently reserved (POST /Register "
        "with the same username still returns 409)."
    ),
    status_code=status.HTTP_200_OK,
    responses={
        200: {"description": "Deactivated. Response body is empty."},
        400: {
            "description": "An attempt to deactivate the currently authenticated account.",
            "content": {
                "application/json": {
                    "example": {
                        "code": 400,
                        "type": "error",
                        "message": "Deactivating the currently authenticated user is not allowed.",
                    }
                }
            },
        },
        403: {
            "description": "The caller is not an administrator.",
            "content": {"application/json": {"example": {"message": "Insufficient privileges"}}},
        },
        404: {
            "description": "No user exists with that id.",
            "content": {"application/json": {"example": {"code": 404, "type": "error", "message": "User not found"}}},
        },
        409: {
            "description": "The user is already deactivated.",
            "content": {
                "application/json": {
                    "example": {
                        "code": 409,
                        "type": "error",
                        "message": "User 'jdoe' is already deactivated.",
                    }
                }
            },
        },
        500: {
            "description": "Unexpected error during deactivation.",
            "content": {
                "application/json": {
                    "example": {"code": 500, "type": "error", "message": "Unexpected error while deactivating user"}
                }
            },
        },
    },
)
async def delete_user(
    user_id: int,
    current_user=Depends(get_current_user),
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    if current_user["role"] != "administrator":
                        raise InsufficientPrivilegeError

                    await set_role(connection, current_user)

                # Check the user exists, and isn't already deactivated,
                # before attempting the update.
                row = await connection.fetchrow(
                    """
                    SELECT username, status FROM sensorthings."User"
                    WHERE id = $1;
                    """,
                    user_id,
                )

                if row is None:
                    return JSONResponse(
                        status_code=status.HTTP_404_NOT_FOUND,
                        content={
                            "code": 404,
                            "type": "error",
                            "message": "User not found",
                        },
                    )

                # Prevent authenticated administrators from deactivating
                # themselves. Checked here, after the row is fetched, so
                # it can compare by id (what current_user carries) rather
                # than needing a second lookup by username.
                if current_user is not None and current_user["id"] == user_id:
                    return JSONResponse(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        content={
                            "code": 400,
                            "type": "error",
                            "message": "Deactivating the currently authenticated user is not allowed.",
                        },
                    )

                if row["status"] == DELETED_STATUS:
                    return JSONResponse(
                        status_code=status.HTTP_409_CONFLICT,
                        content={
                            "code": 409,
                            "type": "error",
                            "message": f"User '{row['username']}' is already deactivated.",
                        },
                    )

                await connection.execute(
                    """
                    UPDATE sensorthings."User"
                    SET status = $1
                    WHERE id = $2;
                    """,
                    DELETED_STATUS,
                    user_id,
                )

        return Response(status_code=status.HTTP_200_OK)

    except InsufficientPrivilegeError:
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={"message": "Insufficient privileges"},
        )
    except Exception:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "code": 500,
                "type": "error",
                "message": "Unexpected error while deactivating user",
            },
        )
