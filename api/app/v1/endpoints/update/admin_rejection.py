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

"""PATCH /Users/{target_user_id}/reject — Admin rejection of pending users.

Flow
----
1.  Verify caller is an ``administrator`` (HTTP 403 otherwise).
2.  Within a single DB transaction (write pool):
    a.  UPDATE ``sensorthings."User"`` — set status='rejected'
        WHERE id = target_user_id AND role = 'pending'.
        RETURNING id; if no row returned -> HTTP 404 (not found or not pending).
        role is left untouched (still 'pending') — rejection is a status
        transition, not a role assignment.
    b.  Insert an ADMIN_REJECTION audit event via ``log_audit_event``.
3.  Return HTTP 200 with a confirmation payload.

Architecture note
-----------------
This is the negative counterpart to PATCH /Users/{id}/policy-approval.
Approval moves a pending user forward into an RBAC role; rejection moves
them sideways into a terminal ``status`` value while leaving ``role``
alone. ``status`` is an unconstrained VARCHAR(50) column (default
'active'), so no schema change was needed there — only the AuditLog
action_type CHECK constraint required extending (see
database/migrations/004_admin_rejection.sql).

A rejected user cannot log in (see the status check in
app.oauth.authenticate_user) but can re-apply via POST /Register, which
overwrites the rejected row instead of returning 409 Conflict.
"""

import logging

from app import POSTGRES_PORT_WRITE
from app.db.asyncpg_db import get_pool, get_pool_w
from app.db.audit_crud import AUDIT_ACTION_ADMIN_REJECTION, log_audit_event
from app.models.reject_request import RejectRequest, RejectionResponse
from app.oauth import get_current_user
from app.v1.endpoints.openapi_responses import (
    ADMIN_ERRORS,
    DB_TIMEOUT,
    DB_UNAVAILABLE,
    INTERNAL,
    NOT_FOUND_PENDING_USER,
    merge,
    response,
)
from asyncpg.exceptions import (
    InsufficientPrivilegeError,
    PostgresConnectionError,
    QueryCanceledError,
    TooManyConnectionsError,
)
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse

v1 = APIRouter()
logger = logging.getLogger(__name__)


@v1.api_route(
    "/Users/{target_user_id}/reject",
    methods=["PATCH"],
    tags=["Registration & Approval"],
    summary="Admin rejection: deny a pending user's access request",
    description=(
        "Reject a pending user's registration request. Sets status='rejected' "
        "and records an ADMIN_REJECTION audit event. Restricted to "
        "administrators. The target user must be in the 'pending' state. "
        "role is left unchanged (still 'pending'); the rejected user can "
        "re-apply via POST /Register."
    ),
    status_code=status.HTTP_200_OK,
    responses=merge(
        {
            200: response(
                RejectionResponse,
                "Rejected. `role` stays 'pending' by design -- only "
                "`status` moves to 'rejected'.",
                {
                    "message": "User 'jdoe' (id=42) has been rejected.",
                    "user_id": 42,
                    "status": "rejected",
                },
            )
        },
        ADMIN_ERRORS,
        NOT_FOUND_PENDING_USER,
        DB_UNAVAILABLE,
        DB_TIMEOUT,
        INTERNAL,
    ),
)
async def patch_reject_user(
    target_user_id: int,
    request: RejectRequest,
    current_user=Depends(get_current_user),
):
    # ------------------------------------------------------------------
    # 1. Authorization: only administrators may reject pending users.
    # ------------------------------------------------------------------
    if current_user["role"] != "administrator":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Forbidden: Administrator access required",
        )

    # ------------------------------------------------------------------
    # 2. Acquire write pool connection and run all mutations atomically.
    # ------------------------------------------------------------------
    try:
        write_pool = await get_pool_w() if POSTGRES_PORT_WRITE else await get_pool()

        async with write_pool.acquire() as conn:
            async with conn.transaction():

                # ------------------------------------------------------
                # 2a. UPDATE the User row — status only, role untouched.
                #     The WHERE clause includes role = 'pending' so an
                #     already-approved active user can never be silently
                #     rejected by mistake; RETURNING id confirms a row
                #     was touched.
                # ------------------------------------------------------
                updated_row = await conn.fetchrow(
                    """
                    UPDATE sensorthings."User"
                    SET status = 'rejected'
                    WHERE id   = $1
                      AND role = 'pending'
                    RETURNING id, username
                    """,
                    target_user_id,
                )

                if updated_row is None:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="User not found or not in pending state",
                    )

                username: str = updated_row["username"]

                # ------------------------------------------------------
                # 2b. Append an ADMIN_REJECTION record to the AuditLog.
                #     Same connection / same transaction -> atomic with
                #     the UPDATE above.
                # ------------------------------------------------------
                await log_audit_event(
                    conn=conn,
                    action_type=AUDIT_ACTION_ADMIN_REJECTION,
                    actor_id=current_user["id"],
                    payload={
                        "rejected_user_id": target_user_id,
                        "reason": request.reason,
                    },
                )

        logger.info(
            "Admin rejection: user '%s' (id=%d) rejected by admin id=%d.",
            username,
            target_user_id,
            current_user["id"],
        )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": (
                    f"User '{username}' (id={target_user_id}) has been rejected."
                ),
                "user_id": target_user_id,
                "status": "rejected",
            },
        )

    except HTTPException:
        # Re-raise HTTPExceptions raised inside the transaction block
        # (404s) without wrapping them in a 500.
        raise
    except InsufficientPrivilegeError:
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={"message": "Insufficient database privileges."},
        )
    except (PostgresConnectionError, TooManyConnectionsError):
        logger.exception("Database unavailable during admin rejection")
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"message": "Database temporarily unavailable."},
        )
    except QueryCanceledError:
        logger.exception("Database timeout during admin rejection")
        return JSONResponse(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            content={"message": "Database request timed out."},
        )
    except Exception:
        logger.exception("Unexpected error during admin rejection")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"message": "Internal server error."},
        )
