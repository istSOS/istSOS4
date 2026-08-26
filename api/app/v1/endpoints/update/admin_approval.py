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

"""PATCH /Users/{target_user_id}/policy-approval — Admin approval of pending users.

Flow
----
1.  Verify caller is an ``administrator`` (HTTP 403 otherwise).
2.  Within a single DB transaction (write pool):
    a.  Fetch the target user's username for use in the RLS policy call.
    b.  UPDATE ``sensorthings."User"`` — set role and status='active'
        WHERE id = target_user_id AND role = 'pending'.
        RETURNING id; if no row returned → HTTP 404 (not found or not pending).
    c.  Apply the RLS policy function for the assigned role (if one exists).
    d.  Insert an ADMIN_APPROVAL audit event via ``log_audit_event``.
3.  Return HTTP 200 with a confirmation payload.

Architecture note
-----------------
This endpoint is the "Path B" counterpart to POST /Register.  The
registration endpoint creates a pending user; this endpoint is the
administrator action that activates it and binds it to an ODRL policy.

Only ``odrl_governed`` still needs an RLS call here — every other role's
access is enforced by static policies created once (see
007_session_scoped_rls_policies.sql), not per-approval.

The entire mutation (UPDATE + RLS call + AuditLog INSERT) runs inside one
``conn.transaction()`` block so any failure leaves the user still pending
with no partial state or silent orphans.
"""

import logging

from app import POSTGRES_PORT_WRITE
from app.db.asyncpg_db import get_pool, get_pool_w
from app.db.audit_crud import AUDIT_ACTION_ADMIN_APPROVAL, log_audit_event
from app.models.approval_request import AdminApprovalRequest, ApprovalResponse
from app.oauth import get_current_user
from app.v1.endpoints.openapi_responses import (
    BAD_REQUEST_REJECTED,
    DB_TIMEOUT,
    DB_UNAVAILABLE,
    FORBIDDEN_ADMIN,
    INTERNAL,
    NOT_FOUND_PENDING_USER,
    UNAUTHORIZED,
    merge,
    response,
)
from asyncpg.exceptions import (
    InsufficientPrivilegeError,
    PostgresConnectionError,
    QueryCanceledError,
    TooManyConnectionsError,
    UndefinedObjectError,
)
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse

v1 = APIRouter()
logger = logging.getLogger(__name__)


@v1.api_route(
    "/Users/{target_user_id}/policy-approval",
    methods=["PATCH"],
    tags=["Registration & Approval"],
    summary="Admin approval: activate a pending user with an ODRL policy",
    description=(
        "Promote a pending user to an active role and bind them to the specified "
        "ODRL dataset policy.  Applies the appropriate Row-Level Security policy "
        "function for the assigned role and records an ADMIN_APPROVAL audit event. "
        "Restricted to administrators.  The target user must be in the 'pending' state."
    ),
    status_code=status.HTTP_200_OK,
    responses=merge(
        {
            200: response(
                ApprovalResponse,
                "Approved. The RLS policy for the granted role is applied "
                "and an ADMIN_APPROVAL audit event is recorded, in the "
                "same transaction as the role change.",
                {
                    "message": "User 'jdoe' (id=42) has been approved with role 'viewer'.",
                    "user_id": 42,
                    "granted_role": "viewer",
                    "dataset_id": "stac://alpine-snow-2024",
                    "odrl_policy_id": "odrl:policy:cc-by-nc",
                },
            )
        },
        UNAUTHORIZED,
        {
            # A second, differently-shaped 403 exists deeper in this
            # handler for a PostgreSQL-privilege failure
            # ({"message": "Insufficient database privileges."}) -- far
            # rarer than this one, so it's not the documented example, but
            # be aware both are possible on this code.
            403: FORBIDDEN_ADMIN[403],
        },
        NOT_FOUND_PENDING_USER,
        BAD_REQUEST_REJECTED,
        DB_UNAVAILABLE,
        DB_TIMEOUT,
        INTERNAL,
    ),
)
async def patch_policy_approval(
    target_user_id: int,
    request: AdminApprovalRequest,
    current_user=Depends(get_current_user),
):
    # ------------------------------------------------------------------
    # 1. Authorization: only administrators may approve pending users.
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
                # 2a. Fetch the target user's username and status.
                #     We need the username to construct the RLS policy
                #     name and pass as the first argument to the policy
                #     function. status is checked separately below —
                #     rejection is a status transition, not a role
                #     change, so a rejected user still has role='pending'
                #     and would otherwise still match the UPDATE's
                #     WHERE clause in step 2b.
                # ------------------------------------------------------
                username_row = await conn.fetchrow(
                    """
                    SELECT username, status, requested_role
                    FROM sensorthings."User"
                    WHERE id = $1
                    """,
                    target_user_id,
                )

                if username_row is None:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="User not found or not in pending state",
                    )

                if username_row["status"] == "rejected":
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=(
                            "This user's registration was rejected and "
                            "cannot be approved directly. They must "
                            "re-apply via POST /Register first."
                        ),
                    )

                username: str = username_row["username"]

                # request.assigned_role is the administrator's explicit
                # choice and always wins; omitting it falls back to what
                # the applicant themselves asked for at registration. Both
                # missing means there is nothing to approve into.
                target_role = request.assigned_role or username_row["requested_role"]
                if target_role is None:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=(
                            f"User '{username}' did not request a role at "
                            "registration, so assigned_role must be "
                            "specified explicitly."
                        ),
                    )

                # ------------------------------------------------------
                # 2b. UPDATE the User row — role + status.
                #     The WHERE clause includes role = 'pending' so we
                #     only activate genuinely pending users; RETURNING id
                #     confirms a row was touched.
                # ------------------------------------------------------
                updated_row = await conn.fetchrow(
                    """
                    UPDATE sensorthings."User"
                    SET role   = $1,
                        status = 'active'
                    WHERE id   = $2
                      AND role = 'pending'
                    RETURNING id
                    """,
                    target_role,
                    target_user_id,
                )

                if updated_row is None:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="User not found or not in pending state",
                    )

                # ------------------------------------------------------
                # 2c. odrl_governed is the one role that still needs a
                #     per-approval CREATE POLICY call -- it needs a
                #     dataset_id to mean anything, and this is the one
                #     endpoint that has one (request.dataset_id). Builds
                #     USING (dataset_id = <value>) — see
                #     005_odrl_dataset_scoping.sql. Deliberately NOT
                #     migrated to the static-policy scheme below; ODRL
                #     work is scoped for later.
                #
                #     Every other assignable role needs no RLS DDL here at
                #     all as of 007_session_scoped_rls_policies.sql: their
                #     policies are static, created once by that migration,
                #     not per-approval. The UPDATE above is the entire
                #     grant.
                #
                #     IMPORTANT: asyncpg marks the entire transaction as
                #     aborted if *any* exception occurs inside it, even a
                #     caught one.  We use a nested savepoint so that an
                #     UndefinedObjectError rolls back only the inner block
                #     and leaves the outer transaction (UPDATE + AuditLog)
                #     in a healthy, committable state.
                # ------------------------------------------------------
                if target_role == "odrl_governed":
                    policyname = f"{username}_default"
                    try:
                        async with conn.transaction():
                            await conn.execute(
                                "SELECT sensorthings.odrl_governed_policy($1, $2, $3);",
                                [username],
                                policyname,
                                request.dataset_id,
                            )
                    except UndefinedObjectError:
                        logger.warning(
                            "RLS policy skipped for '%s': no PostgreSQL role exists "
                            "(application-layer user from /Register — zero DB footprint).",
                            username,
                        )

                # ------------------------------------------------------
                # 2d. Append an ADMIN_APPROVAL record to the AuditLog.
                #     Same connection / same transaction → atomic with
                #     the UPDATE above.
                # ------------------------------------------------------
                await log_audit_event(
                    conn=conn,
                    action_type=AUDIT_ACTION_ADMIN_APPROVAL,
                    actor_id=current_user["id"],
                    dataset_id=request.dataset_id,
                    odrl_policy_id=request.odrl_policy_id,
                    payload={
                        "approved_user_id": target_user_id,
                        "granted_role": target_role,
                    },
                )

        logger.info(
            "Admin approval: user '%s' (id=%d) granted role '%s' "
            "for dataset '%s' by admin id=%d.",
            username,
            target_user_id,
            target_role,
            request.dataset_id,
            current_user["id"],
        )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": (
                    f"User '{username}' (id={target_user_id}) has been approved "
                    f"with role '{target_role}'."
                ),
                "user_id": target_user_id,
                "granted_role": target_role,
                "dataset_id": request.dataset_id,
                "odrl_policy_id": request.odrl_policy_id,
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
        logger.exception("Database unavailable during admin approval")
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"message": "Database temporarily unavailable."},
        )
    except QueryCanceledError:
        logger.exception("Database timeout during admin approval")
        return JSONResponse(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            content={"message": "Database request timed out."},
        )
    except Exception:
        logger.exception("Unexpected error during admin approval")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"message": "Internal server error."},
        )
