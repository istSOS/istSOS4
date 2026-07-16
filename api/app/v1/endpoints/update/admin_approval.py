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

The RLS policy call mirrors the pattern in activate_user.py exactly:
``SELECT {policy_fn}($1, $2)`` receives ``[username]`` and the policy name
string as positional parameters.

The entire mutation (UPDATE + RLS call + AuditLog INSERT) runs inside one
``conn.transaction()`` block so any failure leaves the user still pending
with no partial state or silent orphans.
"""

import logging

from app import POSTGRES_PORT_WRITE
from app.db.asyncpg_db import get_pool, get_pool_w
from app.db.audit_crud import AUDIT_ACTION_ADMIN_APPROVAL, log_audit_event
from app.models.approval_request import AdminApprovalRequest
from app.oauth import get_current_user
from app.rbac_roles import POLICY_FN_MAP
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
    tags=["Users"],
    summary="Admin approval: activate a pending user with an ODRL policy",
    description=(
        "Promote a pending user to an active role and bind them to the specified "
        "ODRL dataset policy.  Applies the appropriate Row-Level Security policy "
        "function for the assigned role and records an ADMIN_APPROVAL audit event. "
        "Restricted to administrators.  The target user must be in the 'pending' state."
    ),
    status_code=status.HTTP_200_OK,
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
                # 2a. Fetch the target user's username.
                #     We need it to construct the RLS policy name and to
                #     pass as the first argument to the policy function.
                # ------------------------------------------------------
                username_row = await conn.fetchrow(
                    """
                    SELECT username
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

                username: str = username_row["username"]

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
                    request.assigned_role,
                    target_user_id,
                )

                if updated_row is None:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="User not found or not in pending state",
                    )

                # ------------------------------------------------------
                # 2c. Apply the default RLS policy for the assigned role.
                #     'custom' has no default policy function; admins
                #     create one explicitly via POST /Policies.
                #     POLICY_FN_MAP is the single source of truth —
                #     imported from rbac_roles.py.
                #
                #     Architecture note: the policy function issues
                #     CREATE POLICY ... TO <username>, which requires a
                #     matching PostgreSQL role.  Self-registered users
                #     (/Register) have zero DB footprint by design.
                #
                #     IMPORTANT: asyncpg marks the entire transaction as
                #     aborted if *any* exception occurs inside it, even a
                #     caught one.  We use a nested savepoint so that an
                #     UndefinedObjectError rolls back only the inner block
                #     and leaves the outer transaction (UPDATE + AuditLog)
                #     in a healthy, committable state.
                # ------------------------------------------------------
                policy_fn = POLICY_FN_MAP.get(request.assigned_role)
                if policy_fn:
                    policyname = f"{username}_default"
                    try:
                        async with conn.transaction():
                            await conn.execute(
                                f"SELECT {policy_fn}($1, $2);",
                                [username],
                                policyname,
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
                        "granted_role": request.assigned_role,
                    },
                )

        logger.info(
            "Admin approval: user '%s' (id=%d) granted role '%s' "
            "for dataset '%s' by admin id=%d.",
            username,
            target_user_id,
            request.assigned_role,
            request.dataset_id,
            current_user["id"],
        )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": (
                    f"User '{username}' (id={target_user_id}) has been approved "
                    f"with role '{request.assigned_role}'."
                ),
                "user_id": target_user_id,
                "granted_role": request.assigned_role,
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
