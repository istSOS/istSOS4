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

"""PATCH /Users/{user_id}/policy-approval — Admin approval of pending users.

Flow
----
1.  Verify caller is an ``administrator`` (HTTP 403 otherwise).
2.  Within a single DB transaction (write pool):
    a.  Fetch the target user's row.
    b.  UPDATE ``sensorthings."User"`` — set role and status='active'
        WHERE id = user_id AND role = 'pending'.
        RETURNING id; if no row returned → HTTP 404 (not found or not pending).
    c.  Optionally set ``User.dataset_id`` (a Network name) to scope the user.
    d.  Insert an ADMIN_APPROVAL audit event via ``log_audit_event``.
3.  Return HTTP 200 with a confirmation payload.

Architecture note
-----------------
This endpoint is the "Path B" counterpart to POST /Register.  The
registration endpoint creates a pending user; this endpoint activates it.
It also covers "Path A" (OIDC JIT-provisioned pending users) -- that path
used to be served by its own POST /Users/{id}/activate endpoint, built
independently for OIDC signups and doing the same job under different
field names. The two had already drifted (activate logged auth_provider
in its audit event; this one didn't, until that gap was closed). activate
has been removed; this endpoint now covers both origins, resolving
target_role from request.role if given, else the requested_role stated at
registration or OIDC login.

No RLS call is made here — capability (what actions) is enforced by the
static per-role policies created once by 006_session_scoped_rls_policies.sql.
This endpoint only sets the role and, optionally, the network scope (which
rows). Both are plain UPDATEs.

The entire mutation runs inside one ``conn.transaction()`` block so any
failure leaves the user still pending with no partial state.
"""

import logging

from app import NETWORK, POSTGRES_PORT_WRITE
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
)
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse

v1 = APIRouter()
logger = logging.getLogger(__name__)


@v1.api_route(
    "/Users/{user_id}/policy-approval",
    methods=["PATCH"],
    tags=["Registration & Approval"],
    summary="Admin approval: activate a pending user",
    description=(
        "Promote a pending user to an active role, optionally scoping them to "
        "a Network. Covers both local self-registration (POST /Register) and "
        "OIDC JIT-provisioned signups. The role is a plain UPDATE (RLS is "
        "enforced by the static policies in 006_session_scoped_rls_policies.sql); "
        "dataset, if given, is written to User.dataset_id and must match an "
        "existing Network. Records an ADMIN_APPROVAL audit event in the same "
        "transaction. Restricted to administrators; the target user must be "
        "in the 'pending' state."
    ),
    status_code=status.HTTP_200_OK,
    responses=merge(
        {
            200: response(
                ApprovalResponse,
                "Approved. The role change, the optional network scope, and "
                "the ADMIN_APPROVAL audit event are all written in one "
                "transaction.",
                {
                    "message": "User 'jdoe' (id=42) has been approved with role 'viewer'.",
                    "user_id": 42,
                    "granted_role": "viewer",
                    "dataset_id": "IDROLOGIA",
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
    user_id: int,
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
                    SELECT username, status, requested_role, dataset_id,
                        auth_provider
                    FROM sensorthings."User"
                    WHERE id = $1
                    """,
                    user_id,
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

                # request.role is the administrator's explicit choice and
                # always wins; omitting it falls back to what the
                # applicant themselves asked for at registration or OIDC
                # login. Both missing means there is nothing to approve
                # into.
                target_role = request.role or username_row["requested_role"]
                if target_role is None:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=(
                            f"User '{username}' did not request a role at "
                            "registration, so role must be specified "
                            "explicitly."
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
                    user_id,
                )

                if updated_row is None:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="User not found or not in pending state",
                    )

                # ------------------------------------------------------
                # 2c. Network scope. The role UPDATE above is the entire
                #     capability grant (enforced by the static policies in
                #     006_session_scoped_rls_policies.sql). This step only
                #     sets *which rows* the user may touch, by writing a
                #     Network name to User.dataset_id.
                #
                #     request.dataset: None  -> leave the applicant's
                #       requested value unchanged.
                #                     ""    -> clear any scope.
                #                     name  -> must match a Network.
                # ------------------------------------------------------
                # With NETWORK=0 there are no Networks, so a requested scope is ignored.
                granted_dataset_id = username_row["dataset_id"] if NETWORK else None
                if NETWORK and request.dataset is not None:
                    new_scope = request.dataset.strip() or None
                    if new_scope is not None and NETWORK:
                        exists = await conn.fetchval(
                            'SELECT 1 FROM sensorthings."Network" WHERE name = $1',
                            new_scope,
                        )
                        if not exists:
                            raise HTTPException(
                                status_code=status.HTTP_400_BAD_REQUEST,
                                detail=(
                                    f"No Network named '{new_scope}'. "
                                    "dataset must match an existing Network."
                                ),
                            )
                    await conn.execute(
                        'UPDATE sensorthings."User" SET dataset_id = $1 WHERE id = $2',
                        new_scope,
                        user_id,
                    )
                    granted_dataset_id = new_scope

                # ------------------------------------------------------
                # 2d. Append an ADMIN_APPROVAL record to the AuditLog.
                #     Same connection / same transaction → atomic with
                #     the UPDATE above.
                # ------------------------------------------------------
                await log_audit_event(
                    conn=conn,
                    action_type=AUDIT_ACTION_ADMIN_APPROVAL,
                    actor_id=current_user["id"],
                    dataset_id=granted_dataset_id,
                    payload={
                        "approved_user_id": user_id,
                        "granted_role": target_role,
                        "auth_provider": username_row["auth_provider"],
                    },
                )

        logger.info(
            "Admin approval: user '%s' (id=%d) granted role '%s' "
            "scoped to '%s' by admin id=%d.",
            username,
            user_id,
            target_role,
            granted_dataset_id,
            current_user["id"],
        )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": (
                    f"User '{username}' (id={user_id}) has been approved "
                    f"with role '{target_role}'."
                ),
                "user_id": user_id,
                "granted_role": target_role,
                "dataset_id": granted_dataset_id,
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
