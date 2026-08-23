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

"""POST /Users/{id}/activate  — Admin activation of pending OIDC users.

Flow
----
1. Verify caller is an ``administrator``.
2. Load the target user row; confirm it is currently in the ``pending`` state.
3. Validate the requested target role via ``validate_rbac_role``.
4. Within a single transaction:
   a. UPDATE sensorthings."User".role  → target role.
   b. Apply the appropriate RLS policy function for the target role.

Architecture note
-----------------
istSOS users are application-level entities; they do NOT have individual
PostgreSQL login roles.  Activation is therefore a pure application-state
mutation: an UPDATE on the role column plus an RLS policy call.  No
``CREATE ROLE``, ``GRANT``, or other DDL is issued.
"""

import logging

from app import POSTGRES_PORT_WRITE
from app.db.asyncpg_db import get_pool, get_pool_w
from app.models.error import MessageError
from app.oauth import get_current_user
from app.rbac_roles import PENDING_ROLE, validate_rbac_role
from app.v1.endpoints.openapi_responses import (
    DB_TIMEOUT,
    DB_UNAVAILABLE,
    INTERNAL,
    MSG_FORBIDDEN_DB,
    MSG_UNAUTHORIZED,
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
from fastapi import APIRouter, Body, Depends, HTTPException, status
from fastapi.responses import JSONResponse

v1 = APIRouter()
logger = logging.getLogger(__name__)

ACTIVATE_PAYLOAD_EXAMPLE = {
    # one of: viewer, editor, obs_manager, sensor, qc, odrl_governed
    "role": "viewer",
}


@v1.api_route(
    "/Users/{user_id}/activate",
    methods=["POST"],
    tags=["Registration & Approval"],
    summary="Activate a pending OIDC user",
    description=(
        "Promote a user from the 'pending' waiting room to a fully active "
        "role.  Applies Row-Level Security policies for the assigned role. "
        "Only accessible by an administrator.  No PostgreSQL DDL is issued."
    ),
    status_code=status.HTTP_200_OK,
    responses=merge(
        {
            200: response(
                MessageError,
                "Activated with the requested role.",
                {"message": "User 'jdoe' has been activated with role 'viewer'."},
            ),
            # Three distinct causes share 400: an unrecognised role string,
            # a rejected applicant (role stays 'pending' by design, so the
            # pending/404 checks alone wouldn't catch this), or
            # 'odrl_governed' requested with no dataset_id on file.
            400: response(
                MessageError,
                "One of: the requested role isn't one of the assignable "
                "roles; the user's registration was rejected and must be "
                "re-applied via POST /Register instead; or 'odrl_governed' "
                "was requested but this user has no dataset_id on file "
                "(only set via GET /auth/{provider}/login).",
                {
                    "message": "User 'jdoe' has no dataset_id on file, so "
                    "they cannot be activated into 'odrl_governed'. They "
                    "must restart login via /auth/{provider}/login with a "
                    "dataset_id and odrl_policy_id selected."
                },
            ),
        },
        MSG_UNAUTHORIZED,
        MSG_FORBIDDEN_DB,
        {
            404: response(
                MessageError,
                "No user exists with that id.",
                {"message": "User with id=42 not found."},
            ),
            409: response(
                MessageError,
                "The target user is not currently 'pending' -- already "
                "activated, or otherwise not eligible.",
                {"message": "User 'jdoe' is not pending (current role: 'viewer')."},
            ),
        },
        DB_UNAVAILABLE,
        DB_TIMEOUT,
        INTERNAL,
    ),
)
async def activate_user(
    user_id: int,
    payload: dict = Body(examples=[ACTIVATE_PAYLOAD_EXAMPLE]),
    current_user=Depends(get_current_user),
    pgpool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    # ------------------------------------------------------------------
    # 1. Authorization: only administrators may activate users.
    # ------------------------------------------------------------------
    if current_user["role"] != "administrator":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only administrators can activate pending users.",
        )

    # ------------------------------------------------------------------
    # 2. Validate the requested target role.
    # ------------------------------------------------------------------
    target_role_raw = payload.get("role", "")
    try:
        target_role = validate_rbac_role(target_role_raw)
    except ValueError as exc:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"message": str(exc)},
        )

    try:
        async with pgpool.acquire() as conn:
            # ----------------------------------------------------------
            # 3. Fetch the target user and assert they are 'pending'.
            # ----------------------------------------------------------
            user_row = await conn.fetchrow(
                """
                SELECT id, username, role, status, dataset_id
                FROM sensorthings."User"
                WHERE id = $1
                """,
                user_id,
            )

            if user_row is None:
                return JSONResponse(
                    status_code=status.HTTP_404_NOT_FOUND,
                    content={"message": f"User with id={user_id} not found."},
                )

            if user_row["role"] != PENDING_ROLE:
                return JSONResponse(
                    status_code=status.HTTP_409_CONFLICT,
                    content={
                        "message": (
                            f"User '{user_row['username']}' is not pending "
                            f"(current role: '{user_row['role']}')."
                        )
                    },
                )

            # Rejection is a status transition, not a role change — a
            # rejected user still has role='pending' by design, so the
            # guard above alone wouldn't catch this. See the identical
            # guard in update/admin_approval.py.
            if user_row["status"] == "rejected":
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={
                        "message": (
                            f"User '{user_row['username']}'s registration "
                            "was rejected and cannot be activated directly. "
                            "They must re-apply via POST /Register first."
                        )
                    },
                )

            username = user_row["username"]

            if target_role == "odrl_governed" and user_row["dataset_id"] is None:
                # This user never went through /auth/{provider}/login with
                # a dataset_id/odrl_policy_id selection (see oidc_login.py)
                # -- 'odrl_governed' has nothing to scope the RLS predicate
                # to, so approving into it would be a silent no-op policy
                # exactly like the old 'custom' role used to be.
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={
                        "message": (
                            f"User '{username}' has no dataset_id on file, "
                            "so they cannot be activated into 'odrl_governed'. "
                            "They must restart login via /auth/{provider}/login "
                            "with a dataset_id and odrl_policy_id selected."
                        )
                    },
                )

            # ----------------------------------------------------------
            # 4. All mutations inside a single transaction so any
            #    failure leaves the user still 'pending' (no half-state).
            # ----------------------------------------------------------
            async with conn.transaction():

                # 4a. Promote the application-layer role in the User table.
                #     Pure parameterised UPDATE — no DDL.
                await conn.execute(
                    """
                    UPDATE sensorthings."User"
                    SET role = $1
                    WHERE id  = $2
                    """,
                    target_role,
                    user_id,
                )

                # 4b. odrl_governed is the one role that still needs a
                #     per-activation CREATE POLICY call — it needs a
                #     dataset_id to mean anything, and an OIDC-provisioned
                #     user can actually have one now (collected at
                #     /auth/{provider}/login, see oidc_login.py), read
                #     straight off user_row. Mirrors
                #     update/admin_approval.py's dispatch. Deliberately NOT
                #     migrated to the static-policy scheme; ODRL work is
                #     scoped for later.
                #
                #     Every other role needs no RLS DDL here at all as of
                #     007_session_scoped_rls_policies.sql — their policies
                #     are static, created once by that migration, not
                #     per-activation. The role UPDATE above is the entire
                #     grant.
                #
                #     IMPORTANT: asyncpg marks the entire transaction as
                #     aborted on any caught exception inside it, so a
                #     nested savepoint isolates UndefinedObjectError —
                #     the outer transaction (role UPDATE) still commits.
                #     Mirrors the identical pattern in
                #     update/admin_approval.py.
                if target_role == "odrl_governed":
                    policyname = f"{username}_default"
                    try:
                        async with conn.transaction():
                            await conn.execute(
                                "SELECT sensorthings.odrl_governed_policy($1, $2, $3);",
                                [username],
                                policyname,
                                user_row["dataset_id"],
                            )
                    except UndefinedObjectError:
                        logger.warning(
                            "RLS policy skipped for '%s': no PostgreSQL role "
                            "exists (application-layer user from /Register "
                            "— zero DB footprint).",
                            username,
                        )

        logger.info(
            "User '%s' (id=%d) activated to role '%s' by admin '%s'.",
            username,
            user_id,
            target_role,
            current_user["username"],
        )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": (
                    f"User '{username}' has been activated with role '{target_role}'."
                )
            },
        )

    except InsufficientPrivilegeError:
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={"message": "Insufficient database privileges."},
        )
    except (PostgresConnectionError, TooManyConnectionsError):
        logger.exception("Database unavailable during user activation")
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"message": "Database temporarily unavailable."},
        )
    except QueryCanceledError:
        logger.exception("Database timeout during user activation")
        return JSONResponse(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            content={"message": "Database request timed out."},
        )
    except Exception:
        logger.exception("Unexpected error during user activation")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"message": "Internal server error."},
        )
