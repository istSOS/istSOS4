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
from app.oauth import get_current_user
from app.rbac_roles import PENDING_ROLE, POLICY_FN_MAP, validate_rbac_role
from asyncpg.exceptions import (
    InsufficientPrivilegeError,
    PostgresConnectionError,
    QueryCanceledError,
    TooManyConnectionsError,
)
from fastapi import APIRouter, Body, Depends, HTTPException, status
from fastapi.responses import JSONResponse

v1 = APIRouter()
logger = logging.getLogger(__name__)

ACTIVATE_PAYLOAD_EXAMPLE = {
    "role": "viewer",  # one of: viewer, editor, obs_manager, sensor, custom
}


@v1.api_route(
    "/Users/{user_id}/activate",
    methods=["POST"],
    tags=["Users"],
    summary="Activate a pending OIDC user",
    description=(
        "Promote a user from the 'pending' waiting room to a fully active "
        "role.  Applies Row-Level Security policies for the assigned role. "
        "Only accessible by an administrator.  No PostgreSQL DDL is issued."
    ),
    status_code=status.HTTP_200_OK,
)
async def activate_user(
    user_id: int,
    payload: dict = Body(example=ACTIVATE_PAYLOAD_EXAMPLE),
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
                SELECT id, username, role
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

            username = user_row["username"]

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

                # 4b. Apply the default RLS policy for the target role.
                #     'custom' has no default policy function; admin must
                #     create one explicitly via POST /Policies.
                #     POLICY_FN_MAP is the single source of truth — imported
                #     from rbac_roles.py to stay in sync with create/user.py.
                policy_fn = POLICY_FN_MAP.get(target_role)
                if policy_fn:
                    policyname = f"{username}_default"
                    await conn.execute(
                        f"SELECT {policy_fn}($1, $2);",
                        [username],
                        policyname,
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
