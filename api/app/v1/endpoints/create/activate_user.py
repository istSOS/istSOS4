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
   b. CREATE ROLE <username> NOLOGIN IN ROLE <db_group_role>
      (NOLOGIN because the user authenticates via OIDC, not pg password).
   c. GRANT <username> TO <activating_admin>  (so admin can later administer).
   d. Call the appropriate RLS policy function for the target role.
"""

import logging

from app import POSTGRES_PORT_WRITE
from app.db.asyncpg_db import get_pool, get_pool_w
from app.oauth import get_current_user
from app.rbac_roles import PENDING_ROLE, get_db_role_for_rbac, validate_rbac_role
from app.utils.utils import pg_quote_ident
from asyncpg.exceptions import (
    InsufficientPrivilegeError,
    PostgresConnectionError,
    QueryCanceledError,
    TooManyConnectionsError,
    UniqueViolationError,
)
from fastapi import APIRouter, Body, Depends, HTTPException, status
from fastapi.responses import JSONResponse

v1 = APIRouter()
logger = logging.getLogger(__name__)

# Maps each assignable RBAC role to the stored PostgreSQL RLS-policy function.
# Must stay in sync with create/user.py's _POLICY_FN_MAP.
_POLICY_FN_MAP = {
    "viewer":      "sensorthings.viewer_policy",
    "editor":      "sensorthings.editor_policy",
    "obs_manager": "sensorthings.obs_manager_policy",
    "sensor":      "sensorthings.sensor_policy",
}

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
        "role. Creates the user's PostgreSQL database role and applies Row-Level "
        "Security policies. Only accessible by an administrator."
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
            db_group_role = get_db_role_for_rbac(target_role)

            # ----------------------------------------------------------
            # 4. All mutations inside a single transaction so any
            #    failure leaves the user still 'pending' (no half-state).
            # ----------------------------------------------------------
            async with conn.transaction():

                # 4a. Promote the application-layer role in the User table.
                await conn.execute(
                    """
                    UPDATE sensorthings."User"
                    SET role = $1
                    WHERE id  = $2
                    """,
                    target_role,
                    user_id,
                )

                # 4b. Create the PostgreSQL role (NOLOGIN because the user
                #     authenticates via OIDC; no pg password is required).
                #     pg_quote_ident protects against identifier injection.
                await conn.execute(
                    "CREATE ROLE {} NOLOGIN IN ROLE {};".format(
                        pg_quote_ident(username),
                        pg_quote_ident(db_group_role),
                    )
                )

                # 4c. Grant the new role to the activating admin so the
                #     admin can later administer it (matches create/user.py).
                await conn.execute(
                    "GRANT {} TO {};".format(
                        pg_quote_ident(username),
                        pg_quote_ident(current_user["username"]),
                    )
                )

                # 4d. Apply the default RLS policy for the target role.
                #     'custom' has no default policy function; admin must
                #     create one explicitly via POST /Policies.
                policy_fn = _POLICY_FN_MAP.get(target_role)
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

    except UniqueViolationError:
        # The CREATE ROLE failed because a PG role with this name already exists.
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "message": (
                    f"A PostgreSQL role named '{username}' already exists. "
                    "Manual cleanup may be required."
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
