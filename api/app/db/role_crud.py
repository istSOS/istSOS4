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

"""Database logic for the PATCH /Users/{id}/role endpoint.

Design decisions
----------------
* All mutations run inside a single asyncpg transaction so a failure
  reverts atomically.

* The SELECT uses FOR UPDATE to serialize concurrent re-assignments of
  the same user row and to make the last-admin count safe under load.

* Role reassignment is a pure UPDATE on sensorthings."User".role.
  No PostgreSQL DDL (REVOKE/GRANT) is issued — users are application-layer
  entities and do not have individual PostgreSQL login roles.  The
  set_role() function in functions.py maps app roles to PG group roles
  dynamically at request time.
"""

import logging

from app import POSTGRES_PORT_WRITE
from app.db.asyncpg_db import get_pool, get_pool_w
from app.rbac_roles import PENDING_ROLE
from fastapi import HTTPException, status

logger = logging.getLogger(__name__)


async def update_user_role(user_id: int, new_role: str) -> None:
    """Atomically update a user's application role.

    Execution order (all within a single transaction):
        1. SELECT … FOR UPDATE — fetch user row; 404 if missing.
        2. Guard: pending users cannot be reassigned (400).
        3. Guard: no-op if current_role == new_role (return early).
        4. Guard: last-admin lockout — 409 if demoting the only admin.
        5. UPDATE sensorthings."User" SET role = new_role WHERE id = user_id.

    Args:
        user_id:  Primary key of the target User row.
        new_role: Target application role (already validated by Pydantic schema).

    Raises:
        HTTPException 404: User not found.
        HTTPException 400: User is in the 'pending' waiting room.
        HTTPException 200: (early return) Role unchanged — no-op.
        HTTPException 409: Would demote the last administrator.
        HTTPException 500: Unexpected database error.
    """
    try:
        pool = await get_pool_w() if POSTGRES_PORT_WRITE else await get_pool()
    except Exception:
        pool = await get_pool()

    async with pool.acquire() as conn:
        async with conn.transaction():

            # ------------------------------------------------------------------
            # 1. Fetch the target user row with a row-level lock.
            # ------------------------------------------------------------------
            row = await conn.fetchrow(
                """
                SELECT id, username, role
                FROM sensorthings."User"
                WHERE id = $1
                FOR UPDATE
                """,
                user_id,
            )

            if row is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"User with id={user_id} not found.",
                )

            current_role = row["role"]
            username = row["username"]

            # ------------------------------------------------------------------
            # 2. Guard: pending users have no PG role — reassignment is invalid.
            # ------------------------------------------------------------------
            if current_role == PENDING_ROLE:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=(
                        "Cannot reassign role for a pending user. "
                        "Activate the account first via POST /Users/{id}/activate."
                    ),
                )

            # ------------------------------------------------------------------
            # 3. No-op guard — avoid unnecessary DDL.
            # ------------------------------------------------------------------
            if current_role == new_role:
                logger.info(
                    "Role reassignment for user %r (id=%d) is a no-op "
                    "(already '%s').",
                    username, user_id, new_role,
                )
                return  # 204 with no DB mutation

            # ------------------------------------------------------------------
            # 4. Last-administrator lockout guard.
            #    Only fires when the user being reassigned is currently an admin.
            # ------------------------------------------------------------------
            if current_role == "administrator":
                admin_count = await conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM sensorthings."User"
                    WHERE role = 'administrator'
                    """,
                )
                if admin_count <= 1:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail=(
                            "Cannot demote the last administrator. "
                            "Promote another user to administrator first."
                        ),
                    )

            # ------------------------------------------------------------------
            # 5. Update the application-layer role in the User table.
            # ------------------------------------------------------------------
            await conn.execute(
                """
                UPDATE sensorthings."User"
                SET role = $1
                WHERE id = $2
                """,
                new_role,
                user_id,
            )

    logger.info(
        "Role for user %r (id=%d) updated: %r → %r.",
        username, user_id, current_role, new_role,
    )

