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
* All mutations (UPDATE + optional REVOKE/GRANT) run inside a single
  asyncpg transaction. If the DDL step fails the User.role column reverts
  atomically — the two can never diverge.

* The SELECT uses FOR UPDATE to serialize concurrent re-assignments of
  the same user row and to make the last-admin count safe under load.

* REVOKE/GRANT use pg_quote_ident for SQL-injection safety. asyncpg $N
  placeholders are not supported in DDL statements.

* viewer and editor both map to the 'user' PG group role; obs_manager and
  sensor both map to 'sensor'. When the underlying PG role is unchanged
  (e.g. viewer → editor) only the application-layer User.role column is
  updated — no DDL is issued.

* 'administrator' has no entry in DB_ROLE_BY_RBAC_ROLE because it is a
  bootstrap-only role. get_db_role_for_rbac() therefore cannot be called
  with 'administrator'. The demotion path uses a raw REVOKE of the
  'administrator' PG role directly.
"""

import logging

from app import POSTGRES_PORT_WRITE
from app.db.asyncpg_db import get_pool, get_pool_w
from app.rbac_roles import PENDING_ROLE, DB_ROLE_BY_RBAC_ROLE, get_db_role_for_rbac
from app.utils.utils import pg_quote_ident
from fastapi import HTTPException, status

logger = logging.getLogger(__name__)

# The PostgreSQL group role that backs the 'administrator' application role.
# Kept here rather than in rbac_roles.py because administrator is intentionally
# excluded from the public-facing VALID_RBAC_ROLES mapping.
_ADMIN_PG_ROLE = "administrator"


async def update_user_role(user_id: int, new_role: str) -> None:
    """Atomically update a user's application role and PostgreSQL group membership.

    Execution order (all within a single transaction):
        1. SELECT … FOR UPDATE — fetch user row; 404 if missing.
        2. Guard: pending users cannot be reassigned (400).
        3. Guard: no-op if current_role == new_role (return early).
        4. Guard: last-admin lockout — 409 if demoting the only admin.
        5. UPDATE sensorthings."User" SET role = new_role WHERE id = user_id.
        6. If the underlying PostgreSQL group role changes:
               REVOKE <old_pg_role> FROM <username>
               GRANT  <new_pg_role> TO  <username>

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
            if current_role == _ADMIN_PG_ROLE:
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

            # ------------------------------------------------------------------
            # 6. Update PostgreSQL group role membership if it has changed.
            #    administrator is handled separately since it is not in
            #    DB_ROLE_BY_RBAC_ROLE.
            # ------------------------------------------------------------------
            if current_role == _ADMIN_PG_ROLE:
                # Demoting an admin: revoke the administrator PG role and
                # grant the new target PG group role.
                old_pg_role = _ADMIN_PG_ROLE
                new_pg_role = get_db_role_for_rbac(new_role)
            else:
                old_pg_role = DB_ROLE_BY_RBAC_ROLE.get(current_role)
                new_pg_role = get_db_role_for_rbac(new_role)

            if old_pg_role and old_pg_role != new_pg_role:
                await conn.execute(
                    "REVOKE {} FROM {};".format(
                        pg_quote_ident(old_pg_role),
                        pg_quote_ident(username),
                    )
                )
                await conn.execute(
                    "GRANT {} TO {};".format(
                        pg_quote_ident(new_pg_role),
                        pg_quote_ident(username),
                    )
                )
                logger.info(
                    "PG role for user %r (id=%d): REVOKE %r, GRANT %r.",
                    username, user_id, old_pg_role, new_pg_role,
                )
            else:
                logger.info(
                    "User %r (id=%d): app role %r → %r "
                    "(same underlying PG role '%s', no DDL issued).",
                    username, user_id, current_role, new_role, new_pg_role,
                )

    logger.info(
        "Role for user %r (id=%d) updated: %r → %r.",
        username, user_id, current_role, new_role,
    )
